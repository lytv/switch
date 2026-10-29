"""Trusted GitHub-compatible transport; never construct it inside a role worker."""

from __future__ import annotations

import re
from typing import Any, Literal
from urllib.parse import quote, urlsplit

import httpx

from switch_core.bridges.jira.contracts import repository_identity
from switch_core.bridges.jira.delivery_contracts import Handoff, ScopeContract
from switch_core.bridges.jira.publisher import DraftRequest, GitCandidateObjects
from switch_core.bridges.jira.verification import MergeReceipt


class GitHubDraftForge:
    def __init__(
        self,
        *,
        scope: ScopeContract,
        objects: GitCandidateObjects,
        client: httpx.AsyncClient,
        authorization: str,
    ) -> None:
        repository = scope.job.repository
        self._scope = scope
        if repository.outcome != "verified" or repository.canonical_url is None:
            raise ValueError("Forge transport requires the verified ticket repository")
        host, port, path = repository_identity(repository.canonical_url)
        parts = path.lstrip("/").split("/")
        if len(parts) != 2 or port is not None:
            raise ValueError("GitHub transport requires an owner/repository URL")
        if not authorization or any(char in authorization for char in "\r\n\0"):
            raise ValueError("Forge authorization must be a nonempty HTTP header")
        if client.base_url.scheme != "https":
            raise ValueError("Forge API requires a trusted HTTPS endpoint")
        self._repository = repository
        self._owner, self._name = parts
        self._api = "/repos/" + "/".join(quote(part, safe="") for part in parts)
        self._push_url = f"https://{host}{path}.git"
        self._objects = objects
        self._client = client
        self._authorization = authorization

    def _check(self, url: str, branch: str) -> None:
        if url != self._repository.canonical_url or branch != self._scope.branch:
            raise ValueError(
                "Forge operation differs from the approved repository or branch"
            )

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        response = await self._client.request(
            method,
            self._api + path,
            headers={"Authorization": self._authorization},
            follow_redirects=False,
            **kwargs,
        )
        if response.is_redirect:
            raise ValueError("Forge redirects cannot receive delivery credentials")
        return response

    async def branch_head(self, repository_url: str, branch: str) -> str | None:
        self._check(repository_url, branch)
        response = await self._request(
            "GET", "/git/ref/heads/" + quote(branch, safe="")
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        head = response.json()["object"]["sha"]
        if not isinstance(head, str) or re.fullmatch(r"[0-9a-f]{40}", head) is None:
            raise ValueError("Forge returned an invalid branch head")
        return head

    async def verify_branch_absent(self, repository_url: str, branch: str) -> None:
        self._check(repository_url, branch)
        repository = await self._request("GET", "")
        repository.raise_for_status()
        if repository.status_code != 200:
            raise ValueError("Repository read did not confirm forge access")
        if await self.branch_head(repository_url, branch) is not None:
            raise ValueError("Remote branch exists; recovery requires reconciliation")

    async def push_new_branch(
        self, repository_url: str, branch: str, head: str
    ) -> None:
        self._check(repository_url, branch)
        if re.fullmatch(r"[0-9a-f]{40}", head) is None:
            raise ValueError(
                "Publisher requires an exact commit; deletion is forbidden"
            )
        if await self.branch_head(repository_url, branch) is not None:
            raise ValueError("Publisher refuses to change an existing branch")
        await self._objects._git(
            "push",
            "--porcelain",
            "--no-verify",
            "--",
            self._push_url,
            f"{head}:refs/heads/{branch}",
            authorization=self._authorization,
        )

    def _draft(self, body: dict[str, Any]) -> DraftRequest:
        if (
            body["head"]["repo"]["full_name"] != f"{self._owner}/{self._name}"
            or body["base"]["repo"]["full_name"] != f"{self._owner}/{self._name}"
        ):
            raise ValueError("Draft request points to another repository")
        return DraftRequest(
            url=body["html_url"],
            branch=body["head"]["ref"],
            base_branch=body["base"]["ref"],
            head=body["head"]["sha"],
            draft=body["draft"],
            open=body["state"] == "open",
        )

    async def find_requests(
        self, repository_url: str, branch: str
    ) -> tuple[DraftRequest, ...]:
        self._check(repository_url, branch)
        requests: list[DraftRequest] = []
        page = 1
        while True:
            response = await self._request(
                "GET",
                "/pulls",
                params={
                    "state": "all",
                    "head": f"{self._owner}:{branch}",
                    "per_page": 100,
                    "page": page,
                },
            )
            response.raise_for_status()
            entries = response.json()
            if not isinstance(entries, list):
                raise ValueError("Forge returned an invalid request list")
            requests.extend(self._draft(entry) for entry in entries)
            if len(entries) < 100:
                return tuple(requests)
            page += 1

    async def create_draft(
        self,
        repository_url: str,
        branch: str,
        base_branch: str,
        head: str,
        execution_key: str,
    ) -> DraftRequest:
        self._check(repository_url, branch)
        if (
            base_branch != self._repository.base_branch
            or await self.branch_head(repository_url, branch) != head
        ):
            raise ValueError("Draft base or head differs from the frozen candidate")
        response = await self._request(
            "POST",
            "/pulls",
            json={
                "head": branch,
                "base": base_branch,
                "draft": True,
                "maintainer_can_modify": False,
                "title": "Switch bug-fix candidate",
                "body": f"Draft candidate for human review.\n\nExecution: {execution_key}\nHead: {head}",
            },
        )
        response.raise_for_status()
        return self._draft(response.json())

    async def verify_candidate(self, scope: ScopeContract, handoff: Handoff) -> None:
        if scope != self._scope:
            raise ValueError("Candidate belongs to another repository contract")
        await self._objects.verify(scope, handoff)

    def _number(self, repository_url: str, request_url: str) -> str:
        self._check(repository_url, self._scope.branch)
        host, _, path = repository_identity(repository_url)
        url = urlsplit(request_url)
        match = re.fullmatch(re.escape(path) + r"/pull/([1-9][0-9]*)", url.path)
        if (
            url.scheme != "https"
            or url.hostname != host
            or url.port not in {None, 443}
            or url.username is not None
            or url.query
            or url.fragment
            or match is None
        ):
            raise ValueError("Request URL differs from the approved repository")
        return match[1]

    async def update_branch(
        self, repository_url: str, branch: str, previous: str, head: str
    ) -> None:
        self._check(repository_url, branch)
        if (
            not all(re.fullmatch(r"[0-9a-f]{40}", value) for value in (previous, head))
            or await self.branch_head(repository_url, branch) != previous
        ):
            raise ValueError("Rework branch changed before publication")
        await self._objects._git("merge-base", "--is-ancestor", previous, head)
        await self._objects._git(
            "push",
            "--porcelain",
            "--no-verify",
            "--",
            self._push_url,
            f"{head}:refs/heads/{branch}",
            authorization=self._authorization,
        )

    async def ready_request(
        self, repository_url: str, request_url: str, head: str
    ) -> None:
        number = self._number(repository_url, request_url)
        response = await self._request("GET", f"/pulls/{number}")
        response.raise_for_status()
        body = response.json()
        request = self._draft(body)
        if (
            request.url != request_url
            or request.head != head
            or request.branch != self._scope.branch
            or request.base_branch != self._repository.base_branch
            or not request.open
            or not request.draft
            or await self.branch_head(repository_url, self._scope.branch) != head
        ):
            raise ValueError("Draft changed before readiness")
        response = await self._client.request(
            "POST",
            "/graphql",
            headers={"Authorization": self._authorization},
            follow_redirects=False,
            json={
                "query": "mutation($id: ID!) { markPullRequestReadyForReview(input: {pullRequestId: $id}) { pullRequest { isDraft } } }",
                "variables": {"id": body["node_id"]},
            },
        )
        response.raise_for_status()
        if (
            response.is_redirect
            or response.json().get("errors")
            or response.json()["data"]["markPullRequestReadyForReview"]["pullRequest"][
                "isDraft"
            ]
            is not False
        ):
            raise ValueError("Forge did not confirm candidate readiness")

    async def merge_receipt(
        self, repository_url: str, request_url: str
    ) -> MergeReceipt:
        number = self._number(repository_url, request_url)
        response = await self._request("GET", f"/pulls/{number}")
        response.raise_for_status()
        body = response.json()
        request = self._draft(body)
        if (
            request.branch != self._scope.branch
            or request.base_branch != self._repository.base_branch
        ):
            raise ValueError("Merge receipt differs from the approved branch or base")
        actor = body.get("merged_by") or {}
        merged = body.get("merged") is True and bool(body.get("merged_at"))
        outcome: Literal["open", "closed_unmerged", "unknown", "merged"] = (
            "merged"
            if merged
            else "open"
            if request.open
            else "closed_unmerged"
            if body.get("merged") is False
            else "unknown"
        )
        return MergeReceipt(
            repository_url=repository_url,
            request_url=request.url,
            candidate_sha=request.head,
            base_branch=request.base_branch,
            outcome=outcome,
            merged_sha=body.get("merge_commit_sha") if merged else None,
            actor_id=str(actor["id"]) if actor.get("id") is not None else None,
            human=actor.get("type") == "User",
        )
