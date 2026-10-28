# Jira delivery pilot: procedure and gate

Status: **Not run**. This procedure grants no execution authority.
Code tests prove isolated behavior, not live readiness or successful delivery.
Keep live results outside this public repository. Never publish credentials,
private repository addresses, account identifiers, or unsanitized logs here.

## Preparation without enablement

1. Run the repository's trusted `bash scripts/local-ci.sh` on the approved revision.
   Retain its backend, lint, type, migration, legacy-intake, and secret-scan results.
2. Copy [the evidence index](evidence-index.md) into an access-controlled evidence root.
3. Copy [the result template](result-template.md) for every case and controlled subrun.
4. Fill [the seed manifest](seed-manifest.md) before the affected run.
5. Obtain separate authorization for deployment, configuration, issue creation,
   execution, faults, restarts, and human merges. A code merge grants none of these.
6. Run the local read-only preflight with the operator's environment and proposed
   serialized `ScopeContract`:

   ```sh
   uv run --project core python -m switch_core.bridges.jira.readiness \
     --instance example --project TEST --scope /approved/evidence/scope.json
   ```

   The command reads configuration and a scope file. It never contacts Jira,
   opens a database, enables delivery, changes a setting, or starts a job.
   It reports each local check and returns nonzero on any blocker.
   It validates the selected immutable pack and its digest pin.
   Scope validation includes bounded commands, paths, repository triage pins,
   role profile digests, reservations, and the unique approval route.
   With the live pack, it also validates the server runtime, pinned Firstmate and pi,
   trusted policy, and role reservations against the configured price table.

The default fixture pack still refuses `live_pack`. Select the live pack and runtime
with the [local setup procedure](local-setup.md). A configured live setup passes every
local item except `live_prerequisites`. That item stays blocked until P0 evidence exists.
Local checks cannot certify live workflow restrictions, sandboxing, provider limit
behavior, or permissions. Do not change the pack, model, or budgets to bypass a refusal.

## Shared preflight P0

Record signed results and evidence for every row. Start every result as Not run.
A failed prerequisite blocks admission; it does not authorize a repair.

| Check | Required evidence |
| --- | --- |
| P0.1 Authority | Named reporter, approver, owner, administrator, independent non-approver, evidence signer; permissions and run authorization |
| P0.2 Scope gate | Exact status/transition IDs; positive authorized action; non-approver REST/UI denial and unchanged status; all bypass routes closed |
| P0.3 Candidate gate | Same refusal/bypass checks bound to tested head; separate acceptance receipt; bot cannot approve or merge |
| P0.4 Chat | Neither scope nor candidate approval comes from chat; only current Jira history grants authority |
| P0.5 Uniqueness | Old delivery automation stays disabled; no competing trigger; durable webhook acknowledgment and webhook/poll deduplication |
| P0.6 Fields | Service and Jira issue type checked separately; webhook, poll, send-time mapping agreement; non-Bug routing |
| P0.7 Pins | Approved pack, runtime, skills, model, prices, base, commands, paths; repository triage plus product corroboration before checkout; no registration allowlist |
| P0.8 Isolation | Distinct worker locations; no host home/socket, production/metadata network, Jira or Switch credentials; publisher-only branch access; safe artifacts |
| P0.9 Accounting | Input/output/cache/retry/cancelled usage; unknown reservations retained; ticket/day limits, UTC rollover, trusted spend enforcement |
| P0.10 Disablement | No fresh start, retry, or checkpoint resume; cancellation and late-output fences; capacity retained until exit/quarantine |
| P0.11 Compatibility | Exact deployed revision; kind filtering; legacy regression and migration results; no old binary with role-job rows |
| P0.12 Operations | Calendar, selected answer channel/fallback, room/admin cards, evidence access/retention; full live wait window reserved |

Use isolated proof before admission. Reuse P01 for authorized live refusal checks
with delivery disabled. Record its real positive scope approval during delivery.
Repeat candidate-bound refusal checks at its actual candidate gate.
A generic status move, dirty history, or alternate edge never grants approval.
An authorized human close stops work; it never certifies delivery.

For configuration, exact APIs, cancellation, and rollback, use the existing
[Jira worker runbook](../../core/switch_core/bridges/jira/README.md), especially
"Scope approval and Bug execution", "Independent verification and completion",
"Admin cards", and "Disable and rollback runbook". Do not reset database rows.

## Execution after separate authorization

Use nineteen mapped issues for twenty records. P14 coordinates P11/P12/P17;
it creates no additional issue. P0 reuses P01. Do not exceed the approved mapping.
Controlled reruns preserve prior evidence, spend, pack, and receipts. They need
supported requalification and fresh authority. Unsupported reuse is Blocked.

Order: P0, clean P01, clean P02, then P16's full live wait. Continue with
P03/P04/P06/P07/P08, the P14 cohort, containment/channels, human interruption,
and timing/closure cases. Do not inject faults before both clean deliveries pass.
Remain within existing capacity and daily budgets. Queue work instead of topping up.

### Full delivery protocol F

1. The reporter supplies a manifest-backed bug. Trusted triage verifies its repository before checkout.
2. Publish the frozen scope summary before the exact approval request.
   The human scope receipt binds actor, history, gate, generation, and contract digest.
3. Reproduce the user-visible failure at the frozen base before repair.
4. Repair within approved paths. Retain separate regression/fix commits and required lint, types, and relevant tests.
   Publish one logical branch and draft request through the trusted publisher.
5. Fresh independent review checks immutable candidate H without editing or publishing.
6. Fresh QA repeats the original behavior and acceptance checks at H. Unit tests alone do not suffice.
7. Publish the tested summary before the separate Jira candidate gate.
   Human acceptance binds that same H. No live role waits for a human.
8. A separately authorized human owner performs the forge merge. No agent merges or deploys.
9. Check the actual receipt: repository, request, human actor, accepted H, and actual merged SHA M.
   H and M can differ. Verify their relationship rather than demanding string equality.
10. Run approved smoke/checks at M. Only success permits the close summary,
    exact Complete delivery, final card synchronization, and archive.

A head change invalidates review, QA, and acceptance. A scope change needs fresh
scope approval. A changed base needs fresh merge validation. Security-sensitive
work, destructive migrations, new dependencies, and public API changes need a
separate decision. Done means accepted, human-merged, and smoke-checked, not deployed.

## Final gate

Copy [the gate review](gate-review.md). A human reviewer signs it only after:

- Every case and required subrun has direct evidence and a Pass verdict.
  A safe refusal can pass its expected refusal case; an unobserved behavior cannot.
- P01/P02/P06/P07/P08 complete F and Done for five distinct verified seed defects.
  Blocked, human-closed, cancelled, and manual-handoff outcomes count as no delivery.
- P0 approval/refusal/bypass, P14 disablement, compatibility, and all safety checks pass.
- P16 proves the live two-work-hour reminder, four-work-hour alert, one-work-day
  Blocked, and restart behavior. Accelerated-clock tests never replace this proof.
- Questions survive restart without lost answers; uncertainty stays visible without
  replay; capacity/spend/idle/error bounds and alert-only 2x behavior hold.
- Cards, archive/reopen, candidate identities, human receipts, and smoke-at-M evidence agree.
- No required phase, safety case, or unsupported variant remains skipped or unexplained.

**Any unsafe bot write, wrong-repository execution, duplicate logical job, or
unapproved merge fails the gate, even if a later action parks the ticket.**
Duplicate rooms/candidates, credential exposure, unauthorized branches, and any
deployment also fail it. A passing automated suite cannot override an incident.

Stop new admission on a safety failure. Preserve evidence before cleanup.
Use the existing cancellation/rollback runbook. Fence late output and reconcile
remote uncertainty before releasing reservations or capacity. Never undo a human
action automatically. Repairs and reruns need separate authorization.

## Automated boundary and remaining proof

`core/tests/switch_core/bridges/jira/test_pilot.py` drives scope summary, both
human gates, reproduction/fix/review/QA, trusted publication, human merge receipt,
merged-SHA smoke, and ordered closure through existing components and PostgreSQL.
It checks concurrent reconciliation, wrong-repository and non-human merge refusal,
and a human interrupt with completion queued. Its P01 evidence assertions also
reject an extra unsafe write, wrong repository, duplicate job, or non-human merge
even when the recorded outcome says delivered. These assertions evaluate the
isolated transcript, not live permissions or all final human gate criteria.
Existing fault, input, migration, legacy, and gateway suites remain required;
this fixture does not replace them.

External Jira, model, process, and forge peers are in-process doubles. No real
provider, forge permission, workflow restriction, OS sandbox, live defect, room
bridge, or wait duration receives proof from these tests. Live results remain
Not run until separately authorized execution supplies evidence.
