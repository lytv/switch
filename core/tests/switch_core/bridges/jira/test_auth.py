from __future__ import annotations

from switch_core.bridges.jira.auth import verify_instance_secret


class TestVerifyInstanceSecret:
    def test_valid_secret(self) -> None:
        assert (
            verify_instance_secret(
                instance="acme",
                provided="s3cret",
                secrets_by_instance={"acme": "s3cret"},
            )
            is True
        )

    def test_wrong_secret(self) -> None:
        assert (
            verify_instance_secret(
                instance="acme",
                provided="nope",
                secrets_by_instance={"acme": "s3cret"},
            )
            is False
        )

    def test_missing_secret(self) -> None:
        assert (
            verify_instance_secret(
                instance="acme",
                provided=None,
                secrets_by_instance={"acme": "s3cret"},
            )
            is False
        )

    def test_unknown_instance(self) -> None:
        assert (
            verify_instance_secret(
                instance="other",
                provided="s3cret",
                secrets_by_instance={"acme": "s3cret"},
            )
            is False
        )

    def test_empty_configured_secret_rejects(self) -> None:
        assert (
            verify_instance_secret(
                instance="acme",
                provided="",
                secrets_by_instance={"acme": ""},
            )
            is False
        )
