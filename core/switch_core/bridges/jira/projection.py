"""Delivery fields on existing cards, without new room or message identities."""

from switch_core.bridges.jira.target import delivery_state
from switch_core.db.models import JiraWorkerTicket


def delivery_lines(ticket: JiraWorkerTicket) -> str:
    state = delivery_state(ticket)
    if state is None:
        return ""
    evidence = [
        f"{name}: {item.head} ({', '.join(check.check_id + '=' + check.artifact_digest for check in item.checks)})"
        for name, item in (
            ("reproduction", state.reproduction),
            ("review", state.review),
            ("QA", state.qa),
            ("smoke", state.smoke),
        )
        if item is not None
    ]
    return "\n" + "\n".join(
        (
            f"Delivery phase: {state.step}",
            f"Gate: {state.gate.kind + ' / ' + state.gate.id if state.gate else 'none'}",
            f"Candidate SHA: {state.candidate_head or 'none'}",
            f"Evidence: {'; '.join(evidence) or 'none'}",
            f"Question: {state.question.id if state.question else 'none'} ({state.question_count}/5)",
            f"Usage: {ticket.tokens_used} tokens known + {state.reserved_tokens} reserved / {state.budget.ticket_tokens}",
            f"Estimated USD: {state.known_estimated_usd} known + {state.reserved_estimated_usd} reserved / {state.budget.ticket_estimated_usd}",
            f"Unknown usage: {'yes' if state.unknown_spend else 'no'}",
            f"Queue: {ticket.queue_reason or 'none'}",
            *(
                ("Manual handoff only; no development jobs.",)
                if state.service != "Bug"
                else ()
            ),
        )
    )
