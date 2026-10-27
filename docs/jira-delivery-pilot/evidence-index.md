# Evidence allocation

All results: **Not run**. Paths are relative to a private evidence root, not this
public repository. Copy the result template to each path before execution.
Each subrun needs its own manifest, authorization, result, and human signature.

| Case | Observation | Result place | Issue allocation |
| --- | --- | --- | --- |
| V2-P01 | Clean backend delivery, full F | P01/result.md | J01; P0 shares this issue |
| V2-P02 | Clean UI delivery, full F and images | P02/result.md | J02 |
| V2-P03 | Five intake questions, rejection, fresh cycle and delivery | P03/result.md | J03 |
| V2-P04 | Job question, restart, unrelated/relevant no-mention replies | P04/result.md | J04 |
| V2-P05 | Cannot reproduce; bounded refusal, no speculative fix | P05/result.md | J05 |
| V2-P06 | Independent review rejects, bounded repair, full F | P06/result.md | J06 |
| V2-P07 | QA rejects broken behavior despite green units, repair, full F | P07/result.md | J07 |
| V2-P08 | Candidate rejected; corrected scope, fresh authority, full F | P08/result.md | J08 |
| V2-P09 | Human interrupt, cancellation, late usage and fences | P09/result.md | J09 |
| V2-P10 | Human close/reopen, same room, no old authority | P10/result.md | J10 |
| V2-P11 | Duplicate/out-of-order events and concurrent claims | P11/result.md | J11; P14 token participant |
| V2-P12 | Restart at external boundaries, no replacement | P12/result.md | J12; P14 loss participant |
| V2-P13 | Hostile data, secrets, isolation, artifact refusal | P13/result.md | J13 |
| V2-P14 | Capacity, spend, unknown holds, disablement | P14/result.md | No new issue; J11/J12/J16 |
| V2-P15 | Independent overrun/error/idle checks | P15/result.md | J14 |
| V2-P16 | Full live wait and reminder restart | P16/result.md | J15 |
| V2-P17 | Known Jira retries, unknown Jira/forge outcomes | P17/result.md | J16; P14 third participant |
| V2-P18 | Wrong place, chat approval, unmapped fallback | P18/result.md | J17 |
| V2-P19 | Repository triage and non-Bug manual handoff | P19/result.md | J18 |
| V2-P20 | Held merge, changed head, failed smoke | P20/result.md | J19 |

J01-J19 are allocation labels, not issue keys. Fill actual keys only after
creation authorization. Five distinct completed defects belong to P01/P02/P06/P07/P08.

## Required subrun places

Create `<place>/result.md` for every place below. Record separate live and
isolated phase matrices. A phase not reached remains Not run or Blocked.

| Case | Places and proof |
| --- | --- |
| P0 | P0/01 through P0/12; isolated scope/candidate refusals, actual P01 gate rechecks |
| P04 | P04/question, P04/restart, P04/unrelated, P04/answer |
| P09 | P09/reproduce, P09/fix, P09/review, P09/qa, P09/smoke; fresh authority per run |
| P10 | P10/intake, P10/reproduce, P10/fix, P10/review, P10/qa, P10/accept, P10/merge, P10/smoke, P10/reopen; record live versus isolated cells |
| P11 | P11/create, P11/approval, P11/concurrent, P11/stale-generation, P11/publication |
| P12 | P12/launch, P12/result, P12/publication, P12/worker-loss |
| P13 | P13/ticket, P13/repository, P13/hooks, P13/output, P13/traversal, P13/symlink, P13/oversize, P13/credential-url, P13/digest |
| P14 | P14/capacity, P14/token, P14/ticket-cost, P14/day-cost, P14/unknown, P14/rollover, P14/budget-request, P14/resume, P14/disable-start, P14/disable-retry, P14/disable-resume, P14/disable-running |
| P15 | P15/A overrun alert only; P15/B repeated error; P15/C heartbeat-only idle |
| P16 | P16/reminder, P16/alert, P16/blocked, P16/restart; all live work-calendar times |
| P17 | P17/rate-limit, P17/jira-unknown, P17/forge-unknown |
| P18 | P18/wrong-place, P18/chat-scope, P18/chat-candidate, P18/unmapped |
| P19 | P19/A wrong repository; P19/A2 unknown URL; P19/B Feature; P19/C Docs; P19/D Other |
| P20 | P20/unmerged, P20/head-change, P20/failed-smoke |

Every run also retains `seed-manifest.md`, serialized frozen scope/pack, sanitized
artifacts, and event/outbox/task/remote receipts beside its result. Preserve
previous run directories. Never overwrite evidence during requalification.
