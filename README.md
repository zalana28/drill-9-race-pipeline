# slotrace — Drill #9: FCFS slot-claim race pipeline

Simulator for a first-come-first-served claim race: a local board opens
claim slots on a schedule; a claimer must detect each slot and POST
`/claim` inside a 2.0-second window. Built to train weakness #7
(design for correctness first, latency second — the Bagent/FEG races
were lost to a 1–2 minute pipeline against slots gone in under 30s).

Zero dependencies. Plain Node.js (`node:http`), Node >= 22.

## Architecture (written BEFORE code)

```
board (src/board.js)                    claimer (src/claimer.js)
 ─────────────────────                  ─────────────────────────
 slot scheduler: opens a slot           SSE subscriber (/stream)
   every 8±3 s (uniform 5–11 s)         on slot-open → POST /claim
 slot valid 2.0 s                       immediately (claimId = UUID)
 1 in 5 slots cancelled before          on 409 RETRY → retry once,
   window ends (every 5th slot;          same claimId, +50 ms, only
   cancel fires 50–500 ms after open,    while the window remains
   announced via SSE)                   on 410 → slot gone, log it
 10% of first claim attempts             on 200 → verify via GET
   rejected (409 RETRY, idempotent)      /slots/:id, log it
 REST responses delayed 0–300 ms        on slot-cancel → mark slot
   (uniform random)                     dead locally, never claim it
 SSE /stream pushes slot-open /         on SSE drop → reconnect
   slot-cancel / slot-claimed /          (max 5×, 100 ms apart),
   board-done                           re-sync via GET /slots
 POST /claim is idempotent via
   claimId: a replayed claimId
   returns the stored result,
   never a second claim
```

Why SSE push instead of polling: polling at 50 ms adds ~25 ms average
detection latency and a full round-trip per check; SSE push delivers the
announcement in ~1–10 ms on loopback and the board stays the single
source of truth (a claim on a cancelled slot is rejected 410 by the
board even if the claimer's cancel event is still in flight). The
claimer never trusts its local state alone — the board is authoritative.

Trust boundaries: the board enforces window expiry, cancellation and
idempotency server-side. The claimer's local cancelled-set is an
optimization (avoid a doomed POST), not the guard.

## Latency budget (detection → claim, p95 target < 1500 ms)

| Stage | Budget | Expected | Notes |
|---|---|---|---|
| Detection (board opens → claimer receives SSE event) | 100 ms | 1–10 ms | loopback SSE push |
| Decision (receive → POST sent) | 50 ms | <5 ms | in-memory cancelled-set check + JSON build |
| POST (send → claim response, incl. board 0–300 ms random delay) | 500 ms | ~150 ms avg, ≤~310 ms max | one retry adds 50 ms backoff + one more POST (≤310 ms) |
| Verification (GET /slots/:id → confirm) | 500 ms | ~150 ms avg | measured separately, not part of detection→claim |
| **Total detection→claim p95** | **< 1500 ms** | **~200 ms typical, ~800 ms worst case with one retry** | ~700 ms margin for scheduling jitter |

Worst-case walk: detection 100 + decision 5 + POST 310 + retry 50 + POST 310
= 775 ms — still inside the 2.0 s window and the 1500 ms p95 target.
A slot is lost only if detection + retries exceed the window, which the
budget makes unlikely by design.

## Targets (dry run ≥ 25 slots)

- Success rate ≥ 90% of valid slots claimed.
- p95 detection→claim latency < 1.5 s.
- Zero successful claims on cancelled slots.
- Zero duplicate claims.

If a target is missed, the real numbers and the diagnosis are reported —
targets are never lowered to pass.

## Run

```bash
npm test            # 10 tests, plain node:test
npm run dry-run     # 30 slots, board + claimer as separate processes
npm run dry-run -- --slots 5   # short smoke run
```

The dry run writes timestamped logs to `logs/` (`dry-run-<ts>.jsonl` for
the claimer, `board-state-<ts>.json` for the board's final state) and
prints statistics computed from those logs by `src/stats.js` — never
hand-estimated.

## Files

- `src/board.js` — board server + slot state machine (also runs standalone).
- `src/claimer.js` — SSE claimer (also runs standalone).
- `src/stats.js` — statistics from a dry-run log + board state.
- `scripts/dry-run.js` — orchestrates board + claimer as separate processes.
- `tests/slotrace.test.js` — 10 tests (happy path, expiry, cancel,
  idempotent retry, duplicate claimId, fixed 300 ms delay, racing
  claimIds, scheduler orphan-timer regression, stats correctness).

## Dry run results

### Official run — 2026-11-19, 30 slots, 235.6 s wall

Script output (verbatim, computed by `src/stats.js` from the claimer log +
board state — no hand-written numbers):

```
{
  "slotsTotal": 30,
  "slotsValid": 29,
  "slotsCancelled": 1,
  "slotsClaimed": 29,
  "successRate": 1,
  "cancelledClaims": 0,
  "duplicates": 0,
  "retries": 2,
  "rejected": 1,
  "latencyMs": { "n": 29, "p50": 164, "p95": 338, "max": 422 },
  "detectMs":  { "n": 29, "p50": 1, "p95": 1, "max": 2 }
}
targets: success>=90% PASS | p95<1500ms PASS | cancelled-claims=0 PASS | duplicates=0 PASS | OVERALL PASS
```

Independent cross-check on the raw log (`logs/dry-run-20261119-123151.jsonl`,
93 lines, 0 out-of-order timestamps, span 228.3 s): slot-open 30, claim-ok 29
(29 distinct slotIds, each exactly once), claim-rejected 1, claim-error 0,
sse-reconnect 0. The one cancelled slot (slot-25, 1 of the 6 every-5th slots)
was claimed-attempted before its cancel but processed after it — the board
rejected it 410 CANCELLED (claimMs 280), so zero claims landed on a cancelled
slot. The twist bit once, and the board's authority held. The 2 retries are
409 RETRY first attempts that succeeded on the same claimId.

Evidence files in this repo: `logs/dry-run-20261119-123151.jsonl` (claimer
log, monotonic `ts` + `mono` per line) and
`logs/board-state-20261119-123151.json` (final board state).

### Smoke runs (rehearsal, before the official run)

- 5 slots, cancel delay 300–1500 ms: 5/5 claimed, p95 372 ms, PASS.
- 5 slots, cancel delay 50–500 ms: 5/5 claimed, p95 249 ms, PASS.

### Tests

`npm test` — 10/10 pass in ~8.5 s (plain node:test, zero deps). The run
before the orphan-timer fix took 65 s; the fix (clear the pending
scheduler timer at the top of `openSlot()`) is covered by the scheduler
regression test.
