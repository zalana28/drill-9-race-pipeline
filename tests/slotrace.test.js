'use strict';

/**
 * slotrace tests (Drill #9), plain node:test.
 *
 * 1. Happy path: a real claimer claims a slot via SSE + POST, latency < 1500 ms.
 * 2. Expired slot: claim after the window → 410 EXPIRED.
 * 3. Cancelled slot: claim after cancel → 410 CANCELLED, zero claims recorded.
 * 4. Idempotent retry: forced first-claim rejection → retry with the same
 *    claimId succeeds, exactly one claim recorded.
 * 5. Duplicate claimId replay: same claimId twice → same 200, still one claim.
 * 6. Fixed 300 ms board delay: claim round-trip proves the delay and stays < 1500 ms.
 * 7. Racing claimIds: second claimId on a claimed slot → 409 ALREADY_CLAIMED.
 * 8. Stats: synthetic log + board state → correct rate, p50/p95/max, counts.
 */
const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { Board } = require('../src/board');
const { Claimer, httpJson } = require('../src/claimer');
const { computeStats } = require('../src/stats');

let board;
let port;
let base;
let claimer;

async function startBoard(opts = {}) {
  board = new Board({ port: 0, intervalMin: 60000, intervalMax: 60000, ...opts });
  await board.start();
  port = board.server.address().port;
  base = `http://127.0.0.1:${port}`;
}

async function stopAll() {
  if (claimer) {
    claimer.stop();
    claimer = null;
  }
  if (board) {
    await board.stop();
    board = null;
  }
}

function waitUntil(fn, timeoutMs = 5000, stepMs = 25) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const tick = () => {
      let v;
      try {
        v = fn();
      } catch {
        v = false;
      }
      if (v) return resolve(v);
      if (Date.now() - start > timeoutMs) return reject(new Error('waitUntil timed out'));
      setTimeout(tick, stepMs);
    };
    tick();
  });
}

async function postClaim(slotId, claimId, clientId = 'test-client') {
  return httpJson(`${base}/claim`, 'POST', { slotId, claimId, clientId });
}

beforeEach(async () => {
  board = null;
  claimer = null;
});

afterEach(async () => {
  await stopAll();
});

describe('claimer happy path', () => {
  it('claims a slot via SSE + POST with latency under 1500 ms', async () => {
    await startBoard({ intervalMin: 50, intervalMax: 100, cancelEvery: 0, rejectFirstP: 0, delayMin: 0, delayMax: 0 });
    const logs = [];
    claimer = new Claimer({ boardUrl: base, onLog: (e) => logs.push(e) });
    claimer.start();
    const ok = await waitUntil(() => logs.find((e) => e.event === 'claim-ok'), 5000);
    assert.ok(ok.claimMs < 1500, `claim latency ${ok.claimMs} ms should be < 1500`);
    assert.ok(ok.detectMs < 100, `detect latency ${ok.detectMs} ms should be < 100`);
    const state = board.dumpState();
    assert.equal(state.slots.filter((s) => s.status === 'claimed').length, 1);
    assert.equal(state.slots[0].claimCount, 1);
  });
});

describe('board guards', () => {
  it('claim after the window expires → 410 EXPIRED', async () => {
    await startBoard({ slotValidMs: 200, cancelEvery: 0, rejectFirstP: 0, delayMin: 0, delayMax: 0 });
    board.openSlot();
    const slotId = [...board.slots.keys()][0];
    await new Promise((r) => setTimeout(r, 400));
    const res = await postClaim(slotId, 'claim-expired-1');
    assert.equal(res.status, 410);
    assert.equal(res.json.error, 'EXPIRED');
  });

  it('claim after cancel → 410 CANCELLED and zero claims recorded', async () => {
    await startBoard({ cancelEvery: 1, cancelDelayMin: 20, cancelDelayMax: 50, rejectFirstP: 0, delayMin: 0, delayMax: 0 });
    board.openSlot();
    const slotId = [...board.slots.keys()][0];
    await waitUntil(() => board.slots.get(slotId).status === 'cancelled', 2000);
    const res = await postClaim(slotId, 'claim-cancelled-1');
    assert.equal(res.status, 410);
    assert.equal(res.json.error, 'CANCELLED');
    assert.equal(board.slots.get(slotId).claims.size, 0);
  });

  it('a claimer never claims a cancelled slot', async () => {
    // Board delay 200 ms > cancel delay 20–50 ms: the claim is sent before
    // the cancel but processed after it — the board must reject it 410.
    await startBoard({ intervalMin: 50, intervalMax: 100, cancelEvery: 1, cancelDelayMin: 20, cancelDelayMax: 50, rejectFirstP: 0, delayMin: 200, delayMax: 200 });
    const logs = [];
    claimer = new Claimer({ boardUrl: base, onLog: (e) => logs.push(e) });
    claimer.start();
    await waitUntil(() => logs.filter((e) => e.event === 'slot-cancel').length >= 2, 5000);
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(logs.filter((e) => e.event === 'claim-ok').length, 0);
    const state = board.dumpState();
    assert.ok(state.slots.length >= 2);
    assert.equal(state.slots.reduce((s, x) => s + x.claimCount, 0), 0);
  });
});

describe('idempotency', () => {
  it('forced first rejection → retry with the same claimId succeeds, one claim', async () => {
    await startBoard({ intervalMin: 50, intervalMax: 100, cancelEvery: 0, rejectFirstP: 1.0, delayMin: 0, delayMax: 0 });
    const logs = [];
    claimer = new Claimer({ boardUrl: base, onLog: (e) => logs.push(e) });
    claimer.start();
    const ok = await waitUntil(() => logs.find((e) => e.event === 'claim-ok'), 5000);
    assert.equal(ok.attempts, 2);
    assert.ok(logs.some((e) => e.event === 'claim-retry'));
    const state = board.dumpState();
    assert.equal(state.slots[0].claimCount, 1);
  });

  it('replaying the same claimId returns the stored 200, still one claim', async () => {
    await startBoard({ cancelEvery: 0, rejectFirstP: 0, delayMin: 0, delayMax: 0 });
    board.openSlot();
    const slotId = [...board.slots.keys()][0];
    const first = await postClaim(slotId, 'claim-dup-1');
    assert.equal(first.status, 200);
    const replay = await postClaim(slotId, 'claim-dup-1');
    assert.equal(replay.status, 200);
    assert.deepEqual(replay.json, first.json);
    assert.equal(board.slots.get(slotId).claims.size, 1);
  });

  it('a second claimId on a claimed slot → 409 ALREADY_CLAIMED', async () => {
    await startBoard({ cancelEvery: 0, rejectFirstP: 0, delayMin: 0, delayMax: 0 });
    board.openSlot();
    const slotId = [...board.slots.keys()][0];
    const first = await postClaim(slotId, 'claim-race-a');
    assert.equal(first.status, 200);
    const second = await postClaim(slotId, 'claim-race-b');
    assert.equal(second.status, 409);
    assert.equal(second.json.error, 'ALREADY_CLAIMED');
    assert.equal(board.slots.get(slotId).claims.size, 1);
  });
});

describe('board delay', () => {
  it('fixed 300 ms delay: claim round-trip proves the delay, stays < 1500 ms', async () => {
    await startBoard({ cancelEvery: 0, rejectFirstP: 0, delayMin: 300, delayMax: 300 });
    board.openSlot();
    const slotId = [...board.slots.keys()][0];
    const t0 = Date.now();
    const res = await postClaim(slotId, 'claim-delay-1');
    const roundTrip = Date.now() - t0;
    assert.equal(res.status, 200);
    assert.ok(roundTrip >= 290, `round-trip ${roundTrip} ms should show the 300 ms board delay`);
    assert.ok(roundTrip < 1500, `round-trip ${roundTrip} ms should stay < 1500`);
  });
});

describe('scheduler', () => {
  it('calling openSlot directly does not orphan the scheduler timer', async () => {
    // With the bug, the timer pending from start() survived a direct
    // openSlot() and opened an extra slot ~100 ms later (two interleaved
    // chains). Fixed: exactly one chain, ~4 slots in 350 ms at 100 ms
    // intervals (1 direct + 3 scheduled).
    await startBoard({ intervalMin: 100, intervalMax: 100, cancelEvery: 0, rejectFirstP: 0, delayMin: 0, delayMax: 0 });
    board.openSlot();
    await new Promise((r) => setTimeout(r, 350));
    const n = board.dumpState().slots.length;
    assert.ok(n >= 3 && n <= 5, `expected 3–5 slots, got ${n}`);
  });
});

describe('stats', () => {
  it('computes success rate, p50/p95/max, cancelled claims and duplicates', () => {
    const log = [
      { event: 'claim-ok', claimMs: 100, detectMs: 5 },
      { event: 'claim-ok', claimMs: 200, detectMs: 8 },
      { event: 'claim-ok', claimMs: 300, detectMs: 10 },
      { event: 'claim-retry' },
    ];
    const state = {
      slots: [
        { id: 'slot-1', status: 'claimed', claimCount: 1 },
        { id: 'slot-2', status: 'claimed', claimCount: 1 },
        { id: 'slot-3', status: 'claimed', claimCount: 2 },
        { id: 'slot-4', status: 'expired', claimCount: 0 },
        { id: 'slot-5', status: 'cancelled', claimCount: 1 },
      ],
    };
    const s = computeStats(log, state);
    assert.equal(s.slotsTotal, 5);
    assert.equal(s.slotsValid, 4);
    assert.equal(s.slotsClaimed, 3);
    assert.equal(s.successRate, 0.75);
    assert.equal(s.latencyMs.p50, 200);
    assert.equal(s.latencyMs.p95, 300);
    assert.equal(s.latencyMs.max, 300);
    assert.equal(s.cancelledClaims, 1);
    assert.equal(s.duplicates, 1);
    assert.equal(s.retries, 1);
  });
});
