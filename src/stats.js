'use strict';

/**
 * Statistics for a slotrace dry run, computed from the claimer's JSONL log
 * and the board's final state dump. Never hand-estimated.
 *
 * - valid slots: opened and never cancelled (board state).
 * - claimed: board status 'claimed' with exactly one claim.
 * - success rate: claimed valid / valid slots.
 * - latencies: claim-ok claimMs (detection → claim response).
 * - cancelled claims: cancelled slots with a recorded claim (must be 0).
 * - duplicates: claims recorded beyond the first per slot (must be 0).
 *
 * Usage: node src/stats.js <log.jsonl> <board-state.json>
 */
const fs = require('fs');

function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)];
}

function computeStats(logEntries, boardState) {
  const slots = new Map(boardState.slots.map((s) => [s.id, s]));
  const valid = [...slots.values()].filter((s) => s.status !== 'cancelled');
  const claimed = valid.filter((s) => s.status === 'claimed');
  const cancelled = [...slots.values()].filter((s) => s.status === 'cancelled');
  const cancelledClaims = cancelled.filter((s) => s.claimCount > 0).length;
  const duplicates = [...slots.values()].reduce((sum, s) => sum + Math.max(0, s.claimCount - 1), 0);
  const latencies = logEntries
    .filter((e) => e.event === 'claim-ok')
    .map((e) => e.claimMs)
    .sort((a, b) => a - b);
  const detects = logEntries
    .filter((e) => e.event === 'claim-ok')
    .map((e) => e.detectMs)
    .sort((a, b) => a - b);
  const retries = logEntries.filter((e) => e.event === 'claim-retry').length;
  const rejected = logEntries.filter((e) => e.event === 'claim-rejected').length;
  return {
    slotsTotal: slots.size,
    slotsValid: valid.length,
    slotsCancelled: cancelled.length,
    slotsClaimed: claimed.length,
    successRate: valid.length ? claimed.length / valid.length : null,
    cancelledClaims,
    duplicates,
    retries,
    rejected,
    latencyMs: {
      n: latencies.length,
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      max: latencies.length ? latencies[latencies.length - 1] : null,
    },
    detectMs: {
      n: detects.length,
      p50: percentile(detects, 50),
      p95: percentile(detects, 95),
      max: detects.length ? detects[detects.length - 1] : null,
    },
  };
}

if (require.main === module) {
  const [logPath, statePath] = process.argv.slice(2);
  if (!logPath || !statePath) {
    console.error('usage: node src/stats.js <log.jsonl> <board-state.json>');
    process.exitCode = 1;
  } else {
    const logEntries = fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
    const boardState = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    const stats = computeStats(logEntries, boardState);
    console.log(JSON.stringify(stats, null, 2));
    const ok =
      stats.successRate !== null && stats.successRate >= 0.9 &&
      stats.latencyMs.p95 !== null && stats.latencyMs.p95 < 1500 &&
      stats.cancelledClaims === 0 && stats.duplicates === 0;
    console.log(`targets: success>=90% ${stats.successRate !== null && stats.successRate >= 0.9 ? 'PASS' : 'FAIL'} | p95<1500ms ${stats.latencyMs.p95 !== null && stats.latencyMs.p95 < 1500 ? 'PASS' : 'FAIL'} | cancelled-claims=0 ${stats.cancelledClaims === 0 ? 'PASS' : 'FAIL'} | duplicates=0 ${stats.duplicates === 0 ? 'PASS' : 'FAIL'} | OVERALL ${ok ? 'PASS' : 'FAIL'}`);
  }
}

module.exports = { computeStats, percentile };
