'use strict';

/**
 * Dry run for slotrace: board + claimer as SEPARATE processes, timed.
 *
 * Usage: node scripts/dry-run.js [--slots 30] [--port 18090] [--out logs]
 *
 * Writes timestamped logs:
 *   <out>/dry-run-<ts>.jsonl       (claimer log, monotonic timestamps)
 *   <out>/board-state-<ts>.json    (board final state)
 * Then prints statistics computed from those files by src/stats.js.
 */
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { computeStats } = require('../src/stats');

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i].startsWith('--')) {
      const key = argv[i].slice(2);
      out[key] = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : 'true';
      if (out[key] !== 'true') i += 1;
    }
  }
  return out;
}

function stamp() {
  return new Date().toISOString().replace(/[-:]/g, '').replace('T', '-').slice(0, 15);
}

function waitForExit(child, timeoutMs) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      try {
        child.kill('SIGKILL');
      } catch {
        // already dead
      }
      resolve(false);
    }, timeoutMs);
    child.once('exit', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const slots = Number(args.slots || 30);
  const port = Number(args.port || 18090);
  const outDir = args.out || path.join(__dirname, '..', 'logs');
  fs.mkdirSync(outDir, { recursive: true });
  const ts = stamp();
  const logPath = path.join(outDir, `dry-run-${ts}.jsonl`);
  const statePath = path.join(outDir, `board-state-${ts}.json`);
  const root = path.join(__dirname, '..');

  console.log(JSON.stringify({ event: 'dry-run-start', slots, port, logPath, statePath }));

  const board = spawn('node', ['src/board.js', '--port', String(port), '--max-slots', String(slots), '--state-out', statePath], { cwd: root });
  let boardOut = '';
  board.stdout.on('data', (d) => {
    boardOut += d.toString();
  });
  board.stderr.on('data', (d) => process.stderr.write(d));
  // Wait until the board reports listening.
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('board did not start in 5s')), 5000);
    const onData = () => {
      if (boardOut.includes('board-listening')) {
        clearTimeout(timer);
        board.stdout.off('data', onData);
        resolve();
      }
    };
    board.stdout.on('data', onData);
  });

  const claimer = spawn('node', ['src/claimer.js', '--board', `http://127.0.0.1:${port}`, '--log', logPath], { cwd: root });
  claimer.stderr.on('data', (d) => process.stderr.write(d));

  // Worst case: slots * 11 s max interval + 60 s buffer.
  const timeoutMs = slots * 12000 + 60000;
  const [boardCode, claimerCode] = await Promise.all([
    waitForExit(board, timeoutMs),
    waitForExit(claimer, timeoutMs),
  ]);
  if (boardCode === false || claimerCode === false) {
    console.error(JSON.stringify({ event: 'dry-run-timeout', boardCode, claimerCode }));
    process.exitCode = 1;
    return;
  }
  if (boardCode !== 0 || claimerCode !== 0) {
    console.error(JSON.stringify({ event: 'dry-run-child-failed', boardCode, claimerCode }));
    process.exitCode = 1;
    return;
  }

  const logEntries = fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
  const boardState = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  const stats = computeStats(logEntries, boardState);
  console.log(JSON.stringify(stats, null, 2));
  const ok =
    stats.successRate !== null && stats.successRate >= 0.9 &&
    stats.latencyMs.p95 !== null && stats.latencyMs.p95 < 1500 &&
    stats.cancelledClaims === 0 && stats.duplicates === 0;
  console.log(`targets: success>=90% ${stats.successRate !== null && stats.successRate >= 0.9 ? 'PASS' : 'FAIL'} | p95<1500ms ${stats.latencyMs.p95 !== null && stats.latencyMs.p95 < 1500 ? 'PASS' : 'FAIL'} | cancelled-claims=0 ${stats.cancelledClaims === 0 ? 'PASS' : 'FAIL'} | duplicates=0 ${stats.duplicates === 0 ? 'PASS' : 'FAIL'} | OVERALL ${ok ? 'PASS' : 'FAIL'}`);
  process.exitCode = ok ? 0 : 1;
}

main().catch((err) => {
  console.error(JSON.stringify({ event: 'dry-run-error', message: String(err.message || err) }));
  process.exitCode = 1;
});
