'use strict';

/**
 * slotrace claimer: subscribes to the board's SSE stream and claims every
 * slot it sees inside the 2.0 s window.
 *
 * - On slot-open: POST /claim immediately with a fresh claimId (UUID).
 * - On 409 RETRY: retry once with the SAME claimId after 50 ms, only
 *   while the window remains. The board treats the retry as the same
 *   claim — never a duplicate.
 * - On 410 (cancelled/expired): the slot is gone; log it, do not retry.
 * - On 200: verify with GET /slots/:id and log the verification.
 * - On slot-cancel: mark the slot dead locally and never claim it.
 * - On SSE drop (anything but board-done): reconnect, re-sync via
 *   GET /slots, resume.
 *
 * Every stage is logged as JSONL with wall-clock (Date.now, comparable
 * with the board's timestamps on the same machine) and monotonic
 * (performance.now, intra-process durations) timestamps.
 *
 * Runs standalone:
 *   node src/claimer.js --board http://127.0.0.1:18090 --log logs/dry-run.jsonl
 */
const http = require('http');
const crypto = require('crypto');
const { performance } = require('perf_hooks');

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

function httpJson(url, method, body, timeoutMs = 1000) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = http.request(
      { host: u.hostname, port: u.port, path: u.pathname + u.search, method, timeout: timeoutMs },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          let json = null;
          try {
            json = JSON.parse(text);
          } catch {
            // non-JSON body
          }
          resolve({ status: res.statusCode, json, text });
        });
      }
    );
    req.on('timeout', () => req.destroy(new Error('TIMEOUT')));
    req.on('error', reject);
    if (body !== undefined) {
      req.setHeader('Content-Type', 'application/json');
      req.end(JSON.stringify(body));
    } else {
      req.end();
    }
  });
}

class Claimer {
  constructor(opts = {}) {
    this.boardUrl = opts.boardUrl || 'http://127.0.0.1:18090';
    this.clientId = opts.clientId || `claimer-${crypto.randomUUID().slice(0, 8)}`;
    this.retryDelayMs = opts.retryDelayMs ?? 50;
    this.maxReconnects = opts.maxReconnects ?? 5;
    this.onLog = opts.onLog || (() => {});
    this.cancelled = new Set();
    this.claimed = new Set();
    this.attempted = new Set();
    this.reconnects = 0;
    this.done = false;
    this.sseReq = null;
    this.onDone = opts.onDone || null;
  }

  log(entry) {
    this.onLog({
      ts: Date.now(),
      mono: performance.now(),
      client: this.clientId,
      ...entry,
    });
  }

  async claim(slot) {
    const claimId = crypto.randomUUID();
    const tDetect = Date.now();
    const detectMs = tDetect - slot.openedAt;
    let attempts = 0;
    while (Date.now() < slot.validUntil - 20) {
      attempts += 1;
      const tSent = Date.now();
      let res;
      try {
        res = await httpJson(`${this.boardUrl}/claim`, 'POST', { slotId: slot.id, claimId, clientId: this.clientId });
      } catch (err) {
        this.log({ event: 'claim-error', slotId: slot.id, claimId, attempts, error: String(err.message || err) });
        break;
      }
      const tDone = Date.now();
      if (res.status === 200) {
        this.claimed.add(slot.id);
        this.log({
          event: 'claim-ok', slotId: slot.id, claimId, attempts,
          detectMs, claimMs: tDone - tDetect, sentMs: tDone - tSent,
        });
        // Verification stage (measured separately).
        const tVerifyStart = Date.now();
        try {
          const v = await httpJson(`${this.boardUrl}/slots/${slot.id}`, 'GET');
          this.log({
            event: 'claim-verified', slotId: slot.id, claimId,
            verifyMs: Date.now() - tVerifyStart,
            status: v.json && v.json.status,
            claimCount: v.json && v.json.claimCount,
          });
        } catch (err) {
          this.log({ event: 'verify-error', slotId: slot.id, error: String(err.message || err) });
        }
        return;
      }
      if (res.status === 409 && res.json && res.json.error === 'RETRY') {
        this.log({ event: 'claim-retry', slotId: slot.id, claimId, attempts, claimMs: tDone - tDetect });
        await new Promise((r) => setTimeout(r, this.retryDelayMs));
        continue;
      }
      this.log({
        event: 'claim-rejected', slotId: slot.id, claimId, attempts,
        status: res.status, error: res.json && res.json.error, claimMs: tDone - tDetect,
      });
      return;
    }
    this.log({ event: 'claim-window-missed', slotId: slot.id, claimId, attempts });
  }

  onEvent(ev) {
    if (ev.type === 'slot-open') {
      this.log({ event: 'slot-open', slotId: ev.slotId, seq: ev.seq, openedAt: ev.openedAt });
      if (this.cancelled.has(ev.slotId) || this.attempted.has(ev.slotId)) return;
      this.attempted.add(ev.slotId);
      this.claim({ id: ev.slotId, openedAt: ev.openedAt, validUntil: ev.validUntil });
      return;
    }
    if (ev.type === 'slot-cancel') {
      this.cancelled.add(ev.slotId);
      this.log({ event: 'slot-cancel', slotId: ev.slotId, cancelledAt: ev.cancelledAt });
      return;
    }
    if (ev.type === 'slot-expired') {
      this.log({ event: 'slot-expired', slotId: ev.slotId });
      return;
    }
    if (ev.type === 'board-done') {
      this.done = true;
      this.log({ event: 'board-done', slots: ev.slots });
      this.stop();
      if (this.onDone) this.onDone();
    }
  }

  connect() {
    const u = new URL(`${this.boardUrl}/stream`);
    let buf = '';
    this.sseReq = http.get({ host: u.hostname, port: u.port, path: '/stream' }, (res) => {
      res.on('data', (chunk) => {
        buf += chunk.toString('utf8');
        let idx;
        while ((idx = buf.indexOf('\n\n')) !== -1) {
          const raw = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          for (const line of raw.split('\n')) {
            if (!line.startsWith('data:')) continue;
            try {
              this.onEvent(JSON.parse(line.slice(5).trim()));
            } catch {
              // malformed event line; ignore
            }
          }
        }
      });
      res.on('end', () => this.onDisconnect());
    });
    this.sseReq.on('error', () => this.onDisconnect());
  }

  async onDisconnect() {
    if (this.done) return;
    if (this.reconnects >= this.maxReconnects) {
      this.log({ event: 'sse-gave-up', reconnects: this.reconnects });
      this.stop();
      if (this.onDone) this.onDone();
      return;
    }
    this.reconnects += 1;
    this.log({ event: 'sse-reconnect', attempt: this.reconnects });
    await new Promise((r) => setTimeout(r, 100));
    // Re-sync: claim any open slot we have not tried yet.
    try {
      const res = await httpJson(`${this.boardUrl}/slots`, 'GET');
      if (res.json && Array.isArray(res.json.slots)) {
        for (const s of res.json.slots) {
          if (s.status === 'open' && !this.attempted.has(s.id) && !this.cancelled.has(s.id)) {
            this.attempted.add(s.id);
            this.claim({ id: s.id, openedAt: s.openedAt, validUntil: s.validUntil });
          }
        }
      }
    } catch {
      // board unreachable; reconnect will retry
    }
    this.connect();
  }

  start() {
    this.connect();
  }

  stop() {
    this.done = true;
    if (this.sseReq) {
      try {
        this.sseReq.destroy();
      } catch {
        // already closed
      }
      this.sseReq = null;
    }
  }
}

if (require.main === module) {
  const args = parseArgs(process.argv.slice(2));
  const fs = require('fs');
  const logPath = args.log || null;
  const stream = logPath ? fs.createWriteStream(logPath, { flags: 'a' }) : null;
  const claimer = new Claimer({
    boardUrl: args.board || 'http://127.0.0.1:18090',
    onLog: (entry) => {
      const line = JSON.stringify(entry);
      if (stream) stream.write(`${line}\n`);
      else console.log(line);
    },
    onDone: () => {
      console.log(JSON.stringify({ event: 'claimer-done' }));
      if (stream) stream.end(() => process.exit(0));
      else process.exit(0);
    },
  });
  claimer.start();
}

module.exports = { Claimer, httpJson };
