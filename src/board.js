'use strict';

/**
 * slotrace board: opens FCFS claim slots on a schedule and judges claims.
 *
 * - A slot opens every 8±3 s (uniform 5–11 s), valid for 2.0 s.
 * - Every 5th slot is cancelled before its window ends (cancel fires
 *   0.3–1.5 s after open, announced via SSE). The board is authoritative:
 *   a claim on a cancelled slot is rejected 410 even if the claimer has
 *   not seen the cancel event yet.
 * - 10% of first claim attempts per slot are rejected 409 RETRY.
 *   Retrying with the same claimId succeeds (idempotent) and never
 *   creates a duplicate claim.
 * - Every REST response is delayed 0–300 ms (uniform random).
 * - POST /claim is idempotent via claimId: replaying a claimId returns
 *   the stored result, never a second claim.
 *
 * Zero dependencies, plain node:http. Runs standalone:
 *   node src/board.js --port 18090 --max-slots 30 --state-out logs/board-state.json
 */
const http = require('http');

const SLOT_VALID_MS = 2000;

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

class Board {
  constructor(opts = {}) {
    this.port = opts.port ?? 18090;
    this.intervalMin = opts.intervalMin ?? 5000;
    this.intervalMax = opts.intervalMax ?? 11000;
    this.slotValidMs = opts.slotValidMs ?? SLOT_VALID_MS;
    this.cancelEvery = opts.cancelEvery ?? 5;
    this.cancelDelayMin = opts.cancelDelayMin ?? 50;
    this.cancelDelayMax = opts.cancelDelayMax ?? 500;
    this.rejectFirstP = opts.rejectFirstP ?? 0.1;
    this.delayMin = opts.delayMin ?? 0;
    this.delayMax = opts.delayMax ?? 300;
    this.maxSlots = opts.maxSlots ?? Infinity;
    this.random = opts.random || Math.random;
    this.slots = new Map();
    this.sseClients = new Set();
    this.seq = 0;
    this.server = null;
    this.timer = null;
    this.stopped = false;
    this.onDone = opts.onDone || null;
  }

  randomDelayMs() {
    return this.delayMin + this.random() * (this.delayMax - this.delayMin);
  }

  randomIntervalMs() {
    return this.intervalMin + this.random() * (this.intervalMax - this.intervalMin);
  }

  broadcast(event) {
    const line = `data: ${JSON.stringify(event)}\n\n`;
    for (const res of this.sseClients) {
      try {
        res.write(line);
      } catch {
        // client gone; pruned on close
      }
    }
  }

  openSlot() {
    if (this.stopped) return;
    // Clear any pending scheduler timer first: openSlot() can be called
    // directly (tests) while a timer is already pending — without this the
    // old timer is orphaned and opens an extra slot later.
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.seq += 1;
    const seq = this.seq;
    const openedAt = Date.now();
    const slot = {
      id: `slot-${seq}`,
      seq,
      openedAt,
      validUntil: openedAt + this.slotValidMs,
      status: 'open',
      willCancel: this.cancelEvery > 0 && seq % this.cancelEvery === 0,
      cancelledAt: null,
      claims: new Map(),
      attempts: [],
      firstAttempted: false,
      claimedBy: null,
    };
    this.slots.set(slot.id, slot);
    this.broadcast({ type: 'slot-open', slotId: slot.id, seq, openedAt, validUntil: slot.validUntil });

    if (slot.willCancel) {
      const delay = this.cancelDelayMin + this.random() * (this.cancelDelayMax - this.cancelDelayMin);
      setTimeout(() => {
        if (slot.status !== 'open') return;
        slot.status = 'cancelled';
        slot.cancelledAt = Date.now();
        this.broadcast({ type: 'slot-cancel', slotId: slot.id, cancelledAt: slot.cancelledAt });
      }, delay);
    }
    setTimeout(() => {
      if (slot.status === 'open') {
        slot.status = 'expired';
        this.broadcast({ type: 'slot-expired', slotId: slot.id });
      }
    }, this.slotValidMs + 50);

    if (seq >= this.maxSlots) {
      // Last slot opened: finish after its window closes.
      setTimeout(() => this.finish(), this.slotValidMs + 1000);
      return;
    }
    this.timer = setTimeout(() => this.openSlot(), this.randomIntervalMs());
  }

  slotSummary(slot) {
    return {
      id: slot.id,
      seq: slot.seq,
      openedAt: slot.openedAt,
      validUntil: slot.validUntil,
      status: slot.status,
      cancelledAt: slot.cancelledAt,
      claimCount: [...slot.claims.values()].filter((c) => c.claimed).length,
      attempts: slot.attempts.length,
    };
  }

  dumpState() {
    return {
      generatedAt: Date.now(),
      slots: [...this.slots.values()].map((s) => this.slotSummary(s)),
    };
  }

  async readJsonBody(req) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > 10240) throw new Error('PAYLOAD_TOO_LARGE');
      chunks.push(chunk);
    }
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  }

  sendJson(res, status, obj) {
    const body = JSON.stringify(obj);
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(body);
  }

  delayed(fn) {
    setTimeout(fn, this.randomDelayMs());
  }

  handleClaim(req, res) {
    this.readJsonBody(req).then((body) => {
      this.delayed(() => {
        const { slotId, claimId, clientId } = body || {};
        const slot = this.slots.get(slotId);
        if (!slot) return this.sendJson(res, 404, { error: 'SLOT_NOT_FOUND', slotId });
        // Idempotency first: a replayed claimId returns its stored result.
        if (claimId && slot.claims.has(claimId)) {
          const stored = slot.claims.get(claimId);
          return this.sendJson(res, stored.code, stored.body);
        }
        slot.attempts.push({ claimId, clientId, at: Date.now() });
        if (slot.status === 'cancelled') {
          return this.sendJson(res, 410, { error: 'CANCELLED', slotId });
        }
        if (slot.status === 'claimed') {
          return this.sendJson(res, 409, { error: 'ALREADY_CLAIMED', slotId });
        }
        if (slot.status === 'expired' || Date.now() > slot.validUntil) {
          slot.status = 'expired';
          return this.sendJson(res, 410, { error: 'EXPIRED', slotId });
        }
        if (!slot.firstAttempted) {
          slot.firstAttempted = true;
          if (this.random() < this.rejectFirstP) {
            return this.sendJson(res, 409, { error: 'RETRY', slotId, message: 'first attempt rejected, retry with the same claimId' });
          }
        }
        const claimedAt = Date.now();
        const result = { claimed: true, slotId, claimId, claimedAt };
        slot.claims.set(claimId, { claimed: true, code: 200, body: result });
        slot.status = 'claimed';
        slot.claimedBy = clientId || null;
        this.broadcast({ type: 'slot-claimed', slotId, claimId, claimedAt });
        return this.sendJson(res, 200, result);
      });
    }).catch(() => this.sendJson(res, 400, { error: 'BAD_REQUEST' }));
  }

  start() {
    return new Promise((resolve) => {
      this.server = http.createServer((req, res) => {
        const url = new URL(req.url, `http://127.0.0.1:${this.port}`);
        if (req.method === 'GET' && url.pathname === '/stream') {
          res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            Connection: 'keep-alive',
          });
          res.write('data: {"type":"hello"}\n\n');
          this.sseClients.add(res);
          req.on('close', () => this.sseClients.delete(res));
          return;
        }
        if (req.method === 'POST' && url.pathname === '/claim') return this.handleClaim(req, res);
        if (req.method === 'GET' && url.pathname === '/slots') {
          return this.delayed(() => this.sendJson(res, 200, { slots: [...this.slots.values()].map((s) => this.slotSummary(s)) }));
        }
        if (req.method === 'GET' && url.pathname.startsWith('/slots/')) {
          const slot = this.slots.get(decodeURIComponent(url.pathname.slice('/slots/'.length)));
          if (!slot) return this.delayed(() => this.sendJson(res, 404, { error: 'SLOT_NOT_FOUND' }));
          return this.delayed(() => this.sendJson(res, 200, this.slotSummary(slot)));
        }
        if (req.method === 'GET' && url.pathname === '/health') {
          return this.sendJson(res, 200, { ok: true, slots: this.slots.size });
        }
        return this.sendJson(res, 404, { error: 'NOT_FOUND' });
      });
      this.server.listen(this.port, '127.0.0.1', () => {
        this.timer = setTimeout(() => this.openSlot(), this.randomIntervalMs());
        resolve();
      });
    });
  }

  finish() {
    if (this.stopped) return;
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.broadcast({ type: 'board-done', slots: this.slots.size });
    // Give in-flight claim verifications a moment before closing.
    setTimeout(() => this.stop(), 1000);
    if (this.onDone) this.onDone(this.dumpState());
  }

  stop() {
    return new Promise((resolve) => {
      this.stopped = true;
      if (this.timer) clearTimeout(this.timer);
      for (const res of this.sseClients) {
        try {
          res.end();
        } catch {
          // already closed
        }
      }
      this.sseClients.clear();
      if (this.server) {
        this.server.close(() => resolve());
      } else {
        resolve();
      }
    });
  }
}

if (require.main === module) {
  const args = parseArgs(process.argv.slice(2));
  const fs = require('fs');
  const board = new Board({
    port: Number(args.port || 18090),
    maxSlots: args['max-slots'] ? Number(args['max-slots']) : Infinity,
    onDone: (state) => {
      if (args['state-out']) {
        fs.writeFileSync(args['state-out'], `${JSON.stringify(state, null, 2)}\n`);
      }
      console.log(JSON.stringify({ event: 'board-done', slots: state.slots.length }));
    },
  });
  board.start().then(() => {
    console.log(JSON.stringify({ event: 'board-listening', port: board.port }));
  });
}

module.exports = { Board, SLOT_VALID_MS };
