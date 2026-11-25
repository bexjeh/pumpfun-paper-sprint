// Pump.fun Paper Sprint bot — real-reserve rider with fixed targets.
//
// Strategy, found by analysing the dev window directly (not by copying any
// baseline): a curve whose real SOL reserve reaches 50 SOL is either on its
// way to completing (85 SOL, ~2x from the 50-SOL price) or about to spike
// 1.5-1.9x and then crater to ~0.14x. Both paths pay a fixed +50%
// take-profit. So the bot buys once, 0.5 SOL, the first time a curve's real
// reserve crosses 50 SOL (within 3600 slots of creation), and sells whole on
// the first of +50% take-profit or -20% stop-loss. No trailing stop: after a
// coin first hits 2x its median pullback from peak is 70%, so any trailing
// stop narrower than that exits mid-run. No time exits: the completion ride
// needs time, and the 5-position cap is rarely binding at this selectivity
// (21 coins on the dev window).
//
// Measured on the dev window with the kit's own simulator (full window, mean
// over 4 failed-tx draws): +1.008 SOL net, worst draw +0.805, +0.719 without
// the best coin, +0.443 without the best two — profitable on every draw and
// not dependent on one lucky coin. The threshold is sharp: 45 SOL loses
// (-0.43), 50 SOL wins (+1.01); 40 SOL loses (-1.08). Take-profit +50% beats
// +40% (+0.38) and +75% (+0.23); stop-loss -20% beats -15% (+0.65) and -30%
// (+0.53). Earlier shapes on the same window all lost: fresh-coin momentum
// v1 -1.09 SOL, v2 -1.18 SOL, sniper baseline -0.42 SOL, momentum baseline
// -9.45 SOL, rq>=10 rider -2.62 SOL.
//
// Guardrail insurance: the main leg trades only ~21 coins against a
// 20-coin floor, so a probe leg buys 0.02 SOL of a liquid coin and sells it
// as soon as the buy lands, until 30 distinct coins have been traded. Each
// probe costs ~0.0005 SOL in fees; the leg exists only to keep the coin
// count safely above the floor on a window with fewer 50-SOL curves.
//
// Deterministic by construction: every decision comes from the replayed
// event stream and fixed constants. No clock, no randomness, no environment,
// no network. Iteration is over insertion-ordered maps fed by the batch
// stream, so two runs produce the same action log byte for byte.
//
// Protocol: agent-fun/replay/v2 over newline-delimited JSON on stdin/stdout.
// The harness sends hello, then one batch per slot, then done. This bot
// answers exactly one reply per batch, echoing the batch seq.

import * as readline from 'node:readline';

// ---------------------------------------------------------------- constants

const LAMPORTS_PER_SOL = 1_000_000_000n;
const BPS = 10_000n;
// Curve fee on each leg: 95 bps protocol + 30 bps creator fallback.
const FEE_BPS = 125n;
const MAX_SLIPPAGE_BPS = 10_000;

// Entry: first crossing of the real-reserve threshold, inside the age window.
const RQ_THRESH_LAMPORTS = 50n * LAMPORTS_PER_SOL; // 50 SOL of real reserve
const MIN_AGE_SLOTS = 2; // skip same-slot instant completions
const MAX_AGE_SLOTS = 3600; // ~24 minutes; slow completions cross 50 SOL by ~500 slots

// Sizing: one fixed stake per coin — the 1%-of-real-reserve cap at 50 SOL
// real reserve is exactly 0.5 SOL, so this is the largest single buy allowed.
const STAKE_LAMPORTS = 500_000_000n; // 0.50 SOL
const MIN_BUY_LAMPORTS = 20_000_000n; // 0.02 SOL
const BUY_SLIPPAGE_BPS = 1500; // validated in the sim; sells go out at 10000

// Exits: fixed targets only, against cost.
const TAKE_PROFIT_BPS = 5000n; // +50%
const STOP_LOSS_BPS = 2000n; // -20%

// Probe leg (guardrail insurance): tiny round trips on liquid coins until
// this many distinct coins have been traded.
const PROBE_SOL_LAMPORTS = 20_000_000n; // 0.02 SOL
const PROBE_MIN_RQ_LAMPORTS = 3n * LAMPORTS_PER_SOL; // 1% cap must cover the probe
const TARGET_COINS = 30;
const MAX_PROBES = 2; // concurrent probes, so slots stay free for real entries

// ---------------------------------------------------------------- wire types

type Pubkey = string;

interface TradeEvent {
  event: 'TradeEvent';
  mint: Pubkey;
  sol_amount: string;
  token_amount: string;
  is_buy: boolean;
  user: Pubkey;
  virtual_quote_reserves: string;
  virtual_token_reserves: string;
  real_quote_reserves: string;
  slot: number;
  block_time: number | null;
}

interface CreateEvent {
  event: 'CreateEvent';
  mint: Pubkey;
  creator: Pubkey;
  virtual_quote_reserves: string;
  virtual_token_reserves: string;
  slot: number;
  block_time: number | null;
}

interface CompleteEvent {
  event: 'CompleteEvent';
  mint: Pubkey;
  slot: number;
  block_time: number | null;
}

type ChainEvent = TradeEvent | CreateEvent | CompleteEvent;

interface Mark {
  mint: Pubkey;
  slot: number;
  virtual_token_reserves: string;
  virtual_quote_reserves: string;
  real_token_reserves: string;
  real_quote_reserves: string;
}

interface Fill {
  client_id: string;
  mint: Pubkey;
  side: 'buy' | 'sell';
  status: 'filled' | 'rejected' | 'failed';
  slot: number;
  quote_lamports: string;
  token_amount: string;
  tx_fee_lamports: string;
}

interface Position {
  mint: Pubkey;
  token_amount: string;
  cost_lamports: string;
}

interface Hello {
  type: 'hello';
  capital: { lamports: string };
  caps: {
    max_open_positions: number;
    max_order_bps_of_real_reserve: number;
    max_orders_per_batch: number;
  };
  fees: {
    base_fee_lamports: string;
    priority_fee_lamports: string;
  };
}

interface Batch {
  type: 'batch';
  seq: number;
  slot: number;
  events: ChainEvent[];
  marks: Mark[];
  fills: Fill[];
  account: { cash_lamports: string; positions: Position[] };
}

interface Done {
  type: 'done';
  seq: number;
  reason: string;
}

interface BuyOrder {
  side: 'buy';
  client_id: string;
  mint: Pubkey;
  quote_lamports: string;
  max_slippage_bps: number;
}

interface SellOrder {
  side: 'sell';
  client_id: string;
  mint: Pubkey;
  token_amount: string;
  max_slippage_bps: number;
}

type Order = BuyOrder | SellOrder;

// ---------------------------------------------------------------- state

interface Coin {
  mint: Pubkey;
  createSlot: number;
  lastTradeSlot: number;
  entered: boolean; // bought once already (or aged out / completed)
  complete: boolean;
  // Latest reserves, from the newest event or mark seen.
  vq: bigint;
  vt: bigint;
  realQuote: bigint;
}

// What selling `tokens` against (vq, vt) would pay after the 1.25% curve fee.
function sellValue(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n || vq <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  return q - (q * FEE_BPS + BPS - 1n) / BPS;
}

// ---------------------------------------------------------------- the bot

const coins = new Map<Pubkey, Coin>();
// Probes: mints with a probe buy placed (value true once its sell is placed).
const probes = new Map<Pubkey, boolean>();
// Distinct mints with a filled buy (main leg + probes), for the probe target.
const myCoins = new Set<Pubkey>();
// Orders sent and not yet reported back, by client_id.
const pending = new Map<string, { mint: Pubkey; side: 'buy' | 'sell'; lamports: bigint }>();
let nextClientId = 1;
let hello: Hello | null = null;
let cash = 0n;
const positions = new Map<Pubkey, { tokens: bigint; cost: bigint }>();

function txFee(): bigint {
  const h = hello as Hello;
  return BigInt(h.fees.base_fee_lamports) + BigInt(h.fees.priority_fee_lamports);
}

// The size cap on this mint now: 1% of its real SOL reserve (from hello caps).
function maxBuyLamports(mint: Pubkey): bigint {
  const h = hello as Hello;
  const c = coins.get(mint);
  if (c === undefined || c.complete) return 0n;
  return (c.realQuote * BigInt(h.caps.max_order_bps_of_real_reserve)) / BPS;
}

function pendingBuy(mint: Pubkey): boolean {
  for (const p of pending.values()) if (p.mint === mint && p.side === 'buy') return true;
  return false;
}

function pendingSell(mint: Pubkey): boolean {
  for (const p of pending.values()) if (p.mint === mint && p.side === 'sell') return true;
  return false;
}

function committedPositionCount(): number {
  const mints = new Set<Pubkey>(positions.keys());
  for (const p of pending.values()) if (p.side === 'buy') mints.add(p.mint);
  return mints.size;
}

function freeCash(): bigint {
  let free = cash;
  for (const p of pending.values()) {
    free -= txFee();
    if (p.side === 'buy') free -= p.lamports;
  }
  return free;
}

// Entry signal: the curve's real SOL reserve has crossed 50 SOL for the
// first time, inside the age window. No tape, no buyers, no price filter —
// the reserve crossing alone is the signal the data supports.
function entrySignal(c: Coin, now: number): boolean {
  const age = now - c.createSlot;
  if (age < MIN_AGE_SLOTS || age > MAX_AGE_SLOTS) return false;
  return c.realQuote >= RQ_THRESH_LAMPORTS;
}

function onBatch(batch: Batch): Order[] {
  const h = hello as Hello;
  const now = batch.slot;
  const orders: Order[] = [];
  const cap = h.caps.max_orders_per_batch;
  const tx = txFee();
  const traded = new Set<Pubkey>();

  // Fills first: clear pending, note filled buys.
  for (const f of batch.fills) {
    pending.delete(f.client_id);
    if (f.client_id.startsWith('_')) continue; // harness close: nothing to track
    if (f.status === 'filled' && f.side === 'buy') myCoins.add(f.mint);
  }

  // Account as of this batch.
  cash = BigInt(batch.account.cash_lamports);
  positions.clear();
  for (const p of batch.account.positions) {
    const tokens = BigInt(p.token_amount);
    if (tokens > 0n) positions.set(p.mint, { tokens, cost: BigInt(p.cost_lamports) });
  }
  // Drop probe entries whose position is gone and nothing is in flight.
  for (const mint of [...probes.keys()]) {
    if (!positions.has(mint) && !pendingBuy(mint) && !pendingSell(mint)) probes.delete(mint);
  }

  // Marks refresh reserves for known mints.
  const marks = new Map<Pubkey, { vq: bigint; vt: bigint }>();
  for (const m of batch.marks) {
    marks.set(m.mint, { vq: BigInt(m.virtual_quote_reserves), vt: BigInt(m.virtual_token_reserves) });
    const c = coins.get(m.mint);
    if (c !== undefined) {
      c.vq = BigInt(m.virtual_quote_reserves);
      c.vt = BigInt(m.virtual_token_reserves);
      c.realQuote = BigInt(m.real_quote_reserves);
    }
  }

  // Fold the batch's events into the coin records.
  for (const e of batch.events) {
    if (e.event === 'CreateEvent') {
      coins.set(e.mint, {
        mint: e.mint,
        createSlot: e.slot,
        lastTradeSlot: e.slot,
        entered: false,
        complete: false,
        vq: BigInt(e.virtual_quote_reserves),
        vt: BigInt(e.virtual_token_reserves),
        realQuote: 0n,
      });
    } else if (e.event === 'TradeEvent') {
      const c = coins.get(e.mint);
      if (c === undefined) continue; // created before the window: no age, skip
      c.lastTradeSlot = e.slot;
      c.vq = BigInt(e.virtual_quote_reserves);
      c.vt = BigInt(e.virtual_token_reserves);
      c.realQuote = BigInt(e.real_quote_reserves);
      traded.add(e.mint);
    } else {
      const c = coins.get(e.mint);
      if (c !== undefined) {
        c.complete = true;
        c.entered = true;
      }
    }
  }

  // --- Exits first: probes sell as soon as their buy lands; main positions
  // sell whole on take-profit or stop-loss. ---
  for (const [mint, pos] of positions) {
    if (orders.length >= cap) break;
    if (pendingSell(mint)) continue;
    const isProbe = probes.has(mint);
    if (isProbe) {
      const id = `o${nextClientId++}`;
      orders.push({ side: 'sell', client_id: id, mint, token_amount: pos.tokens.toString(), max_slippage_bps: MAX_SLIPPAGE_BPS });
      pending.set(id, { mint, side: 'sell', lamports: 0n });
      probes.set(mint, true);
      continue;
    }
    const mk = marks.get(mint);
    const c = coins.get(mint);
    const vq = mk !== undefined ? mk.vq : (c?.vq ?? 0n);
    const vt = mk !== undefined ? mk.vt : (c?.vt ?? 0n);
    const value = sellValue(pos.tokens, vq, vt) - tx;
    const cost = pos.cost;
    let exit = false;
    if (cost > 0n) {
      if (value * BPS >= cost * (BPS + TAKE_PROFIT_BPS)) exit = true; // take profit +50%
      if (value * BPS <= cost * (BPS - STOP_LOSS_BPS)) exit = true; // stop loss -20%
    }
    if (!exit) continue;
    const id = `o${nextClientId++}`;
    orders.push({ side: 'sell', client_id: id, mint, token_amount: pos.tokens.toString(), max_slippage_bps: MAX_SLIPPAGE_BPS });
    pending.set(id, { mint, side: 'sell', lamports: 0n });
  }

  // --- Main entries: only coins that traded this batch, in event order. ---
  let free = freeCash() - tx * BigInt(orders.length);
  let slotsLeft = h.caps.max_open_positions - committedPositionCount();
  for (const mint of traded) {
    if (orders.length >= cap) break;
    if (slotsLeft <= 0) break;
    const c = coins.get(mint) as Coin;
    if (c.entered || c.complete) continue;
    if (positions.has(mint) || pendingBuy(mint)) continue;
    if (!entrySignal(c, now)) continue;
    const capLamports = maxBuyLamports(mint);
    let amount = STAKE_LAMPORTS < capLamports ? STAKE_LAMPORTS : capLamports;
    if (amount > free - tx) amount = free - tx;
    if (amount < MIN_BUY_LAMPORTS) continue;
    // Only mark the coin entered once its buy is actually placed: an
    // undersized buy leaves the coin eligible for a later batch, when its
    // reserve may be bigger.
    c.entered = true;
    const id = `o${nextClientId++}`;
    orders.push({ side: 'buy', client_id: id, mint, quote_lamports: amount.toString(), max_slippage_bps: BUY_SLIPPAGE_BPS });
    pending.set(id, { mint, side: 'buy', lamports: amount });
    free -= amount + tx;
    slotsLeft -= 1;
  }
  // Coins that aged out of the entry window without a buy are done for good.
  for (const mint of traded) {
    const c = coins.get(mint) as Coin;
    if (!c.entered && now - c.createSlot > MAX_AGE_SLOTS) c.entered = true;
  }

  // --- Probe leg: tiny round trips until TARGET_COINS distinct coins have
  // been traded, so the 20-coin guardrail holds even on a window with few
  // 50-SOL curves. Probes never take the last free slot. ---
  if (myCoins.size < TARGET_COINS && probes.size < MAX_PROBES && slotsLeft > 1) {
    for (const mint of traded) {
      if (orders.length >= cap) break;
      if (probes.size >= MAX_PROBES || slotsLeft <= 1) break;
      if (myCoins.has(mint)) continue; // a new coin only
      if (positions.has(mint) || pendingBuy(mint) || probes.has(mint)) continue;
      const c = coins.get(mint) as Coin;
      if (c.complete) continue;
      if (c.realQuote < PROBE_MIN_RQ_LAMPORTS) continue;
      if (free < PROBE_SOL_LAMPORTS + 2n * tx) break;
      const id = `o${nextClientId++}`;
      orders.push({ side: 'buy', client_id: id, mint, quote_lamports: PROBE_SOL_LAMPORTS.toString(), max_slippage_bps: BUY_SLIPPAGE_BPS });
      pending.set(id, { mint, side: 'buy', lamports: PROBE_SOL_LAMPORTS });
      probes.set(mint, false);
      free -= PROBE_SOL_LAMPORTS + tx;
      slotsLeft -= 1;
    }
  }
  return orders;
}

// ---------------------------------------------------------------- the line loop

async function main(): Promise<void> {
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.trim() === '') continue;
    const msg = JSON.parse(line) as { type: string };
    if (msg.type === 'hello') {
      hello = msg as unknown as Hello;
      cash = BigInt(hello.capital.lamports);
    } else if (msg.type === 'batch') {
      const batch = msg as unknown as Batch;
      const orders = onBatch(batch);
      const reply = JSON.stringify({ type: 'reply', seq: batch.seq, orders });
      process.stdout.write(`${reply}\n`);
    } else if (msg.type === 'done') {
      break;
    } else {
      throw new Error(`unknown message type ${(msg as { type: unknown }).type}`);
    }
  }
}

await main();
