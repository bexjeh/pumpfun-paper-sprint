// Pump.fun Paper Sprint bot — fresh-coin momentum with fast exits.
//
// Strategy: watch every bonding curve from its CreateEvent. Buy a coin once, after it
// has survived its first minute, when its recent tape shows real buy pressure — net
// SOL flowing in, several distinct buyers, price rising and sitting near its window
// high — and the creator has not sold. One fixed stake per coin, never more than the
// open-position cap. Every position exits whole on the first of: stop-loss, a
// trailing stop once the position is up, a max hold time that only bites while the
// position is down, the creator selling, or the coin going stale. There is no fixed
// take-profit: pump.fun returns are fat-tailed, so winners are left to run under the
// trailing stop instead of being capped. Positions that complete are closed by the
// replay itself.
//
// Revision (measured on the dev window): the first build capped winners at +20%
// while taking -15% losses and lost -1.09 SOL over 307 coins at a 29% win rate.
// The pilot baseline's swept parameters agree — no take-profit, trailing stop only,
// stricter entry, a minimum coin age. This revision adopts that shape: no fixed
// take-profit, trailing arm +5% / trail 30%, stop-loss -20%, entry only after 60
// slots with stronger buy pressure, and the max-hold exit skipped while a position
// is up.
//
// Deterministic by construction: every decision comes from the replayed event stream
// and fixed constants. No clock, no randomness, no environment, no network. Iteration
// is over insertion-ordered maps fed by the batch stream, so two runs produce the
// same action log byte for byte.
//
// Protocol: agent-fun/replay/v2 over newline-delimited JSON on stdin/stdout.
// The harness sends hello, then one batch per slot, then done. This bot answers
// exactly one reply per batch, echoing the batch seq.

import * as readline from 'node:readline';

// ---------------------------------------------------------------- constants

const LAMPORTS_PER_SOL = 1_000_000_000n;
const BPS = 10_000n;
// Curve fee on each leg: 95 bps protocol + 30 bps creator fallback.
const FEE_BPS = 125n;
const MAX_SLIPPAGE_BPS = 10_000;

// Entry: the tape window and what it must show.
const WINDOW_SLOTS = 60;
const MIN_AGE_SLOTS = 60;
const MAX_AGE_SLOTS = 900;
const MIN_NET_SOL = 1.0; // net buy SOL (buys minus sells) over the window
const MIN_BUYS = 3; // buy trades in the window
const MIN_BUYERS = 5; // distinct non-creator buyers in the window
const MIN_PRICE_UP = 0.05; // price now vs window-start price, +5%
const NEAR_HIGH = 0.95; // price now at least 95% of the window high
const MIN_REAL_SOL = 2; // real SOL reserve band at entry
const MAX_REAL_SOL = 60;
const MAX_DEV_BUY_SOL = 2.5; // creator's own buys in the create slot, at most

// Sizing: one fixed stake per coin.
const STAKE_LAMPORTS = 100_000_000n; // 0.10 SOL
const MIN_BUY_LAMPORTS = 20_000_000n; // 0.02 SOL

// Exits (bps against cost, or slots held). No fixed take-profit: winners run
// under the trailing stop; the max-hold exit only bites while a position is down.
const STOP_LOSS_BPS = 2000n; // -20%
const TRAIL_ARM_BPS = 500n; // trailing stop arms once up 5%
const TRAIL_BPS = 3000n; // ... then exits on a 30% fall from the peak value
const MAX_HOLD_SLOTS = 1500; // ~10 minutes
const STALE_SLOTS = 600; // no trade for this long: exit

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

interface Tick {
  slot: number;
  isBuy: boolean;
  sol: number; // SOL
  user: Pubkey;
  price: number; // vq / vt
}

interface Coin {
  mint: Pubkey;
  createSlot: number;
  creator: Pubkey;
  devBuySol: number; // creator's buys in the create slot
  devSold: boolean;
  trades: number;
  lastTradeSlot: number;
  recent: Tick[]; // trades inside the entry window (pruned as new ones come)
  entered: boolean; // bought once already (or decided against for good)
  complete: boolean;
  // Latest reserves, from the newest event or mark seen.
  vq: bigint;
  vt: bigint;
  realQuote: bigint;
}

interface Held {
  openedSlot: number;
  peakValue: bigint; // best close value seen (lamports, fees deducted)
  devSoldAtOpen: boolean;
}

// What selling `tokens` against (vq, vt) would pay after the 1.25% curve fee.
function sellValue(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n || vq <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  return q - (q * FEE_BPS + BPS - 1n) / BPS;
}

// ---------------------------------------------------------------- the bot

const coins = new Map<Pubkey, Coin>();
const held = new Map<Pubkey, Held>();
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

// Entry signal on a coin's tape at slot `now`.
function entrySignal(c: Coin, now: number): boolean {
  const age = now - c.createSlot;
  if (age < MIN_AGE_SLOTS || age > MAX_AGE_SLOTS) return false;
  if (c.devSold) return false;
  if (c.devBuySol > MAX_DEV_BUY_SOL) return false;
  const rqSol = Number(c.realQuote) / 1e9;
  if (rqSol < MIN_REAL_SOL || rqSol > MAX_REAL_SOL) return false;
  const from = now - WINDOW_SLOTS;
  let buySol = 0;
  let sellSol = 0;
  let buys = 0;
  let startPrice = -1;
  let high = 0;
  let last = 0;
  const buyers = new Set<Pubkey>();
  for (const t of c.recent) {
    if (t.slot <= from) {
      startPrice = t.price;
      continue;
    }
    if (startPrice < 0) startPrice = t.price;
    if (t.isBuy) {
      buySol += t.sol;
      buys++;
      if (t.user !== c.creator) buyers.add(t.user);
    } else {
      sellSol += t.sol;
    }
    if (t.price > high) high = t.price;
    last = t.price;
  }
  if (buys < MIN_BUYS || buyers.size < MIN_BUYERS) return false;
  if (buySol - sellSol < MIN_NET_SOL) return false;
  if (startPrice <= 0) return false;
  if (last < startPrice * (1 + MIN_PRICE_UP)) return false;
  if (last < high * NEAR_HIGH) return false;
  return true;
}

function onBatch(batch: Batch): Order[] {
  const h = hello as Hello;
  const now = batch.slot;
  const orders: Order[] = [];
  const cap = h.caps.max_orders_per_batch;
  const tx = txFee();
  const traded = new Set<Pubkey>();

  // Fills first: clear pending, note new positions.
  for (const f of batch.fills) {
    pending.delete(f.client_id);
    if (f.client_id.startsWith('_')) {
      // A harness close (complete / flatten / writeoff): the position is gone.
      held.delete(f.mint);
      continue;
    }
    if (f.status === 'filled' && f.side === 'buy' && !held.has(f.mint)) {
      held.set(f.mint, {
        openedSlot: f.slot,
        peakValue: 0n,
        devSoldAtOpen: coins.get(f.mint)?.devSold ?? false,
      });
    }
  }

  // Account as of this batch.
  cash = BigInt(batch.account.cash_lamports);
  positions.clear();
  for (const p of batch.account.positions) {
    const tokens = BigInt(p.token_amount);
    if (tokens > 0n) positions.set(p.mint, { tokens, cost: BigInt(p.cost_lamports) });
  }
  // Drop held entries whose position is gone and nothing is in flight.
  for (const mint of [...held.keys()]) {
    if (!positions.has(mint) && !pendingBuy(mint) && !pendingSell(mint)) held.delete(mint);
  }

  // Marks refresh reserves for held / pending mints.
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

  // Fold the batch's events into the coin tapes.
  for (const e of batch.events) {
    if (e.event === 'CreateEvent') {
      coins.set(e.mint, {
        mint: e.mint,
        createSlot: e.slot,
        creator: e.creator,
        devBuySol: 0,
        devSold: false,
        trades: 0,
        lastTradeSlot: e.slot,
        recent: [],
        entered: false,
        complete: false,
        vq: BigInt(e.virtual_quote_reserves),
        vt: BigInt(e.virtual_token_reserves),
        realQuote: 0n,
      });
    } else if (e.event === 'TradeEvent') {
      const c = coins.get(e.mint);
      if (c === undefined) continue; // created before the window: no age, skip
      c.trades += 1;
      c.lastTradeSlot = e.slot;
      c.vq = BigInt(e.virtual_quote_reserves);
      c.vt = BigInt(e.virtual_token_reserves);
      c.realQuote = BigInt(e.real_quote_reserves);
      const sol = Number(e.sol_amount) / 1e9;
      const price = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
      c.recent.push({ slot: e.slot, isBuy: e.is_buy, sol, user: e.user, price });
      if (e.is_buy) {
        if (e.user === c.creator && e.slot === c.createSlot) c.devBuySol += sol;
      } else if (e.user === c.creator) {
        c.devSold = true;
      }
      traded.add(e.mint);
    } else {
      const c = coins.get(e.mint);
      if (c !== undefined) {
        c.complete = true;
        c.entered = true;
      }
    }
  }
  // Prune tapes of coins that traded: keep the window plus a little margin.
  const keepFrom = now - WINDOW_SLOTS - 5;
  for (const mint of traded) {
    const c = coins.get(mint) as Coin;
    let cut = 0;
    while (cut < c.recent.length && c.recent[cut]!.slot <= keepFrom) cut++;
    if (cut > 0) c.recent.splice(0, cut);
  }

  // --- Exits first: every held position is re-priced every batch. ---
  for (const [mint, pos] of positions) {
    if (orders.length >= cap) break;
    if (pendingSell(mint)) continue;
    let hd = held.get(mint);
    if (hd === undefined) {
      hd = { openedSlot: now, peakValue: 0n, devSoldAtOpen: coins.get(mint)?.devSold ?? false };
      held.set(mint, hd);
    }
    const mk = marks.get(mint);
    const c = coins.get(mint);
    const vq = mk !== undefined ? mk.vq : (c?.vq ?? 0n);
    const vt = mk !== undefined ? mk.vt : (c?.vt ?? 0n);
    const value = sellValue(pos.tokens, vq, vt) - tx;
    const cost = pos.cost;
    if (value > hd.peakValue) hd.peakValue = value;
    let exit = false;
    if (cost > 0n) {
      if (value * BPS <= cost * (BPS - STOP_LOSS_BPS)) exit = true; // stop loss
      if (
        hd.peakValue * BPS >= cost * (BPS + TRAIL_ARM_BPS) &&
        value * BPS <= hd.peakValue * (BPS - TRAIL_BPS)
      ) {
        exit = true; // trailing stop
      }
    }
    // Time limit bites only while the position is down: never cut a winner on time.
    if (now - hd.openedSlot >= MAX_HOLD_SLOTS && value < cost) exit = true;
    if (c !== undefined) {
      if (c.devSold && !hd.devSoldAtOpen) exit = true; // creator sold: leave
      if (now - c.lastTradeSlot >= STALE_SLOTS) exit = true; // gone quiet
    }
    if (!exit) continue;
    const id = `o${nextClientId++}`;
    orders.push({ side: 'sell', client_id: id, mint, token_amount: pos.tokens.toString(), max_slippage_bps: MAX_SLIPPAGE_BPS });
    pending.set(id, { mint, side: 'sell', lamports: 0n });
  }

  // --- Entries: only coins that traded this batch, in event order. ---
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
    // Only mark the coin entered once its buy is actually placed: an undersized
    // buy leaves the coin eligible for a later batch, when its reserve may be bigger.
    c.entered = true;
    const id = `o${nextClientId++}`;
    orders.push({ side: 'buy', client_id: id, mint, quote_lamports: amount.toString(), max_slippage_bps: MAX_SLIPPAGE_BPS });
    pending.set(id, { mint, side: 'buy', lamports: amount });
    free -= amount + tx;
    slotsLeft -= 1;
  }
  // Coins that aged out of the entry window without a buy are done for good.
  for (const mint of traded) {
    const c = coins.get(mint) as Coin;
    if (!c.entered && now - c.createSlot > MAX_AGE_SLOTS) c.entered = true;
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
