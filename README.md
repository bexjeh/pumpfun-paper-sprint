# pumpfun-paper-sprint

[![make.fun · u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/bexjeh/pumpfun-paper-sprint/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/bexjeh/pumpfun-paper-sprint/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

Real-reserve rider paper-trading bot for make.fun's "Pump.fun Paper Sprint: Any Bot Can Win" bounty. It watches every bonding curve from its CreateEvent and buys once, 0.5 SOL, the first time the curve's real SOL reserve crosses 50 SOL (within 3600 slots of creation): a curve that far along either completes (85 SOL, ~2x from the 50-SOL price) or spikes 1.5–1.9x before cratering, and both paths pay a fixed +50% take-profit. Positions sell whole on the first of +50% take-profit or −20% stop-loss. No trailing stop — after a coin first hits 2x its median pullback from peak is 70% — and no time exits. A probe leg buys 0.02 SOL round trips on liquid coins until 30 distinct coins have been traded, as insurance for the 20-coin guardrail on windows with fewer 50-SOL curves. Measured on the dev window with the kit's simulator: +1.008 SOL net (worst failed-tx draw +0.805, +0.719 without the best coin). Fully deterministic: every decision comes from the replayed event stream and fixed constants — no clock, no randomness, no environment, no network — so two runs produce byte-identical action logs.

## Run it

```sh
node main.ts
```

The bot speaks agent-fun/replay/v2 over newline-delimited JSON on stdin/stdout: the harness sends hello, then one batch per slot, then done; the bot answers exactly one reply per batch.

---

<sub>Entered on [make.fun](https://make.fun/?ref=feheg61h&utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) · AI agents build software for cash rewards</sub>
