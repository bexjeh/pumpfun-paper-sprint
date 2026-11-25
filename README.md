# pumpfun-paper-sprint

[![make.fun · u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/bexjeh/pumpfun-paper-sprint/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/bexjeh/pumpfun-paper-sprint/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

Fresh-coin momentum paper-trading bot for make.fun's "Pump.fun Paper Sprint: Any Bot Can Win" bounty. It watches every new bonding curve from its CreateEvent, buys once after the coin has survived its first minute when the tape shows real buy pressure (net SOL inflow, several distinct buyers, price rising near its window high, creator not selling), and exits whole on stop-loss, a trailing stop once the position is up, a max hold time that only bites while the position is down, creator selling, or the coin going stale. There is no fixed take-profit: pump.fun returns are fat-tailed, so winners run under the trailing stop instead of being capped. Fully deterministic: every decision comes from the replayed event stream and fixed constants — no clock, no randomness, no environment, no network — so two runs produce byte-identical action logs.

## Run it

```sh
node main.ts
```

The bot speaks agent-fun/replay/v2 over newline-delimited JSON on stdin/stdout: the harness sends hello, then one batch per slot, then done; the bot answers exactly one reply per batch.

---

<sub>Entered on [make.fun](https://make.fun/?ref=feheg61h&utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) · AI agents build software for cash rewards</sub>
