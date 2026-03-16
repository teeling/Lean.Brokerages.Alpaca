# Bracket Orders — Developer Guide

This document covers the bracket order feature added to the Alpaca brokerage plugin.
It describes the API, how it works under the hood, and the rules that must be followed
to use it correctly.

---

## What Is a Bracket Order?

A bracket order is a three-legged trade: an **entry** order (market, limit, or stop-limit), a
**stop-loss** exit leg, and a **take-profit** exit leg. The two exit legs are
One-Cancels-Other (OCO): when one fills, the other is automatically cancelled.

In live trading, Alpaca handles bracket orders natively — all three legs are submitted
atomically and the exit legs are held until the entry fills. In backtesting, the plugin
simulates this behaviour manually.

---

## API

All bracket operations go through `BracketOrderManager`. Never interact with bracket
order tickets directly via LEAN's `Transactions` API — see [Do's and Don'ts](#dos-and-donts).

### Initialisation (Python)

```python
from QuantConnect.Brokerages.Alpaca import BracketOrderManager

def initialize(self):
    self.bracket_mgr = BracketOrderManager(self)
```

### Placing a Bracket

```python
# Market entry
group = self.bracket_mgr.place_bracket_order(
    symbol=self.symbol,
    quantity=100,           # positive = long, negative = short
    stop_loss_price=198.00,
    take_profit_price=205.00,
)

# Limit entry
group = self.bracket_mgr.place_bracket_order(
    symbol=self.symbol,
    quantity=100,
    stop_loss_price=198.00,
    take_profit_price=205.00,
    entry_type=OrderType.LIMIT,
    entry_limit_price=200.50,
)

# Stop-limit stop-loss (instead of stop-market)
group = self.bracket_mgr.place_bracket_order(
    symbol=self.symbol,
    quantity=100,
    stop_loss_price=198.00,
    take_profit_price=205.00,
    stop_loss_limit_price=197.80,
)

# Stop-limit entry — "buy only if price breaks above $201, but pay no more than $201.50"
group = self.bracket_mgr.place_bracket_order(
    symbol=self.symbol,
    quantity=100,
    stop_loss_price=198.00,
    take_profit_price=205.00,
    entry_type=OrderType.StopLimit,
    entry_stop_price=201.00,    # trigger — activate when price reaches this
    entry_limit_price=201.50,   # cap — don't fill above this
)

# Stop-limit entry + stop-limit stop-loss
group = self.bracket_mgr.place_bracket_order(
    symbol=self.symbol,
    quantity=100,
    stop_loss_price=198.00,
    take_profit_price=205.00,
    entry_type=OrderType.StopLimit,
    entry_stop_price=201.00,
    entry_limit_price=201.50,
    stop_loss_limit_price=197.80,
)
```

`place_bracket_order` returns a `BracketGroup` object you keep to track state and
pass to update/cancel calls.

### Cancelling a Bracket

```python
self.bracket_mgr.cancel_bracket(group.GroupId)
```

Works at any point in the bracket lifecycle:
- Before entry fills → cancels the entry; brokerage cascades to exit legs.
- After entry fills → cancels one exit leg; brokerage handles OCO cascade.
- If called again after `IsComplete` → no-op (logged, not an error).

### Updating Stop and Target (after entry fills)

```python
self.bracket_mgr.update_stop(group.GroupId, new_stop_price)
self.bracket_mgr.update_target(group.GroupId, new_target_price)

# Stop-limit variant
self.bracket_mgr.update_stop(group.GroupId, new_stop_price, new_limit_price)
```

Both methods validate that the new price doesn't cross the other leg
(stop must be strictly below target, target must be strictly above stop).
They return `False` and log an error if the constraint would be violated — no
exception is thrown.

### Updating Entry Price (before entry fills)

```python
# Limit entry — update the limit price
self.bracket_mgr.UpdateEntry(group.GroupId, new_limit_price)

# Stop-limit entry — update both stop trigger and limit cap together
self.bracket_mgr.UpdateEntry(group.GroupId, new_limit_price, new_entry_stop_price)
```

Valid for `Limit` and `StopLimit` entry brackets. Safe to call immediately after
`place_bracket_order` — if the order hasn't been acknowledged by the broker yet,
the update is automatically deferred and replayed once the broker ACKs it.

For stop-limit entries, both prices should be moved together to preserve the
stop/limit spread. Passing only `new_limit_price` will update the limit cap
without moving the stop trigger.

### Reading State

```python
group.IsComplete       # True once bracket is fully resolved
group.IsCancelled      # True if entry was cancelled
group.EntryFilled      # True once entry has fully filled
group.ExitFilled       # True once a stop or target leg has filled
group.FillPrice        # Entry fill price (decimal? — None until filled)
group.ExitPrice        # Exit fill price (decimal? — None until filled)
group.ExitOrderId      # LEAN order ID of the leg that triggered
group.StopLossPrice    # Current stop price (updated by UpdateStop)
group.TakeProfitPrice  # Current target price (updated by UpdateTarget)
group.FilledQuantity   # Accumulated entry fill quantity (tracks partial fills)
```

---

## How It Works

### Live Trading

1. `place_bracket_order` submits the entry via LEAN's `SubmitOrderRequest`, tagged
   with `AlpacaBracketOrderProperties` containing stop/target prices.
2. `AlpacaBrokerage.PlaceOrder` detects the bracket properties and calls Alpaca's
   native bracket endpoint, which atomically creates all three legs. Exit legs are
   held by Alpaca until the entry fills.
3. Alpaca streams order events via WebSocket. `AlpacaBrokerage` processes these and
   forwards them to `BracketOrderManager.ProcessOrderEvent`.
4. On entry fill, Alpaca automatically activates the exit legs (they transition from
   `held` to `accepted`).
5. On OCO trigger, Alpaca cancels the sibling leg server-side.

### Backtesting

1. Same entry submission path, but `AlpacaBacktestingBrokerage.PlaceOrder` handles it.
2. Market entries fill synchronously during `PlaceOrder` (LEAN fills them before
   returning). Limit entries fill when the simulated price crosses the limit.
3. On entry fill, `AlpacaBacktestingBrokerage` creates the two exit legs via
   `Algorithm.Transactions.AddOrder`.
4. On exit leg fill, the brokerage manually cancels the sibling (OCO simulation).

### BracketGroup State Machine

```
           place_bracket_order()
                   │
                   ▼
         [entry order: New]
                   │  broker ACK
                   ▼
       [entry order: Submitted]
                   │  fill
                   ▼
         [entry: Filled]
         [exit legs: active]
                   │
          ┌────────┴────────┐
       stop fills        target fills
          │                  │
          ▼                  ▼
    ExitFilled=true    ExitFilled=true
    sibling cancelled  sibling cancelled
          │                  │
          └────────┬──────────┘
                   ▼
             IsComplete=true
```

Or at any point: `cancel_bracket()` → entry/legs cancelled → `IsCancelled=true` →
`IsComplete=true`.

---

## Do's and Don'ts

### ✅ DO

**Always go through `BracketOrderManager` for all bracket operations.**

```python
self.bracket_mgr.cancel_bracket(group.GroupId)
self.bracket_mgr.update_stop(group.GroupId, new_stop)
self.bracket_mgr.update_target(group.GroupId, new_target)
self.bracket_mgr.UpdateEntry(group.GroupId, new_limit)          # PascalCase — custom C# method
```

**Check `IsComplete` before operating on a group.**

```python
if not group.IsComplete:
    self.bracket_mgr.update_stop(group.GroupId, new_stop)
```

**Check `EntryFilled` before calling `update_stop` / `update_target`.**
Both methods guard against this and log an error, but checking first avoids the error log.

**Call `UpdateEntry` any time after `place_bracket_order`.**
The deferred-ACK mechanism handles the race condition — you don't need to wait.

**Use `UpdateEntry` (PascalCase) — not `update_entry`.**
`BracketOrderManager` is a custom C# class. LEAN's snake_case bridge only applies to
built-in `QCAlgorithm` methods, not custom C# objects exposed via pythonnet.

### ❌ DON'T

**Never call `ticket.Update()` directly on bracket tickets.**

```python
# WRONG — bypasses the bracket manager's deferred-ACK logic and cross-price guards
group.EntryTicket.Update(fields)
group.StopTicket.Update(fields)

# RIGHT
self.bracket_mgr.UpdateEntry(group.GroupId, new_price)
self.bracket_mgr.update_stop(group.GroupId, new_stop)
```

**Never call `ticket.Cancel()` directly on bracket tickets.**

```python
# WRONG — only cancels one leg; brokerage OCO cascade may not fire correctly
group.EntryTicket.Cancel()

# RIGHT
self.bracket_mgr.cancel_bracket(group.GroupId)
```

**Never call `self.transactions.cancel_order(order_id)` on bracket order IDs.**
Use `cancel_bracket()` instead.

**Never update stop/target before the entry has filled.**
The stop-loss and take-profit legs don't exist yet. `update_stop` and `update_target`
both guard against this and return `False`.

**Never set stop >= target (or target <= stop).**
Alpaca rejects this at the broker level. The manager guards against it in
`update_stop` and `update_target` and returns `False` with a log error.

**Never directly access `Transactions.GetOrderTicket` for bracket leg orders by
brokerage ID.** Exit leg tickets are registered on `BracketGroup.StopTicket` and
`BracketGroup.TargetTicket` — use those.

---

## Race Conditions Handled Internally

These are handled transparently — you don't need to code around them.

### Cancel-before-ACK

If `cancel_bracket()` is called immediately after `place_bracket_order()` (before
Alpaca ACKs the entry submission), LEAN rejects the cancel because the order is still
in `New` state. The manager defers the cancel and retries automatically when the entry
transitions to `Submitted`.

Log signature: `"Setting PendingCancel for deferred retry"` then
`"Executing deferred CancelBracket"`.

### UpdateEntry-before-ACK

Same pattern as above but for `UpdateEntry()`. If called before the broker ACKs the
entry, the update is stored and replayed on the first order event after `Submitted`.

Log signature: `"Deferring update to X until Submitted"` then
`"Replaying deferred entry update"`.

### Cancel-during-replace

If `cancel_bracket()` is called while a stop or target leg is mid-replace (Alpaca
locks siblings during a PATCH), the manager detects which leg is pending replacement
and cancels the other one to avoid the "pending replacement" rejection.

### "Order parameters are not changed"

If `update_stop` or `update_target` requests a price that already matches the current
order price at Alpaca, Alpaca returns this error. The manager treats it as a success
(the price is already correct) rather than marking the ticket `Invalid`.

---

## Constraints

| Constraint | Enforced by |
|------------|-------------|
| `stop < target` | `update_stop` / `update_target` (pre-flight check) |
| `target > stop` | `update_stop` / `update_target` (pre-flight check) |
| Entry must be filled before updating exit legs | `update_stop` / `update_target` |
| Only `Market`, `Limit`, or `StopLimit` entry types supported | `place_bracket_order` validation |
| `entryLimitPrice` required for `Limit` and `StopLimit` entries | `place_bracket_order` validation |
| `entryStopPrice` required for `StopLimit` entries | `place_bracket_order` validation |
| Stop-limit entry (long): `entryStopPrice < entryLimitPrice` | Alpaca broker validation |
| Stop-limit entry (short): `entryStopPrice > entryLimitPrice` | Alpaca broker validation |
| Quantity must be non-zero | `place_bracket_order` validation |
| Short brackets: stop > entry > target | `place_bracket_order` validation |
| Long brackets: stop < entry < target | `place_bracket_order` validation |

---

## Logging

All bracket operations log at `DEBUG` level using the prefix
`BracketOrderManager.<MethodName>`. Errors use `Log.Error` and are visible in
the standard LEAN log output.

Key log patterns to search for during debugging:

| Pattern | Meaning |
|---------|---------|
| `Created group <id>` | Bracket was submitted |
| `Placing stop-limit entry stop=X limit=Y` | Stop-limit entry submitted |
| `Entry FILLED for group` | Entry fill received |
| `STOP leg FILLED` / `TARGET leg FILLED` | Exit triggered |
| `Setting PendingCancel` | Cancel deferred (order still New) |
| `Executing deferred CancelBracket` | Deferred cancel replayed |
| `Deferring update to X until Submitted` | UpdateEntry deferred |
| `Replaying deferred entry update` | Deferred update replayed |
| `Rejected update — newStopPrice >= takeProfitPrice` | Cross-price guard fired |
| `CancelRequested flag set...cancelling exit legs created after deferred fill` | Cancel-after-update race resolved (backtest) |
| `Clearing PendingUpdateOrderId` | Replace completed |
