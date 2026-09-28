---
title: "Idempotent Payment Settlement in Go: Surviving Webhook Redelivery Without Double-Crediting Anyone"
permalink: /idempotent-payment-settlement-in-go/
---

# Idempotent Payment Settlement in Go: Surviving Webhook Redelivery Without Double-Crediting Anyone

All the code in this article is runnable and tested — including the
concurrency tests — at
[github.com/mikewhsieh/go-payments-patterns](https://github.com/mikewhsieh/go-payments-patterns).

## The problem nobody warns you about

Here is a promise every payment provider makes, buried somewhere in
their integration docs: *webhooks are delivered at least once.*

"At least once" sounds harmless until you read it as what it really
means: **the same success notification will arrive twice.** Sometimes
three times. Sometimes twice within the same 50 milliseconds, from two
different retry workers on the provider's side.

And webhooks are not the only repeat caller. Your own client retries a
settlement RPC after a timeout — the first call actually succeeded,
but the response got lost, so the client sends it again. Your
reconciliation job queries the provider, sees "paid", and tries to
settle an order that a webhook settled two seconds earlier.

The naive settlement handler does this:

1. Load the order.
2. Check it is pending.
3. Compute the fee, credit the balance, mark it settled.

Run that twice concurrently and both requests pass the check in step 2
before either one writes step 3. The customer gets credited twice. Real
money is now wrong, and no exception was thrown anywhere — every log
line looks like success.

I spent the last few years rewriting a high-volume payments platform
from C#/.NET into Go, and this exact failure mode — in several
costumes — is the thing the whole settlement design defends against.
This article walks through the three layers that close it, plus the
foundation they stand on: exact money arithmetic.

## First, get money itself right

Before idempotency, a smaller question: what type holds the amount?

Not `float64`. Never `float64`. Floating point cannot represent most
decimal fractions exactly, and payment math multiplies small errors
into visible ones. The classic symptom from our legacy system: a
213.00 order at a 0.37% fee rate should produce a fee of exactly
**0.7881**. Depending on which layer rounded first, we would see
`0.78` (a precision cap applied too early) or `0.7800` (an ORM
silently rounding to a default scale). Both are wrong, and both were
produced without any error being raised.

The fix is structural, not disciplinary: a fixed-point type where the
amount is a scaled integer, and the scale is defined in exactly one
place.

```go
// Scale is the number of decimal places every Money value carries.
const Scale = 4

// Money is a fixed-point decimal amount, stored as value * 10^Scale.
type Money struct {
	scaled int64
}
```

Multiplying by a rate is the only place precision can leak, so the
intermediate product uses `big.Int` and rounds half-away-from-zero
once, at the end:

```go
func (m Money) MulRate(rate Money) Money {
	// Both operands are scaled by 10^Scale, so the raw product is
	// scaled by 10^(2*Scale); divide one factor back out, rounding.
	product := new(big.Int).Mul(big.NewInt(m.scaled), big.NewInt(rate.scaled))
	return Money{scaled: divRoundHalfAwayFromZero(product, big.NewInt(scaleFactor))}
}
```

Storage follows the same rule: every money column is `BIGINT` holding
`Scaled()`. No `DECIMAL`, no `NUMERIC` — those invite the driver or an
ORM to apply its own rounding on the way in or out. A scaled integer
survives any number of round-trips unchanged, and the test suite pins
the canonical case:

```go
func TestMulRateExactFee(t *testing.T) {
	amount := MustParse("213.00")
	rate := MustParse("0.0037")
	if got, want := amount.MulRate(rate), MustParse("0.7881"); got.Cmp(want) != 0 {
		t.Errorf("213.00 * 0.0037 = %s, want %s", got, want)
	}
}
```

Now the interesting part.

## Layer 1: the status guard

The first defense is embarrassingly simple: only a pending order can be
settled, and the check lives in the domain function itself — not in a
handler, not in middleware.

```go
order, err := tx.GetOrderForUpdate(ctx, orderID)
if err != nil {
	return fmt.Errorf("settle %s: load order: %w", orderID, err)
}

// The status guard. Everything below only runs for a pending order.
if order.Status != StatusPending {
	result.Order = *order
	result.AlreadySettled = true
	return nil
}
```

Putting the guard in the domain function matters because settlement has
more than one entrance: the webhook handler, a gRPC endpoint, a manual
back-office confirmation, a reconciliation job. If the guard lives in
one handler, the other three entrances are unguarded. In the domain
function, every caller goes through the same door.

But a guard alone has a race. Two concurrent transactions can both
read `StatusPending` before either writes `StatusSettled`. That is a
textbook TOCTOU (time-of-check to time-of-use) bug, and no amount of
application code fixes it — the fix belongs to the storage layer.

## Layer 2: the row lock

The load in the snippet above is not a plain `SELECT`. It locks:

```go
row := t.tx.QueryRow(ctx, `
	SELECT id, amount, actual_amount, fee, status, settled_at
	FROM orders
	WHERE id = $1
	FOR UPDATE`, orderID)
```

`FOR UPDATE` makes two concurrent settlements of the same order
serialize on the row. The first transaction takes the lock, checks the
guard, settles, commits. The second transaction was blocked the whole
time; when it finally reads the row, it sees `StatusSettled` and takes
the `AlreadySettled` path. The check and the act now happen inside the
same lock, so there is nothing left to race.

Two details worth calling out:

**The lock is scoped to the row, not the table.** A thousand different
orders settle in parallel without touching each other. Only concurrent
attempts on the *same* order serialize — which is exactly the set of
requests that must not run in parallel.

**Read-only paths do not lock.** A reconciliation job that lists
pending orders reads without `FOR UPDATE`; it only re-acquires the
locked read when it actually settles one. Locking on the list would
serialize the whole system for no benefit — the idempotency guarantee
lives at the settlement point, not the discovery point.

## Layer 3: one transaction or it didn't happen

A settlement is never one write. In this reference implementation it is
three; in the production system it was closer to seven:

1. Update the order (actual amount, fee, status, settled-at).
2. Append a ledger entry — the append-only audit record.
3. Record an outbox event that downstream systems will consume.

If the process crashes after write 1 but before write 2, you have an
order that claims to be settled with no ledger entry to prove it. Those
two facts must never be observable separately, so all writes happen
inside one database transaction:

```go
type Store interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}
```

Everything inside `fn` commits together or not at all. The settlement
function is pure orchestration — load, guard, compute, write, write,
write — and the store decides what "atomic" means (a real transaction
in Postgres; a mutex plus snapshot-rollback in the in-memory test
store).

The third write deserves its own paragraph, because it fixes a bug
most systems have without knowing it.

After settling, you usually need to tell someone: publish a
"credit the wallet" message to RabbitMQ or Kafka. The tempting
implementation is *commit the transaction, then publish*. But if the
process dies between the two, the money moved and the message is gone
— your ledger and your downstream world now permanently disagree.
Publish-first is worse: the message can fire for a settlement that
rolls back.

The **transactional outbox** closes it. The event is written to an
`outbox` table *inside the settlement transaction* — so "settlement
committed" and "event durably recorded" are literally the same fact. A
relay process then reads undispatched events and publishes them:

```go
rows, err := s.pool.Query(ctx, `
	SELECT id, event_type, payload, created_at, dispatched_at
	FROM outbox
	WHERE dispatched_at IS NULL
	ORDER BY id
	LIMIT $1
	FOR UPDATE SKIP LOCKED`, limit)
```

`SKIP LOCKED` is the small clause doing big work: if you run two relay
instances, they never grab the same event — the second instance skips
rows the first one holds and moves on. Events are marked dispatched
only *after* a successful publish, so delivery is at-least-once and a
crashed relay loses nothing. (At-least-once means consumers must
deduplicate by event ID — which is the same idempotency discipline,
one layer down.)

## Idempotent is not an error

One design decision turns out to matter a lot in practice: what does
the *second* caller get back?

The tempting answer is an error — `ErrAlreadySettled`, HTTP 409,
something red. But think about who that caller is: a payment provider
redelivering a webhook. If you return an error, the provider retries
*again*. Some providers keep retrying for days and eventually page a
human on their side.

For a redelivered notification, "already done" is not a failure. It is
the system working exactly as designed. So the settlement returns a
result, not an error:

```go
type Result struct {
	Order Order

	// AlreadySettled is true when this call found the order already
	// settled by an earlier delivery. That is an expected, successful
	// outcome — for a redelivered webhook, "already done" is done.
	AlreadySettled bool
}
```

The webhook handler acknowledges with a 200 either way. The flag exists
so callers can skip side effects that must not repeat — publishing
another wallet-credit event, recording stats twice — while still
treating the request as a success.

There is one more property hiding in this design, and it is the one
that protects you from malicious or buggy repeat calls: **the repeat
carries different data, and nothing happens.** If the second delivery
claims a different amount or a later timestamp, it never reaches the
write path — the guard turned it away before any mutation. The values
the *first* settlement recorded are immutable in practice, enforced by
control flow rather than by hope. The test suite makes this explicit:
settle once with `213.00`, settle again with `999.00`, assert the
stored amount is still `213.00` and the ledger still has exactly one
entry.

## Testing it honestly

Concurrency claims need concurrency tests. Two kinds, because they
catch different bugs:

**Unit tests against an in-memory store.** The store implements the
same `WithinTx` contract with a mutex and snapshot-rollback. Fifty
goroutines hammer the same order; the test asserts exactly one winner,
one ledger entry, one outbox event. Fast, no database, runs anywhere.

**Opt-in integration tests against real PostgreSQL.** The in-memory
store serializes everything with one mutex, so it cannot catch a
missing `FOR UPDATE` — only a real database can. The integration tests
run the same hammer against real transactions:

```go
func TestSettle_PostgresConcurrentAttemptsSerializeOnRowLock(t *testing.T) {
	// 20 goroutines, 20 real transactions, one order.
	// Exactly one may win; the ledger must hold exactly one entry.
}
```

They are gated by an environment variable, so `go test ./...` stays
green on a laptop with no database, and CI with a Postgres service
runs the real thing. Delete `FOR UPDATE` from the query and this test
fails within milliseconds — which is precisely the property you want
from a test guarding a race.

## Closing

Nothing in this article is a new invention. Fixed-point money is older
than computers; `FOR UPDATE` shipped decades ago; the transactional
outbox has its own pattern page. What is rare is systems that do all of
them at the same time, on every settlement path, with tests that would
catch the regression.

That gap is where double-credits come from.

The full runnable code — the money type, the settlement flow, the
in-memory and PostgreSQL stores, and all the concurrency tests — is at
[github.com/mikewhsieh/go-payments-patterns](https://github.com/mikewhsieh/go-payments-patterns).

---

*I'm a backend engineer in Taipei. I spent the last few years
rewriting a high-volume payments platform from C#/.NET into Go — these
patterns are what survived contact with production. I'm open to remote
contract work: mike255920002000@gmail.com*
