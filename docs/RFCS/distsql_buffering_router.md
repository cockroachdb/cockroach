- Feature Name: distsql buffering hash router
- Status: completed
- Start Date: 2017-07-19
- Authors: Radu
- RFC PR: [#17105](https://github.com/cockroachdb/cockroach/pull/17105)
- Cockroach Issue: [#17097](https://github.com/cockroachdb/cockroach/issues/17097)

# Summary

This RFC discusses the implementation of a "by-hash" output router in distsql
which doesn't stop sending results once a consumer is blocked.

# Motivation

Issue [#17097](https://github.com/cockroachdb/cockroach/issues/17097) describes
scenarios in which a distsql computation can deadlock. The crux of the issue is
that the streams between the processors have limited buffers, and when sending
on one of these streams blocks, it can block a producer with multiple consumers;
in some cases, sending rows to one of the non-blocked consumers is required for
progress, so the current implementation deadlocks. See the issue for some
examples.

We can fix this by adding buffering on the input side, wherever a consumer reads
from multiple producers (synchronizers, joiners). However, it is difficult to
determine when we need to buffer (and we don't want to buffer unnecessarily);
and, there are multiple distsql components that would be affected.

The alternative is to buffer on the output side; currently, the only component
which has multiple consumers is the hash router (we don't yet use the mirror
router). Moreover, it is easy to have a heuristic for when to buffer: only when
some of the consumers are blocked and others aren't.

The current implementation of the hash router is simple: it is a routine that is
called directly from a processor, which hashes the relevant columns and calls
`Push` on the correct consumer.

The requirements for the new implementation are:
 - if at least one of the consumers is blocked, the router needs to continue
   absorbing rows, buffering rows for blocked consumers and sending rows to
   non-blocked consumers. This is required for preventing deadlocks.
 - if all consumers are blocked, the router must stop buffering rows. This is
   necessary to apply backpressure and prevent buffering a lot of rows when the
   producer is faster than the consumers.

Note that the consumers we are concerned about here are `RowChannel`s which are
implemented using go channels. Routers never send rows directly to gRPC (they go
through a `RowChannel ` to an `outbox` goroutine which does gRPC calls).

# Proposed design

For a k-way hash router, create k goroutines and k `memRowContainer`s (later
`diskRowContainer`s). Each goroutine is responsible for sending rows to a
consumer.

The main router routine adds rows to the containers and uses a channel or
condition variable to wake up the goroutine. The goroutine `Push`es the first
row (which blocks until it gets sent).

To ensure the second requirement above, all the k goroutines as well as the main
routine use a semaphore of capacity `k`. Whenever a goroutine has buffered rows,
it acquires the semaphore; whenever it has no more buffered rows, it releases
the semaphore. The main router routine tries to acquire the semaphore whenever
it's trying to buffer a new row. The result here is that if all consumers have
buffered rows, the router routine also blocks on the semaphore.

### Pros

 - Efficient when fanout is high and many consumers are blocked.
 - Efficient when no buffering is necessary (the goroutines will never acquire
   the semaphore in that case).

### Cons

 - Extra goroutines = extra overhead.

## Implementation notes

 - Proof-of-concept benchmarks showed very little difference between using a
   condition variable vs a wake-up channel.
 
 - Adding and removing rows to a `memRowContainer` has overhead (e.g. memory
   accounting). The implementation should use a small lookaside buffer to avoid
   going through the container if we only buffer a few rows.

 - The goroutine that sends the rows along should grab multiple buffered rows
   instead of reacquiring the mutex for each row.

 - The main routine can reduce overhead by only acquiring the semaphore
   occasionally (e.g. every 8 rows) - it's ok if we buffer a few extra rows
   before we block.

# Considered alternatives

## Channels to k goroutines

Similar to the proposed solution, except that the router routine sends rows to
the goroutines via channels, and the goroutines are responsible for buffering as
necessary.

The goroutines have a loop which tries to either receive a row, or send a row to
a consumer (via a `select`). This would require exposing the underlying channel
(we can no longer hide it behind the `RowReceiver` interface).

The solution still uses the semaphore but an optimization is possible: we can
have a semaphore of `k-1` and only acquire the semaphore from the goroutines,
the idea being that if all consumers are blocked, the last goroutine blocks,
eventually causing the router routine to also block. This optimization has some
subtleties (especially for k=2), and there are cases where it doesn't block as
early as the proposed solution leading to more buffering (even when all
consumers are blocked, the router routine will continue to send rows until it
has to send to the one blocked goroutine (the last to acquire the semaphore).

This solution seems more complicated to implement correctly, and
proof-of-concept
[benchmarks](https://github.com/RaduBerinde/playground/tree/master/buffering_router)
suggest this solution (`Option1` in the benchmarks) is slower anyway.

## reflect.Select

An alternative solution involves using the channels to the consumers directly
and avoids the use of `k` goroutines.

The hashing router routine receives a row destined to a certain consumer. If we
don't have rows buffered for this consumer, we do a non-blocking send to the
consumer. If that doesn't succeed, we buffer the row. In either case, if there
are other consumers with buffered rows, we `TryPush` a row to each one
(repeating if we are successful).

If all the consumers have buffered rows, we need to block so we stop consuming
more rows. We need to block until one consumer is able to receive a row; because
the list of consumers is not fixed at compile time, we can't use a regular
`select` statement; we would need to use `reflect.Select`.

This solution was decided against because `reflect.Select` is likely [too
slow](https://stackoverflow.com/a/32342741/4019276) and the solution is overall
less idiomatic Go than the proposed solution.
