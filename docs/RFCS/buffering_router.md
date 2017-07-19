- Feature Name: buffering hash router
- Status: draft
- Start Date: 2017-07-19
- Authors: Radu
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #17097

# Summary

This RFC discusses the implementation of a "by-hash" output router in distsql
which doesn't stop sending results once a consumer is blocked.

# Motivation

Issue [17097](https://github.com/cockroachdb/cockroach/issues/17097) describes
scenarios in which a distsql computation can deadlock. The crux of the issue is
that the streams between the processors have limited buffers, and when sending
on one of these streams blocks, it can block a producer with multiple consumers;
in some cases, sending rows to one of the non-blocked consumers is required for
progress, so the current implementation deadlocks.

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
implemented using go channels. Routers never send data directly to gRPC (they go
through a `RowChannel ` to an `outbox` goroutine which does gRPC calls).

# Proposed design

For a k-way hash router, create k goroutines and k `memRowContainer`s (later
`diskRowContainer`s). Each goroutine is responsible for sending rows to a
consumer.

All the k goroutines use a semaphore of capacity `k`. Whenever a goroutine has
buffered rows, it acquires the semaphore; whenever it has no more buffered
rows, it releases the semaphore. The main router routine also tries to acquire
the semaphore whenever it's trying to push a new row. The result here is that
if all consumers have buffered rows, the router routine also blocks on the
semaphore.
  
  
There are two implementation "flavors" here:
 1. The k goroutines receive rows from the main router routine via channels,
    and they are responsible for buffering rows into their row container.
    The goroutines only block on reading rows from this routine. Whenever they
    receive a row, they use non-blocking channel sends (via a new `TryPush`
    primitive) to try to send a row to the consumer. If they can't, they will
    try again when they receive another row. The disadvantage is that if the
    consumer can receive more rows, we won't send them until we receive
    another row for that consumer (or the stream ends).

 2. The main router routine adds rows directly to the row containers and uses a
    channel or condition variable to wake up the goroutine. The goroutine
    `Push`es the first row (which will block until it gets sent). The
    disadvantage here is the extra synchronization (each row container would
    need to be protected by a mutex).

### Pros

 - Efficient when fanout is high and many consumers are blocked.
 - Efficient when no buffering is necessary (the goroutines will never acquire
   the semaphore in that case).

### Cons

 - Extra goroutines = extra overhead.
 - Extra synchronization (the semaphore) for every row.

# Alternative design

Like `1.` above, this design also uses a new `TryPush` primitive for
non-blocking channel sends .

The hashing router routine receives a row destined to a certain consumer. If we
don't have rows buffered for this consumer, we `TryPush` to the consumer.  If
that doesn't succeed, we buffer the row. In either case, if there are other
consumers with buffered rows, we `TryPush` a row to each one (repeating if we
are successful).

If all the consumers have buffered rows, we need to block so we stop consuming
more rows. We need to block until one consumer is able to receive a row; this is
difficult to achieve. Since the consumers are always channels, we could use
`reflect.Select` but it's unclear how much overhead there would be to this
approach (any thoughts on this?)

When the producer finishes, we need to flush all the buffered rows. This is
similar to the blocking case above - we need to push to whichever consumers can
receive.


### Pros

 - No extra goroutines.
 - No overhead (compared to the current implementation) when we don't need to
   buffer.
 - Low overhead when there is a small number of blocked consumers.

### Cons

 - Less "idiomatic Go" than the first approach.
 - Unclear how efficient `reflect.Select` would be.
 - If we have a large number of blocked consumers, it might be inefficient to
   try to send to all of them whenever we receive a new row.
 - The router only sends rows when it receives a new row; rows could stay in
   buffers even while the consumer can accept them until a new row is produced.
