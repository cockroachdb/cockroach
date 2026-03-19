// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package intentresolver resolves intents left behind by
// transactions. It is used by several subsystems with very different
// performance characteristics, and employs a layered set of
// concurrency limiters to prevent any single use case from
// overwhelming the node.
//
// # Use cases
//
// The intent resolver serves four main use cases, listed roughly in
// order of latency sensitivity.
//
// 1. Foreground intent resolution (ResolveIntent, ResolveIntents).
// When a transaction encounters a conflicting intent in the lock
// table, it pushes the lock holder and then resolves the intent
// synchronously. This is on the critical path of user-facing
// requests: a SELECT blocked on an intent from a committed
// transaction must resolve that intent before it can return results.
// ResolveIntent sends a single ResolveIntentRequest immediately
// (bypassing the request batcher) to minimize latency. ResolveIntents
// (plural) is used for deferred resolution of multiple intents
// accumulated during lock table scanning, and routes through the
// batcher. Neither method uses the async task semaphore (ir.sem);
// backpressure comes from the user's request goroutine itself.
//
// 2. Post-transaction cleanup (CleanupTxnIntentsAsync). After a
// transaction commits or aborts (EndTxn), the executing replica
// asynchronously resolves the transaction's intents and GCs its
// record. This is semi-synchronous: if the async semaphore is full,
// the work runs in the caller's goroutine, applying backpressure to
// the client. This ensures that a burst of small transactions does
// not accumulate a large intent resolution backlog.
//
// 3. Encountered-intent cleanup (CleanupIntentsAsync). During
// inconsistent reads or SKIP LOCKED operations, the reader may
// discover intents from other transactions. These are cleaned up
// asynchronously using PUSH_TOUCH (a non-committal push that only
// aborts expired transactions). Like post-transaction cleanup, this
// falls back to synchronous processing when the semaphore is full.
//
// 4. MVCC GC cleanup (CleanupTxnIntentsOnGCAsync). The MVCC GC queue
// encounters old transaction records and resolves their intents. This
// is background work that must not block the GC queue, so it uses
// WaitForSem=false: if no async slot is available, the transaction is
// skipped and retried in a future GC pass. Any transaction can
// leave behind a large number of intents (tens of thousands is not
// unusual), so additional intent-level limiting is needed within
// the goroutine (see "Concurrency limiters" below). The GC queue also
// resolves intents discovered during the GC scan via CleanupIntents
// (synchronous, using PUSH_TOUCH); this path does not use ir.sem at
// all.
//
// # Resolution pipeline
//
// For all use cases, the resolution pipeline is:
//
//  1. Push the transaction (MaybePushTransactions / PushTransaction)
//     to determine its final status if not already finalized.
//  2. Resolve the intents (resolveIntents) by sending
//     ResolveIntentRequest / ResolveIntentRangeRequest to each range.
//  3. GC the transaction record (gcTxnRecord) once all intents are
//     resolved.
//
// Step 2 is where the bulk of the work happens and where the
// concurrency limiters described below apply.
//
// # Request batching
//
// Intent resolution requests are routed through one of two
// RequestBatcher instances: irBatcher for point intents
// (ResolveIntentRequest) and irRangeBatcher for ranged intents
// (ResolveIntentRangeRequest). The batcher groups requests by
// destination range and coalesces them into batches, so N intents on
// the same range become a single BatchRequest. This is important
// because a committed transaction's intents are often co-located on a
// small number of ranges, and without batching each intent would be a
// separate RPC.
//
// The batcher spawns one goroutine per in-flight batch (i.e. per
// destination range with pending work). Multiple callers may submit
// work for the same range concurrently, each resulting in its own
// goroutine. For example, a transaction with 1000 intents spread
// across 100 ranges produces ~100 concurrent goroutines (one per
// range), not 1000. If the 1000 intents were all on the same range,
// this would be a single goroutine sending one batch.
//
// The intent resolution batchers (irBatcher, irRangeBatcher) do not
// impose their own in-flight backpressure limit by default. This is
// primarily to avoid priority inversion: if the batcher had a shared
// concurrency limit, low-priority intent resolutions could occupy all
// slots (perhaps blocked talking to an overloaded leaseholder),
// preventing high-priority work from resolving its intents — which
// would in turn block other high-priority operations. By not limiting
// at the batcher level, each caller's work proceeds independently.
// Additionally, the callers already limit concurrency (via ir.sem and
// gcIntentBudget), and have already paid the memory cost of buffering
// the requests, so there is little benefit in refusing to send them.
// The GC batcher (gcBatcher, used for GCRequest to clean up txn
// records) is the exception: GC record cleanup runs outside ir.sem,
// so the gcBatcher uses an in-flight limit of 1000.
//
// Foreground resolution of a single point intent (use case 1,
// ResolveIntent) bypasses the batcher entirely and sends a
// ResolveIntentRequest in its own batch immediately. This latency
// optimization matters because the calling transaction is blocked on
// the conflicting intent and waiting for batcher coalescing would add
// unnecessary delay.
//
// # Concurrency limiters
//
// The intent resolver has several layers of concurrency control that
// work together. Understanding which limiters apply to which use case
// is key to understanding the system's behavior under load.
//
// ir.sem — async task semaphore (capacity: 1000).
// Limits the total number of goroutines spawned by the three async
// entry points (CleanupTxnIntentsAsync, CleanupIntentsAsync,
// CleanupTxnIntentsOnGCAsync). Post-transaction and
// encountered-intent cleanup use WaitForSem=false with a synchronous
// fallback: if the semaphore is full, the work runs in the caller's
// goroutine. GC cleanup also uses WaitForSem=false but simply skips
// the transaction, since blocking the GC queue would be worse than
// deferring work.
//
// gcIntentBudget — GC intent budget (capacity: 1000 intents).
// Limits the total number of in-flight intents across all concurrent
// GC goroutines. Each GC goroutine acquires budget equal to
// len(txn.LockSpans) before resolving intents. If a transaction has
// more intents than the budget capacity, the IntPool truncates the
// acquisition to the full capacity, meaning the transaction consumes
// the entire budget. This ensures that at most ~1000 intents are
// being actively resolved via GC at any time, with the caveat that a
// single large transaction may overshoot.
//
// Example: 50 GC goroutines each cleaning up a 20-intent transaction
// can all run concurrently (50 * 20 = 1000 intents). But if one
// goroutine is cleaning up a 70,000-intent transaction, it holds the
// full budget and all other GC goroutines block until it finishes.
//
// maxIntentsInFlightPerCaller — per-caller chunking (1000 intents).
// Within a single call to resolveIntents, requests are submitted to
// the batcher in chunks of 1000. Each chunk's responses are collected
// before the next chunk is submitted. This bounds the number of
// in-flight batcher goroutines that a single caller can create.
// Without this, a transaction with 70,000 intents spread across
// 70,000 ranges would spawn ~70,000 batcher goroutines
// simultaneously. Instead, it will work through its 70,000 intents
// in sequential batches of 1000, so in the best case the batcher
// will only create between 10 (ten batches of 100 intents each,
// when all 1000 intents are colocated) and 1000 (a batch per range,
// when all 1000 intents reside on different ranges — unlikely)
// concurrent goroutines.
//
// lockInFlightTxnCleanup — per-transaction deduplication.
// Ensures that only one goroutine is cleaning up a given
// transaction's intents at a time. If a second attempt arrives, it
// is skipped. This prevents redundant work and is important because
// a transaction's own eager post-commit cleanup (use case 2) can
// race with another request that encounters one of its intents (use
// case 3).
//
// inFlightPushes — per-transaction push deduplication.
// Tracks which transactions are currently being pushed, so that
// concurrent push attempts for the same transaction can coalesce. The
// push request is deduplicated at the intent resolver level; the
// actual PushTxnRequest is sent once and the result shared.
//
// These limiters compose as follows for each use case:
//
//   - Foreground resolution: no ir.sem, no gcIntentBudget. One
//     request goroutine, one intent (or a small batch). Bounded by
//     the number of concurrent user requests.
//
//   - Post-txn cleanup: ir.sem (with sync fallback). Chunks of 1000
//     intents per caller. At most 1000 async goroutines, each
//     resolving one transaction.
//
//   - Encountered-intent cleanup: ir.sem (with sync fallback).
//     Typically small batches of intents from a single read.
//
//   - GC cleanup: ir.sem (non-blocking skip) + gcIntentBudget
//     (blocking). At most 1000 goroutines, but further limited so
//     that the total in-flight intent count across all of them is
//     ~1000. Within each goroutine, per-caller chunking limits the
//     batcher goroutines to 1000 at a time.
//
// # Admission control
//
// Intent resolution participates in admission control (AC). The
// interaction between AC and intent resolution is subtle due to
// priority inversion concerns. See the comment at the top of
// admission.go for a detailed discussion of the problem, the design
// constraints, and worked examples.
package intentresolver
