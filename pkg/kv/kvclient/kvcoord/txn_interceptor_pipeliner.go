// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	gbtree "github.com/google/btree"
)

// The degree of the inFlightWrites btree.
const txnPipelinerBtreeDegree = 32

// PipelinedWritesEnabled is the kv.transaction.write_pipelining.enabled cluster setting.
var PipelinedWritesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.write_pipelining_enabled",
	"if enabled, transactional writes are pipelined through Raft consensus",
	true,
	settings.WithName("kv.transaction.write_pipelining.enabled"),
	settings.WithPublic,
)

var pipelinedRangedWritesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.write_pipelining.ranged_writes.enabled",
	"if enabled, transactional ranged writes are pipelined through Raft consensus",
	true,
	settings.WithPublic,
)

var pipelinedLockingReadsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.write_pipelining.locking_reads.enabled",
	"if enabled, transactional locking reads are pipelined through Raft consensus",
	true,
	settings.WithPublic,
)

var pipelinedWritesMaxBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.transaction.write_pipelining_max_batch_size",
	"if non-zero, defines that maximum size batch that will be pipelined through Raft consensus",
	// NB: there is a tradeoff between the overhead of synchronously waiting for
	// consensus for a batch if we don't pipeline and proving that all of the
	// writes in the batch succeed if we do pipeline. We set this default to a
	// value which experimentally strikes a balance between the two costs.
	//
	// Notably, this is well below sql.max{Insert/Update/Upsert/Delete}BatchSize,
	// so implicit SQL txns should never pipeline their writes - they should either
	// hit the 1PC fast-path or should have batches which exceed this limit.
	128,
	settings.NonNegativeInt,
	settings.WithName("kv.transaction.write_pipelining.max_batch_size"),
	settings.WithPublic,
)

// TrackedWritesMaxSize is a byte threshold for the tracking of writes performed
// a single transaction. This includes the tracking of lock spans and of
// in-flight writes, both stored in the txnPipeliner.
//
// Locks are included with a transaction on commit or abort, to be cleaned up
// asynchronously. If they exceed this threshold, they're condensed to avoid
// memory blowup both on the coordinator and (critically) on the EndTxn command
// at the Raft group responsible for the transaction record.
//
// The in-flight writes are, on the happy path, also attached to the commit
// EndTxn. On less happy paths, they are turned into lock spans.
//
// NB: this is called "max_intents_bytes" instead of "max_lock_bytes" because it
// was created before the concept of intents were generalized to locks, and also
// before we introduced in-flight writes with the "parallel commits". Switching
// it would require a migration which doesn't seem worth it.
//
// Note: Default value was arbitrarily set to 256KB but practice showed that
// it could be raised higher. When transaction reaches this limit, intent
// cleanup switches to range instead of point cleanup which is expensive
// because it needs to hold broad latches and iterate through the range to
// find matching intents.
// See #54029 for more details.
var TrackedWritesMaxSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.transaction.max_intents_bytes",
	"maximum number of bytes used to track locks in transactions",
	1<<22, /* 4 MB */
	settings.WithPublic)

// rejectTxnOverTrackedWritesBudget dictates what happens when a txn exceeds
// kv.transaction.max_intents_bytes.
var rejectTxnOverTrackedWritesBudget = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.reject_over_max_intents_budget.enabled",
	"if set, transactions that exceed their lock tracking budget (kv.transaction.max_intents_bytes) "+
		"are rejected instead of having their lock spans imprecisely compressed",
	false,
	settings.WithPublic)

// rejectTxnMaxCount will reject transactions if the number of inserts or locks
// exceeds this value. It is preferable to use this setting instead of
// kv.transaction.reject_over_max_intents_budget.enabled.
var rejectTxnMaxCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.transaction.max_intents_and_locks",
	"maximum count of inserts or durable locks for a single transactions, 0 to disable",
	0,
	settings.WithPublic)

// txnPipeliner is a txnInterceptor that pipelines transactional writes by using
// asynchronous consensus. The interceptor then tracks all writes that have been
// asynchronously proposed through Raft and ensures that all interfering
// requests chain on to them by first proving that the async writes succeeded.
// The interceptor also ensures that when committing a transaction all writes
// that have been proposed but not proven to have succeeded are first checked
// before considering the transaction committed. These async writes are referred
// to as "in-flight writes" and this process of proving that an in-flight write
// succeeded is called "proving" the write. Once writes are proven to have
// finished, they are considered "stable".
//
// Chaining on to in-flight async writes is important for two main reasons to
// txnPipeliner:
//
//  1. requests proposed to Raft will not necessarily succeed. For any number of
//     reasons, the request may make it through Raft and be discarded or fail to
//     ever even be replicated. A transaction must check that all async writes
//     succeeded before committing. However, when these proposals do fail, their
//     errors aren't particularly interesting to a transaction. This is because
//     these errors are not deterministic Transaction-domain errors that a
//     transaction must adhere to for correctness such as conditional-put errors or
//     other symptoms of constraint violations. These kinds of errors are all
//     discovered during write *evaluation*, which an async write will perform
//     synchronously before consensus. Any error during consensus is outside of the
//     Transaction-domain and can always trigger a transaction retry.
//
//  2. transport layers beneath the txnPipeliner do not provide strong enough
//     ordering guarantees between concurrent requests in the same transaction to
//     avoid needing explicit chaining. For instance, DistSender uses unary gRPC
//     requests instead of gRPC streams, so it can't natively expose strong ordering
//     guarantees. Perhaps more importantly, even when a command has acquired latches
//     and evaluated on a Replica, it is not guaranteed to be applied before
//     interfering commands. This is because the command may be retried outside of
//     the serialization of the spanlatch manager for any number of reasons, such as
//     leaseholder changes. When the command re-acquired its latches, it's possible
//     that interfering commands may jump ahead of it. To combat this, the
//     txnPipeliner uses chaining to throw an error when these re-orderings would
//     have affected the order that transactional requests evaluate in.
//
// The interceptor proves all in-flight writes before explicitly committing a
// transaction by tacking on a QueryIntent request for each one to the front of
// an EndTxn(Commit=true) request. The in-flight writes that are being queried
// in the batch with the EndTxn request are treated as in-flight writes for the
// purposes of parallel commits. The effect of this is that the in-flight writes
// must all be proven for a transaction to be considered implicitly committed.
// It also follows that they will need to be queried during transaction
// recovery.
//
// This is beneficial from the standpoint of latency because it means that the
// consensus latency for every write in a transaction, including the write to
// the transaction record, is paid in parallel (mod pipeline stalls) and an
// entire transaction can commit in a single consensus round-trip!
//
// On the flip side, this means that every unproven write is considered
// in-flight at the time of the commit and needs to be proven at the time of the
// commit. This is a little unfortunate because a transaction could have
// accumulated a large number of in-flight writes over a long period of time
// without proving any of them, and the more of these writes there are, the
// greater the chance that querying one of them gets delayed and delays the
// overall transaction. Additionally, the more of these writes there are, the
// more expensive transaction recovery will be if the transaction ends up stuck
// in an indeterminate commit state.
//
// Three approaches have been considered to address this, all of which revolve
// around the idea that earlier writes in a transaction may have finished
// consensus well before the EndTxn is sent. Following this logic, it would be
// in the txnPipeliner's best interest to prove in-flight writes as early as
// possible, even if no other overlapping requests force them to be proven. The
// approaches are:
//
//  1. launch a background process after each successful async write to query its
//     intents and wait for it to succeed. This would effectively solve the issue,
//     but at the cost of many more goroutines and many more QueryIntent requests,
//     most of which would be redundant because their corresponding write wouldn't
//     complete until after an EndTxn synchronously needed to prove them anyway.
//
//  2. to address the issue of an unbounded number of background goroutines
//     proving writes in approach 1, a single background goroutine could be run
//     that repeatedly loops over all in-flight writes and attempts to prove
//     them. This approach was used in an early revision of #26599 and has the nice
//     property that only one batch of QueryIntent requests is ever active at a
//     given time. It may be revisited, but for now it is not used for the same
//     reason as approach 1: most of its QueryIntent requests will be useless
//     because a transaction will send an EndTxn immediately after sending all
//     of its writes.
//
//  3. turn the KV interface into a streaming protocol (#8360) that could support
//     returning multiple results. This would allow clients to return immediately
//     after a writes "evaluation" phase completed but hold onto a handle to the
//     request and be notified immediately after its "replication" phase completes.
//     This would allow txnPipeliner to prove in-flight writes immediately after
//     they finish consensus without any extra RPCs.
//
// So far, none of these approaches have been integrated.
//
// The txnPipeliner also tracks the locks that a transaction has acquired in a
// set of spans known as the "lock footprint". This lock footprint contains
// spans encompassing all keys and key ranges where locks have been acquired at
// some point by the transaction. This set includes the bounds of locks acquired
// by all locking read and write requests. Additionally, it includes the bounds
// of locks acquired by the current and all previous epochs. These spans are
// attached to any end transaction request that is passed through the pipeliner
// to ensure that they the locks within them are released.
type txnPipeliner struct {
	st                       *cluster.Settings
	riGen                    rangeIteratorFactory // used to condense lock spans, if provided
	wrapped                  lockedSender
	disabled                 bool
	txnMetrics               *TxnMetrics
	condensedIntentsEveryN   *log.EveryN
	inflightOverBudgetEveryN *log.EveryN

	// In-flight writes are intent point writes that have not yet been proved
	// to have succeeded. They will need to be proven before the transaction
	// can commit.
	// TODO(nvanbenschoten): once we start tracking locking read requests in
	// this set, we should decide on whether to rename "in-flight writes" to
	// something else. We could rename to "in-flight locks". Or we could keep
	// "in-flight writes" but make it clear that these are lock writes of any
	// strength, and not just intent writes.
	ifWrites inFlightWriteSet
	// The in-flight writes chain index is used to uniquely identify calls to
	// chainToInFlightWrites, so that each call can limit itself to adding a
	// single QueryIntent request to the batch per overlapping in-flight write.
	ifWritesChainIndex int64
	// The transaction's lock footprint contains spans where locks (replicated
	// and unreplicated) have been acquired at some point by the transaction.
	// The span set contains spans encompassing the keys from all intent writes
	// that have already been proven during this epoch and the keys from all
	// locking reads that have been performed during this epoch. Additionally,
	// the span set contains all locks held at the end of prior epochs. All of
	// the transaction's in-flight writes are morally in this set as well, but
	// they are not stored here to avoid duplication.
	//
	// Unlike the in-flight writes, this set does not need to be tracked with
	// full precision. Instead, the tracking can be an overestimate (i.e. the
	// spans may cover keys never locked) and should be thought of as an
	// upper-bound on the influence that the transaction has had. The set
	// contains all keys spans that the transaction will need to eventually
	// clean up upon its completion.
	lockFootprint condensableSpanSet

	// writeCount counts the number of replicated lock acquisitions and intents
	// written by this txnPipeliner. This includes both in-flight and successful
	// operations.
	writeCount int64
}

// condensableSpanSetRangeIterator describes the interface of RangeIterator
// needed by the condensableSpanSetRangeIterator. Useful for mocking an
// iterator in tests.
type condensableSpanSetRangeIterator interface {
	Valid() bool
	Seek(ctx context.Context, key roachpb.RKey, scanDir ScanDirection)
	Error() error
	Desc() *roachpb.RangeDescriptor
}

// rangeIteratorFactory is used to create a condensableSpanSetRangeIterator
// lazily. It's used to avoid allocating an iterator when it's not needed. The
// factory can be configured either with a callback, used for mocking in tests,
// or with a DistSender. Can also be left empty for unittests that don't push
// memory limits in their span sets (and thus don't need collapsing).
type rangeIteratorFactory struct {
	factory func() condensableSpanSetRangeIterator
	ds      *DistSender
}

// newRangeIterator creates a range iterator. If no factory was configured, it panics.
func (f rangeIteratorFactory) newRangeIterator() condensableSpanSetRangeIterator {
	if f.factory != nil {
		return f.factory()
	}
	if f.ds != nil {
		ri := MakeRangeIterator(f.ds)
		return &ri
	}
	panic("no iterator factory configured")
}

// SendLocked implements the lockedSender interface.
func (tp *txnPipeliner) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// If an EndTxn request is part of this batch, attach the in-flight writes
	// and the lock footprint to it.
	ba, pErr := tp.attachLocksToEndTxn(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	// If we're configured to reject txns over budget, we preemptively check
	// whether this current batch is likely to push us over the edge and, if it
	// does, we reject it. Note that this check is not precise because generally
	// we can't know exactly the size of the locks that will be taken by a
	// request (think ResumeSpan); even if the check passes, we might end up over
	// budget.
	rejectOverBudget := rejectTxnOverTrackedWritesBudget.Get(&tp.st.SV)
	maxBytes := TrackedWritesMaxSize.Get(&tp.st.SV)
	rejectTxnMaxCount := rejectTxnMaxCount.Get(&tp.st.SV)
	if err := tp.maybeRejectOverBudget(ba, maxBytes, rejectOverBudget, rejectTxnMaxCount); err != nil {
		return nil, kvpb.NewError(err)
	}

	ba.AsyncConsensus = tp.canUseAsyncConsensus(ctx, ba)

	// Adjust the batch so that it doesn't miss any in-flight writes.
	ba = tp.chainToInFlightWrites(ba)

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	br, pErr := tp.wrapped.SendLocked(ctx, ba)

	// Update the in-flight write set and the lock footprint with the results of
	// the request.
	//
	// If we were configured to reject transaction when they go over budget, we
	// don't want to condense the lock spans (even if it turns out that we did go
	// over budget). The point of the rejection setting is to avoid condensing
	// because of the possible performance cliff when doing so. As such, if we did
	// go over budget despite the earlier pre-emptive check, then we stay over
	// budget. Further requests will be rejected if they attempt to take more
	// locks.
	if err := tp.updateLockTracking(
		ctx, ba, br, pErr, maxBytes, !rejectOverBudget /* condenseLocksIfOverBudget */, rejectTxnMaxCount,
	); err != nil {
		return nil, kvpb.NewError(err)
	}
	if pErr != nil {
		return nil, tp.adjustError(ctx, ba, pErr)
	}
	return tp.stripQueryIntents(br), nil
}

// maybeRejectOverBudget checks the request against the memory limit for
// tracking the txn's locks. If the request is estimated to exceed the lock
// tracking budget, an error is returned.
//
// We can only estimate what spans this request would end up locking
// (because of ResumeSpans, for example), so this is a best-effort check.
//
// NOTE: We could be more discriminate here: if the request contains an
// EndTxn among other writes (for example, if it's a 1PC batch) then we
// could allow the request through and hope that it succeeds. If if
// succeeds, then its locks are never technically counted against the
// transaction's budget because the client doesn't need to track them after
// the transaction commits. If it fails, then we'd add the lock spans to our
// tracking and exceed the budget. It's easier for this code and more
// predictable for the user if we just reject this batch, though.
func (tp *txnPipeliner) maybeRejectOverBudget(
	ba *kvpb.BatchRequest, maxBytes int64, rejectIfWouldCondense bool, rejectTxnMaxCount int64,
) error {
	// Bail early if the current request is not locking, even if we are already
	// over budget. In particular, we definitely want to permit rollbacks. We also
	// want to permit lone commits, since the damage in taking too much memory has
	// already been done.
	if !ba.IsLocking() {
		return nil
	}

	// NB: The reqEstimate is a count the number of spans in this request with
	// replicated durability. This is an estimate since accurate accounting
	// requires the response as well. For point requests this will be accurate,
	// but for scans, we will count 1 for every span. In reality for scans, it
	// could be 0 or many replicated locks. When we receive the response we will
	// get the actual counts in `updateLockTracking` and update
	// `txnPipeliner.writeCount`.
	var reqEstimate int64
	var spans []roachpb.Span
	if err := ba.LockSpanIterate(nil /* br */, func(sp roachpb.Span, durability lock.Durability) {
		spans = append(spans, sp)
		if durability == lock.Replicated {
			reqEstimate++
		}
	}); err != nil {
		return errors.Wrap(err, "iterating lock spans")
	}

	// Compute how many bytes we can allocate for locks. We account for the
	// inflight-writes conservatively, since these might turn into lock spans
	// later.
	locksBudget := maxBytes - tp.ifWrites.byteSize()

	estimate := tp.lockFootprint.estimateSize(spans, locksBudget)
	if rejectIfWouldCondense && estimate > locksBudget {
		tp.txnMetrics.TxnsRejectedByLockSpanBudget.Inc(1)
		bErr := newLockSpansOverBudgetError(estimate+tp.ifWrites.byteSize(), maxBytes, ba)
		return pgerror.WithCandidateCode(bErr, pgcode.ConfigurationLimitExceeded)
	}

	// This counts from three different sources. The inflight writes are
	// included in the tp.writeCount.
	estimateCount := tp.writeCount + reqEstimate
	// TODO(baptist): We use the same error message as the one above, to avoid
	// adding additional encoding and decoding for a backport. We could consider
	// splitting this error message in the future.
	if rejectTxnMaxCount > 0 && estimateCount > rejectTxnMaxCount {
		tp.txnMetrics.TxnsRejectedByCountLimit.Inc(1)
		bErr := newLockSpansOverBudgetError(estimateCount, rejectTxnMaxCount, ba)
		return pgerror.WithCandidateCode(bErr, pgcode.ConfigurationLimitExceeded)
	}
	return nil
}

// attachLocksToEndTxn attaches the in-flight writes and the lock footprint that
// the interceptor has been tracking to any EndTxn requests present in the
// provided batch. It augments these sets with locking requests from the current
// batch.
func (tp *txnPipeliner) attachLocksToEndTxn(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchRequest, *kvpb.Error) {
	args, hasET := ba.GetArg(kvpb.EndTxn)
	if !hasET {
		return ba, nil
	}
	et := args.(*kvpb.EndTxnRequest)
	if len(et.LockSpans) > 0 {
		return ba, kvpb.NewError(errors.AssertionFailedf("client must not pass intents to EndTxn"))
	}
	if len(et.InFlightWrites) > 0 {
		return ba, kvpb.NewError(errors.AssertionFailedf("client must not pass in-flight writes to EndTxn"))
	}

	// Populate et.LockSpans and et.InFlightWrites.
	if !tp.lockFootprint.empty() {
		et.LockSpans = append([]roachpb.Span(nil), tp.lockFootprint.asSlice()...)
	}
	if inFlight := tp.ifWrites.len(); inFlight != 0 {
		et.InFlightWrites = make([]roachpb.SequencedWrite, 0, inFlight)
		tp.ifWrites.ascend(func(w *inFlightWrite) {
			et.InFlightWrites = append(et.InFlightWrites, w.SequencedWrite)
		})
	}

	// Augment et.LockSpans and et.InFlightWrites with writes from the current
	// batch.
	for _, ru := range ba.Requests[:len(ba.Requests)-1] {
		req := ru.GetInner()
		h := req.Header()
		if kvpb.IsLocking(req) {
			// Ranged writes are added immediately to the lock spans because
			// it's not clear where they will actually leave intents. Point
			// writes are added to the in-flight writes set. All other locking
			// requests are also added to the lock spans.
			//
			// If we see any ranged writes then we know that the txnCommitter
			// will fold the in-flight writes into the lock spans immediately
			// and forgo a parallel commit, but let's not break that abstraction
			// boundary here.
			if kvpb.IsIntentWrite(req) && !kvpb.IsRange(req) {
				w := roachpb.SequencedWrite{Key: h.Key, Sequence: h.Sequence, Strength: lock.Intent}
				et.InFlightWrites = append(et.InFlightWrites, w)
			} else {
				et.LockSpans = append(et.LockSpans, h.Span())
			}
		}
	}

	// Sort both sets and condense the lock spans.
	et.LockSpans, _ = roachpb.MergeSpans(&et.LockSpans)
	sort.Sort(roachpb.SequencedWriteBySeq(et.InFlightWrites))

	if log.V(3) {
		for _, intent := range et.LockSpans {
			log.Infof(ctx, "intent: [%s,%s)", intent.Key, intent.EndKey)
		}
		for _, write := range et.InFlightWrites {
			log.Infof(ctx, "in-flight: %d:%s (%s)", write.Sequence, write.Key, write.Strength)
		}
	}
	return ba, nil
}

// canUseAsyncConsensus checks the conditions necessary for this batch to be
// allowed to set the AsyncConsensus flag.
func (tp *txnPipeliner) canUseAsyncConsensus(ctx context.Context, ba *kvpb.BatchRequest) bool {
	// Short-circuit for EndTransactions; it's common enough to have batches
	// containing a prefix of writes (which, by themselves, are all eligible for
	// async consensus) and then an EndTxn (which is not eligible). Note that
	// ba.GetArg() is efficient for EndTransactions, having its own internal
	// optimization.
	if _, hasET := ba.GetArg(kvpb.EndTxn); hasET {
		return false
	}

	if !PipelinedWritesEnabled.Get(&tp.st.SV) || tp.disabled {
		return false
	}

	// There's a memory budget for lock tracking. If this batch would push us over
	// this setting, don't allow it to perform async consensus.
	addedIFBytes := int64(0)
	maxTrackingBytes := TrackedWritesMaxSize.Get(&tp.st.SV)

	// We provide a setting to bound the number of writes we permit in a batch
	// that uses async consensus. This is useful because we'll have to prove
	// each write that uses async consensus using a QueryIntent, so there's a
	// point where it makes more sense to just perform consensus for the entire
	// batch synchronously and avoid all of the overhead of pipelining.
	if maxBatch := pipelinedWritesMaxBatchSize.Get(&tp.st.SV); maxBatch > 0 {
		batchSize := int64(len(ba.Requests))
		if batchSize > maxBatch {
			return false
		}
	}

	for _, ru := range ba.Requests {
		req := ru.GetInner()

		if req.Method() == kvpb.DeleteRange {
			// Special handling for DeleteRangeRequests.
			deleteRangeReq := req.(*kvpb.DeleteRangeRequest)
			// We'll need the list of keys deleted to verify whether replication
			// succeeded or not. Override ReturnKeys.
			//
			// NB: This means we'll return keys to the client even if it explicitly
			// set this to false. If this proves to be a problem in practice, we can
			// always add some tracking here and strip the response. Alternatively, we
			// can disable DeleteRange pipelining entirely for requests that set this
			// field to false.
			//
			// TODO(arul): Get rid of this flag entirely and always treat it as true.
			// Now that we're overriding ReturnKeys here, the number of cases where
			// this will be false are very few -- it's only when DeleteRange is part
			// of the same batch as an EndTxn request.
			deleteRangeReq.ReturnKeys = true
		}

		if !kvpb.CanPipeline(req) {
			// The current request cannot be pipelined, so it prevents us from
			// performing async consensus on the batch.
			return false
		}

		if kvpb.IsRange(req) {
			if !pipelinedRangedWritesEnabled.Get(&tp.st.SV) {
				return false
			}
		}

		if !kvpb.IsIntentWrite(req) {
			if !pipelinedLockingReadsEnabled.Get(&tp.st.SV) {
				return false
			}
		}

		// Inhibit async consensus if the batch would push us over the maximum
		// tracking memory budget. If we allowed async consensus on this batch, its
		// writes would need to be tracked precisely. By inhibiting async consensus,
		// its writes will only need to be tracked as locks, and we can compress the
		// lock spans with loss of fidelity. This helps both memory usage and the
		// eventual size of the Raft command for the commit.
		//
		// NB: this estimation is conservative because it doesn't factor
		// in that some writes may be proven by this batch and removed
		// from the in-flight write set. The real accounting in
		// inFlightWriteSet.{insert,remove} gets this right.
		addedIFBytes += keySize(req.Header().Key)
		if (tp.ifWrites.byteSize() + addedIFBytes + tp.lockFootprint.bytes) > maxTrackingBytes {
			log.VEventf(ctx, 2, "cannot perform async consensus because memory budget exceeded")
			return false
		}
	}
	return true
}

// chainToInFlightWrites ensures that we "chain" on to any in-flight writes that
// overlap the keys we're trying to read/write. We do this by prepending
// QueryIntent requests with the ErrorIfMissing option before each request that
// touches any of the in-flight writes. In effect, this allows us to prove that
// a write succeeded before depending on its existence. We later prune down the
// list of writes we proved to exist that are no longer "in-flight" in
// updateLockTracking.
func (tp *txnPipeliner) chainToInFlightWrites(ba *kvpb.BatchRequest) *kvpb.BatchRequest {
	// If there are no in-flight writes, there's nothing to chain to.
	if tp.ifWrites.len() == 0 {
		return ba
	}

	// We may need to add QueryIntent requests to the batch. These variables are
	// used to implement a copy-on-write scheme.
	forked := false
	oldReqs := ba.Requests

	// We only want to add a single QueryIntent request to the BatchRequest per
	// overlapping in-flight write. These counters allow us to accomplish this
	// without a separate data structure.
	tp.ifWritesChainIndex++
	chainIndex := tp.ifWritesChainIndex
	chainCount := 0

	for i, ru := range oldReqs {
		req := ru.GetInner()

		// If we've chained onto all the in-flight writes (ifWrites.len() ==
		// chainCount), we don't need to pile on more QueryIntents. So, only
		// do this work if that's not the case.
		if tp.ifWrites.len() > chainCount {
			// For each conflicting in-flight write, add a QueryIntent request
			// to the batch to assert that it has succeeded and "chain" onto it.
			writeIter := func(w *inFlightWrite) {
				// We don't want to modify the batch's request slice directly,
				// so fork it before modifying it.
				if !forked {
					ba = ba.ShallowCopy()
					ba.Requests = append([]kvpb.RequestUnion(nil), ba.Requests[:i]...)
					forked = true
				}

				if w.chainIndex != chainIndex {
					// The write has not already been chained onto by an earlier
					// request in this batch. Add a QueryIntent request to the
					// batch (before the conflicting request) to ensure that we
					// chain on to the success of the in-flight write.
					meta := ba.Txn.TxnMeta
					meta.Sequence = w.Sequence
					ba.Add(&kvpb.QueryIntentRequest{
						RequestHeader: kvpb.RequestHeader{
							Key: w.Key,
						},
						Txn:            meta,
						Strength:       w.Strength,
						IgnoredSeqNums: ba.Txn.IgnoredSeqNums,
						ErrorIfMissing: true,
					})

					// Record that the key has been chained onto at least once
					// in this batch so that we don't chain onto it again. If
					// we fail to prove the write exists for any reason, future
					// requests will use a different chainIndex and will try to
					// prove the write again.
					w.chainIndex = chainIndex
					chainCount++
				}
			}

			if !kvpb.IsTransactional(req) {
				// Non-transactional requests require that we stall the entire
				// pipeline by chaining on to all in-flight writes. This is
				// because their request header is often insufficient to
				// determine all of the keys that they will interact with.
				tp.ifWrites.ascend(writeIter)
			} else if et, ok := req.(*kvpb.EndTxnRequest); ok {
				if et.Commit {
					// EndTxns need to prove all in-flight writes before being
					// allowed to succeed themselves.
					tp.ifWrites.ascend(writeIter)
				}
			} else {
				// Transactional reads and writes needs to chain on to any
				// overlapping in-flight writes.
				s := req.Header()
				tp.ifWrites.ascendRange(s.Key, s.EndKey, writeIter)
			}
		}

		// If the BatchRequest's slice of requests has been forked from the original,
		// append the request to the new slice.
		if forked {
			ba.Add(req)
		}
	}

	return ba
}

// updateLockTracking reads the response for the given request and uses it to
// update the tracked in-flight write set and lock footprint. It does so by
// performing three actions:
//  1. it adds all async writes that the request performed to the in-flight
//     write set.
//  2. it adds all non-async writes and locking reads that the request
//     performed to the lock footprint.
//  3. it moves all in-flight writes that the request proved to exist from
//     the in-flight writes set to the lock footprint.
//
// The transaction's lock set is only allowed to go up to maxBytes. If it goes
// over, the behavior depends on the condenseLocksIfOverBudget - we either
// compress the spans with loss of fidelity (which can be a significant
// performance problem at commit/rollback time) or we don't compress (and we
// stay over budget).
//
// If no response is provided (indicating an error), all writes from the batch
// are added directly to the lock footprint to avoid leaking any locks when the
// transaction cleans up.
func (tp *txnPipeliner) updateLockTracking(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	br *kvpb.BatchResponse,
	pErr *kvpb.Error,
	maxBytes int64,
	condenseLocksIfOverBudget bool,
	rejectTxnMaxCount int64,
) error {
	if err := tp.updateLockTrackingInner(ctx, ba, br, pErr); err != nil {
		return err
	}

	// Because the in-flight writes can include locking reads, and because we
	// don't estimate the size of the locks accurately for ranged locking reads,
	// it is possible that ifWrites have exceeded the maxBytes threshold. That's
	// fine for now, but we add some observability to be aware of this happening.
	if tp.ifWrites.byteSize() > maxBytes {
		if tp.inflightOverBudgetEveryN.ShouldLog() || log.ExpensiveLogEnabled(ctx, 2) {
			log.Warningf(ctx, "a transaction's in-flight writes and locking reads have "+
				"exceeded the intent tracking limit (kv.transaction.max_intents_bytes). "+
				"in-flight writes and locking reads size: %d bytes, txn: %s, ba: %s",
				tp.ifWrites.byteSize(), ba.Txn, ba.Summary())
		}
		tp.txnMetrics.TxnsInFlightLocksOverTrackingBudget.Inc(1)
	}
	// Similar to the in-flight writes case above, we may have gone over the
	// rejectTxnMaxCount threshold because we don't accurately estimate the
	// number of ranged locking reads before sending the request.
	if rejectTxnMaxCount > 0 && tp.writeCount > rejectTxnMaxCount {
		if tp.inflightOverBudgetEveryN.ShouldLog() || log.ExpensiveLogEnabled(ctx, 2) {
			log.Warningf(ctx, "a transaction has exceeded the maximum number of writes "+
				"allowed by kv.transaction.max_intents_and_locks: "+
				"count: %d, txn: %s, ba: %s", tp.writeCount, ba.Txn, ba.Summary())
		}
		tp.txnMetrics.TxnsResponseOverCountLimit.Inc(1)
	}

	// Deal with compacting the lock spans.

	// Compute how many bytes are left for locks after accounting for the
	// in-flight writes. It's possible that locksBudget is negative, but the
	// remainder of this function and maybeCondense handle this case (each span
	// will be maximally condensed).
	locksBudget := maxBytes - tp.ifWrites.byteSize()
	// If we're below budget, there's nothing more to do.
	if tp.lockFootprint.bytesSize() <= locksBudget {
		return nil
	}

	// We're over budget. If condenseLocksIfOverBudget is set, we condense the
	// lock spans. If not set, we defer the work to a future locking request where
	// we're going to try to save space without condensing and perhaps reject the
	// txn if we fail (see the estimateSize() call).

	if !condenseLocksIfOverBudget {
		return nil
	}

	// After adding new writes to the lock footprint, check whether we need to
	// condense the set to stay below memory limits.
	alreadyCondensed := tp.lockFootprint.condensed
	condensed := tp.lockFootprint.maybeCondense(ctx, tp.riGen, locksBudget)
	if condensed && !alreadyCondensed {
		if tp.condensedIntentsEveryN.ShouldLog() || log.ExpensiveLogEnabled(ctx, 2) {
			log.Warningf(ctx,
				"a transaction has hit the intent tracking limit (kv.transaction.max_intents_bytes); "+
					"is it a bulk operation? Intent cleanup will be slower. txn: %s ba: %s",
				ba.Txn, ba.Summary())
		}
		tp.txnMetrics.TxnsWithCondensedIntents.Inc(1)
		tp.txnMetrics.TxnsWithCondensedIntentsGauge.Inc(1)
	}
	return nil
}

func (tp *txnPipeliner) updateLockTrackingInner(
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse, pErr *kvpb.Error,
) error {
	// If the request failed, add all lock acquisitions attempts directly to the
	// lock footprint. This reduces the likelihood of dangling locks blocking
	// concurrent requests for extended periods of time. See #3346.
	if pErr != nil {
		// However, as an optimization, if the error indicates that a specific
		// request (identified by pErr.Index) unambiguously did not acquire any
		// locks, we ignore that request for the purposes of accounting for lock
		// spans. This is important for transactions that only perform a single
		// request and hit an unambiguous error like a ConditionFailedError, as it
		// can allow them to avoid sending a rollback. It is also important for
		// transactions that throw a LockConflictError due to heavy contention on a
		// certain key after either passing a Error wait policy or hitting a lock
		// timeout / queue depth limit. In such cases, this optimization prevents
		// these transactions from adding even more load to the contended key by
		// trying to perform unnecessary intent resolution.
		baStripped := *ba
		if kvpb.ErrPriority(pErr.GoError()) <= kvpb.ErrorScoreUnambiguousError && pErr.Index != nil {
			baStripped.Requests = make([]kvpb.RequestUnion, len(ba.Requests)-1)
			copy(baStripped.Requests, ba.Requests[:pErr.Index.Index])
			copy(baStripped.Requests[pErr.Index.Index:], ba.Requests[pErr.Index.Index+1:])
		}
		return baStripped.LockSpanIterate(nil, tp.trackLocks)
	}

	// Similarly, if the transaction is now finalized, we don't need to
	// accurately update the lock tracking.
	if br.Txn.Status.IsFinalized() {
		switch br.Txn.Status {
		case roachpb.ABORTED:
			// If the transaction is now ABORTED, add all locks acquired by the
			// batch directly to the lock footprint. We don't know which of
			// these succeeded.
			return ba.LockSpanIterate(nil, tp.trackLocks)
		case roachpb.COMMITTED:
			// If the transaction is now COMMITTED, it must not have any more
			// in-flight writes, so clear them. Technically we should move all
			// of these to the lock footprint, but since the transaction is
			// already committed, there's no reason to.
			tp.ifWrites.clear(
				/* reuse - we're not going to use this Btree again, so there's no point in
				   moving the nodes to a free list */
				false)
			return nil
		default:
			panic("unexpected")
		}
	}

	for i, ru := range ba.Requests {
		req := ru.GetInner()
		resp := br.Responses[i].GetInner()

		if qiReq, ok := req.(*kvpb.QueryIntentRequest); ok {
			// Remove any in-flight writes that were proven to exist. It should not be
			// possible for a QueryIntentRequest with the ErrorIfMissing option set to
			// return without error and with FoundIntent=false if the request was
			// evaluated on the server.
			//
			// However, it is possible that the batch was split on a range boundary
			// and hit a batch-wide key or byte limit before a portion was even sent
			// by the DistSender. In such cases, an empty response will be returned
			// for the requests that were not evaluated (see fillSkippedResponses).
			// For these requests, we neither proved nor disproved the existence of
			// their intent, so we ignore the response.
			//
			// TODO(nvanbenschoten): we only need to check FoundIntent, but this field
			// was not set before v23.2, so for now, we check both fields. Remove this
			// in the future.
			qiResp := resp.(*kvpb.QueryIntentResponse)
			if qiResp.FoundIntent || qiResp.FoundUnpushedIntent {
				tp.ifWrites.remove(qiReq.Key, qiReq.Txn.Sequence, qiReq.Strength)
				// Move to lock footprint.
				tp.lockFootprint.insert(roachpb.Span{Key: qiReq.Key})
			}
		} else if kvpb.IsLocking(req) {
			// If the request intended to acquire locks, track its lock spans.
			seq := req.Header().Sequence
			str := lock.Intent
			if readOnlyReq, ok := req.(kvpb.LockingReadRequest); ok {
				str, _ = readOnlyReq.KeyLocking()
			}
			trackLocks := func(span roachpb.Span, durability lock.Durability) {
				if ba.AsyncConsensus {
					// Record any writes that were performed asynchronously. We'll
					// need to prove that these succeeded sometime before we commit.
					if span.EndKey != nil {
						log.Fatalf(ctx, "unexpected multi-key intent pipelined")
					}
					tp.ifWrites.insert(span.Key, seq, str)
				} else {
					// If the lock acquisitions weren't performed asynchronously
					// then add them directly to our lock footprint.
					tp.lockFootprint.insert(span)
				}
				if durability == lock.Replicated {
					tp.writeCount++
				}
			}
			if err := kvpb.LockSpanIterate(req, resp, trackLocks); err != nil {
				return errors.Wrap(err, "iterating lock spans")
			}
		}
	}
	return nil
}

func (tp *txnPipeliner) trackLocks(s roachpb.Span, durability lock.Durability) {
	tp.lockFootprint.insert(s)
	if durability == lock.Replicated {
		tp.writeCount++
	}
}

// stripQueryIntents adjusts the BatchResponse to hide the fact that this
// interceptor added new requests to the batch. It returns an adjusted batch
// response without the responses that correspond to these added requests.
func (tp *txnPipeliner) stripQueryIntents(br *kvpb.BatchResponse) *kvpb.BatchResponse {
	j := 0
	for i, ru := range br.Responses {
		if ru.GetQueryIntent() != nil {
			continue
		}
		if i != j {
			br.Responses[j] = br.Responses[i]
		}
		j++
	}
	br.Responses = br.Responses[:j]
	return br
}

// adjustError adjusts the provided error based on the request that caused it.
// It transforms any IntentMissingError into a TransactionRetryError and fixes
// the error's index position.
func (tp *txnPipeliner) adjustError(
	ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error,
) *kvpb.Error {
	// Fix the error index to hide the impact of any QueryIntent requests.
	if pErr.Index != nil {
		before := int32(0)
		for _, ru := range ba.Requests[:int(pErr.Index.Index)] {
			req := ru.GetInner()
			if req.Method() == kvpb.QueryIntent {
				before++
			}
		}
		pErr.Index.Index -= before
	}

	// Turn an IntentMissingError into a transactional retry error.
	if ime, ok := pErr.GetDetail().(*kvpb.IntentMissingError); ok {
		log.VEventf(ctx, 2, "transforming intent missing error into retry: %v", ime)
		err := kvpb.NewTransactionRetryError(
			kvpb.RETRY_ASYNC_WRITE_FAILURE, redact.Sprintf("missing intent on: %s", ime.Key))
		retryErr := kvpb.NewErrorWithTxn(err, pErr.GetTxn())
		retryErr.Index = pErr.Index
		return retryErr
	}
	return pErr
}

// setWrapped implements the txnInterceptor interface.
func (tp *txnPipeliner) setWrapped(wrapped lockedSender) {
	tp.wrapped = wrapped
}

// populateLeafInputState is part of the txnInterceptor interface.
func (tp *txnPipeliner) populateLeafInputState(tis *roachpb.LeafTxnInputState) {
	tis.InFlightWrites = tp.ifWrites.asSlice()
}

// initializeLeaf loads the in-flight writes for a leaf transaction.
func (tp *txnPipeliner) initializeLeaf(tis *roachpb.LeafTxnInputState) {
	// Copy all in-flight writes into the inFlightWrite tree.
	for _, w := range tis.InFlightWrites {
		tp.ifWrites.insert(w.Key, w.Sequence, w.Strength)
	}
}

// populateLeafFinalState is part of the txnInterceptor interface.
func (tp *txnPipeliner) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (tp *txnPipeliner) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked implements the txnInterceptor interface.
func (tp *txnPipeliner) epochBumpedLocked() {
	// Move all in-flight writes into the lock footprint. These writes no longer
	// need to be tracked precisely, but we don't want to forget about them and
	// fail to clean them up.
	if tp.ifWrites.len() > 0 {
		tp.ifWrites.ascend(func(w *inFlightWrite) {
			tp.lockFootprint.insert(roachpb.Span{Key: w.Key})
		})
		tp.lockFootprint.mergeAndSort()
		tp.ifWrites.clear(true /* reuse */)
	}
}

// createSavepointLocked is part of the txnInterceptor interface.
func (tp *txnPipeliner) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (tp *txnPipeliner) rollbackToSavepointLocked(ctx context.Context, s savepoint) {
	// Move all the writes in txnPipeliner that are not in the savepoint to the
	// lock footprint. We no longer care if these write succeed or fail, so we're
	// going to stop tracking these as in-flight writes. The respective intents
	// still need to be cleaned up at the end of the transaction.
	var writesToDelete []*inFlightWrite
	needCollecting := !s.Initial()
	tp.ifWrites.ascend(func(w *inFlightWrite) {
		if w.Sequence >= s.seqNum {
			tp.lockFootprint.insert(roachpb.Span{Key: w.Key})
			if needCollecting {
				writesToDelete = append(writesToDelete, w)
			}
		}
	})
	tp.lockFootprint.mergeAndSort()

	// Restore the inflight writes from the savepoint (minus the ones that have
	// been verified in the meantime) by removing all the extra ones.
	if needCollecting {
		for _, ifw := range writesToDelete {
			tp.ifWrites.remove(ifw.Key, ifw.Sequence, ifw.Strength)
		}
	} else {
		tp.ifWrites.clear(true /* reuse */)
	}
}

// closeLocked implements the txnInterceptor interface.
func (tp *txnPipeliner) closeLocked() {
	if tp.lockFootprint.condensed {
		tp.txnMetrics.TxnsWithCondensedIntentsGauge.Dec(1)
	}
}

// hasAcquiredLocks returns whether the interceptor has made an attempt to
// acquire any locks, whether doing so was known to be successful or not.
func (tp *txnPipeliner) hasAcquiredLocks() bool {
	return tp.ifWrites.len() > 0 || !tp.lockFootprint.empty()
}

// inFlightWrites represent a commitment to proving (via QueryIntent) that
// a point write succeeded in replicating an intent with a specific sequence
// number and strength.
type inFlightWrite struct {
	roachpb.SequencedWrite
	// chainIndex is used to avoid chaining on to the same in-flight write
	// multiple times in the same batch. Each index uniquely identifies a
	// call to txnPipeliner.chainToInFlightWrites.
	chainIndex int64
}

// makeInFlightWrite constructs an inFlightWrite.
func makeInFlightWrite(key roachpb.Key, seq enginepb.TxnSeq, str lock.Strength) inFlightWrite {
	return inFlightWrite{SequencedWrite: roachpb.SequencedWrite{
		Key: key, Sequence: seq, Strength: str,
	}}
}

// Less implements the gbtree.Item interface.
//
// inFlightWrites are ordered by Key, then by Sequence, then by Strength. Two
// inFlightWrites with the same Key but different Sequences and/or Strengths are
// not considered equal and are maintained separately in the inFlightWritesSet.
func (a *inFlightWrite) Less(bItem gbtree.Item) bool {
	b := bItem.(*inFlightWrite)
	kCmp := a.Key.Compare(b.Key)
	if kCmp != 0 {
		// Different Keys.
		return kCmp < 0
	}
	if a.Sequence != b.Sequence {
		// Different Sequence.
		return a.Sequence < b.Sequence
	}
	if a.Strength != b.Strength {
		// Different Strength.
		return a.Strength < b.Strength
	}
	// Equal.
	return false
}

// inFlightWriteSet is an ordered set of in-flight point writes. Given a set
// of n elements, the structure supports O(log n) insertion of new in-flight
// writes, O(log n) removal of existing in-flight writes, and O(m + log n)
// retrieval over m in-flight writes that overlap with a given key.
type inFlightWriteSet struct {
	t     *gbtree.BTree
	bytes int64

	// Avoids allocs.
	tmp1, tmp2 inFlightWrite
	alloc      inFlightWriteAlloc
}

// insert attempts to insert an in-flight write that has not been proven to have
// succeeded into the in-flight write set.
func (s *inFlightWriteSet) insert(key roachpb.Key, seq enginepb.TxnSeq, str lock.Strength) {
	if s.t == nil {
		// Lazily initialize btree.
		s.t = gbtree.New(txnPipelinerBtreeDegree)
	}

	w := s.alloc.alloc(key, seq, str)
	delItem := s.t.ReplaceOrInsert(w)
	if delItem != nil {
		// An in-flight write with the same key and sequence already existed in the
		// set. We replaced it with an identical in-flight write.
		*delItem.(*inFlightWrite) = inFlightWrite{} // for GC
	} else {
		s.bytes += keySize(key)
	}
}

// remove attempts to remove an in-flight write from the in-flight write set.
// The method will be a no-op if the write was already proved.
func (s *inFlightWriteSet) remove(key roachpb.Key, seq enginepb.TxnSeq, str lock.Strength) {
	if s.len() == 0 {
		// Set is empty.
		return
	}

	// Delete the write from the in-flight writes set.
	s.tmp1 = makeInFlightWrite(key, seq, str)
	delItem := s.t.Delete(&s.tmp1)
	if delItem == nil {
		// The write was already proven or the txn epoch was incremented.
		return
	}
	*delItem.(*inFlightWrite) = inFlightWrite{} // for GC
	s.bytes -= keySize(key)

	// Assert that the byte accounting is believable.
	if s.bytes < 0 {
		panic("negative in-flight write size")
	} else if s.t.Len() == 0 && s.bytes != 0 {
		panic("non-zero in-flight write size with 0 in-flight writes")
	}
}

// ascend calls the provided function for every write in the set.
func (s *inFlightWriteSet) ascend(f func(w *inFlightWrite)) {
	if s.len() == 0 {
		// Set is empty.
		return
	}
	s.t.Ascend(func(i gbtree.Item) bool {
		f(i.(*inFlightWrite))
		return true
	})
}

// ascendRange calls the provided function for every write in the set
// with a key in the range [start, end).
func (s *inFlightWriteSet) ascendRange(start, end roachpb.Key, f func(w *inFlightWrite)) {
	if s.len() == 0 {
		// Set is empty.
		return
	}
	s.tmp1 = makeInFlightWrite(start, 0, 0)
	if end == nil {
		// Point lookup.
		s.tmp2 = makeInFlightWrite(start, math.MaxInt32, 0)
	} else {
		// Range lookup.
		s.tmp2 = makeInFlightWrite(end, 0, 0)
	}
	s.t.AscendRange(&s.tmp1, &s.tmp2, func(i gbtree.Item) bool {
		f(i.(*inFlightWrite))
		return true
	})
}

// len returns the number of the in-flight writes in the set.
func (s *inFlightWriteSet) len() int {
	if s.t == nil {
		return 0
	}
	return s.t.Len()
}

// byteSize returns the size in bytes of the in-flight writes in the set.
func (s *inFlightWriteSet) byteSize() int64 {
	return s.bytes
}

// clear purges all elements from the in-flight write set and frees associated
// memory. The reuse flag indicates whether the caller is intending to reuse
// the set or not.
func (s *inFlightWriteSet) clear(reuse bool) {
	if s.t == nil {
		return
	}
	s.t.Clear(reuse /* addNodesToFreelist */)
	s.bytes = 0
	s.alloc.clear()
}

// asSlice returns the in-flight writes, ordered by key.
func (s *inFlightWriteSet) asSlice() []roachpb.SequencedWrite {
	l := s.len()
	if l == 0 {
		return nil
	}
	writes := make([]roachpb.SequencedWrite, 0, l)
	s.ascend(func(w *inFlightWrite) {
		writes = append(writes, w.SequencedWrite)
	})
	return writes
}

// inFlightWriteAlloc provides chunk allocation of inFlightWrites,
// amortizing the overhead of each allocation.
type inFlightWriteAlloc []inFlightWrite

// alloc allocates a new inFlightWrite with the specified key, sequence number,
// and strength.
func (a *inFlightWriteAlloc) alloc(
	key roachpb.Key, seq enginepb.TxnSeq, str lock.Strength,
) *inFlightWrite {
	// If the current alloc slice has no extra capacity, reallocate a new chunk.
	if cap(*a)-len(*a) == 0 {
		const chunkAllocMinSize = 4
		const chunkAllocMaxSize = 1024

		allocSize := cap(*a) * 2
		if allocSize < chunkAllocMinSize {
			allocSize = chunkAllocMinSize
		} else if allocSize > chunkAllocMaxSize {
			allocSize = chunkAllocMaxSize
		}
		*a = make([]inFlightWrite, 0, allocSize)
	}

	*a = (*a)[:len(*a)+1]
	w := &(*a)[len(*a)-1]
	*w = makeInFlightWrite(key, seq, str)
	return w
}

// clear removes all allocated in-flight writes and attempts to reclaim as
// much allocated memory as possible.
func (a *inFlightWriteAlloc) clear() {
	for i := range *a {
		(*a)[i] = inFlightWrite{} // for GC
	}
	*a = (*a)[:0]
}

func keySize(k roachpb.Key) int64 {
	return int64(len(k))
}
