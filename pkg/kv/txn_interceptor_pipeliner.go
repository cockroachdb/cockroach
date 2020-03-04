// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/google/btree"
)

// The degree of the inFlightWrites btree.
const txnPipelinerBtreeDegree = 32

var pipelinedWritesEnabled = settings.RegisterBoolSetting(
	"kv.transaction.write_pipelining_enabled",
	"if enabled, transactional writes are pipelined through Raft consensus",
	true,
)
var pipelinedWritesMaxInFlightSize = settings.RegisterByteSizeSetting(
	// TODO(nvanbenschoten): The need for this extra setting alongside
	// kv.transaction.max_intents_bytes indicates that we should explore
	// the unification of intent tracking and in-flight write tracking.
	// The two mechanisms track subtly different information, but there's
	// no fundamental reason why they can't be unified.
	"kv.transaction.write_pipelining_max_outstanding_size",
	"maximum number of bytes used to track in-flight pipelined writes before disabling pipelining",
	1<<18, /* 256 KB */
)
var pipelinedWritesMaxBatchSize = settings.RegisterNonNegativeIntSetting(
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
)

// trackedWritesMaxSize is a threshold in bytes for intent spans stored on the
// coordinator during the lifetime of a transaction. Intents are included with a
// transaction on commit or abort, to be cleaned up asynchronously. If they
// exceed this threshold, they're condensed to avoid memory blowup both on the
// coordinator and (critically) on the EndTxn command at the Raft group
// responsible for the transaction record.
var trackedWritesMaxSize = settings.RegisterPublicIntSetting(
	"kv.transaction.max_intents_bytes",
	"maximum number of bytes used to track write intents in transactions",
	1<<18, /* 256 KB */
)

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
// 1. requests proposed to Raft will not necessarily succeed. For any number of
//    reasons, the request may make it through Raft and be discarded or fail to
//    ever even be replicated. A transaction must check that all async writes
//    succeeded before committing. However, when these proposals do fail, their
//    errors aren't particularly interesting to a transaction. This is because
//    these errors are not deterministic Transaction-domain errors that a
//    transaction must adhere to for correctness such as conditional-put errors or
//    other symptoms of constraint violations. These kinds of errors are all
//    discovered during write *evaluation*, which an async write will perform
//    synchronously before consensus. Any error during consensus is outside of the
//    Transaction-domain and can always trigger a transaction retry.
//
// 2. transport layers beneath the txnPipeliner do not provide strong enough
//    ordering guarantees between concurrent requests in the same transaction to
//    avoid needing explicit chaining. For instance, DistSender uses unary gRPC
//    requests instead of gRPC streams, so it can't natively expose strong ordering
//    guarantees. Perhaps more importantly, even when a command has acquired latches
//    and evaluated on a Replica, it is not guaranteed to be applied before
//    interfering commands. This is because the command may be retried outside of
//    the serialization of the spanlatch manager for any number of reasons, such as
//    leaseholder changes. When the command re-acquired its latches, it's possible
//    that interfering commands may jump ahead of it. To combat this, the
//    txnPipeliner uses chaining to throw an error when these re-orderings would
//    have affected the order that transactional requests evaluate in.
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
// This is fantastic from the standpoint of transaction latency because it means
// that the consensus latency for every write in a transaction, including the
// write to the transaction record, is paid in parallel (mod pipeline stalls)
// and an entire transaction can commit in a single consensus round-trip!
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
// 1. launch a background process after each successful async write to query its
//    intents and wait for it to succeed. This would effectively solve the issue,
//    but at the cost of many more goroutines and many more QueryIntent requests,
//    most of which would be redundant because their corresponding write wouldn't
//    complete until after an EndTxn synchronously needed to prove them anyway.
//
// 2. to address the issue of an unbounded number of background goroutines
//    proving writes in approach 1, a single background goroutine could be run
//    that repeatedly loops over all in-flight writes and attempts to prove
//    them. This approach was used in an early revision of #26599 and has the nice
//    property that only one batch of QueryIntent requests is ever active at a
//    given time. It may be revisited, but for now it is not used for the same
//    reason as approach 1: most of its QueryIntent requests will be useless
//    because a transaction will send an EndTxn immediately after sending all
//    of its writes.
//
// 3. turn the KV interface into a streaming protocol (#8360) that could support
//    returning multiple results. This would allow clients to return immediately
//    after a writes "evaluation" phase completed but hold onto a handle to the
//    request and be notified immediately after its "replication" phase completes.
//    This would allow txnPipeliner to prove in-flight writes immediately after
//    they finish consensus without any extra RPCs.
//
// So far, none of these approaches have been integrated.
type txnPipeliner struct {
	st *cluster.Settings
	// Optional; used to condense intent spans, if provided. If not provided,
	// a transaction's write footprint may grow without bound.
	riGen    RangeIteratorGen
	wrapped  lockedSender
	disabled bool

	// In-flight writes are intent point writes that have not yet been proved
	// to have succeeded. They will need to be proven before the transaction
	// can commit.
	ifWrites inFlightWriteSet
	// The transaction's write footprint contains spans where intent writes have
	// been performed at some point by the transaction. The span set contains
	// spans encompassing all writes that have already been proven in this epoch
	// and all writes, in-flight or not, at the end of prior epochs. All of the
	// transaction's in-flight writes are morally in this set as well, but they
	// are not stored here to avoid duplication.
	//
	// Unlike the in-flight writes, this set does not need to be tracked with
	// full precision. Instead, the tracking can be an overestimate (i.e. the
	// spans may cover keys never written to) and should be thought of as an
	// upper-bound on the influence that the transaction has had. The set
	// contains all keys spans that the transaction will need to eventually
	// clean up upon its completion.
	footprint condensableSpanSet
}

// SendLocked implements the lockedSender interface.
func (tp *txnPipeliner) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// If an EndTxn request is part of this batch, attach the in-flight writes
	// and the write footprint to it.
	ba, pErr := tp.attachWritesToEndTxn(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	// Adjust the batch so that it doesn't miss any in-flight writes.
	ba = tp.chainToInFlightWrites(ba)

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	br, pErr := tp.wrapped.SendLocked(ctx, ba)

	// Update the in-flight write set and the write footprint with the results
	// of the request.
	tp.updateWriteTracking(ctx, ba, br)
	if pErr != nil {
		return nil, tp.adjustError(ctx, ba, pErr)
	}
	return tp.stripQueryIntents(br), nil
}

// attachWritesToEndTxn attaches the in-flight writes and the write footprint
// that the interceptor has been tracking to any EndTxn requests present in the
// provided batch. It augments these sets with writes from the current batch.
func (tp *txnPipeliner) attachWritesToEndTxn(
	ctx context.Context, ba roachpb.BatchRequest,
) (roachpb.BatchRequest, *roachpb.Error) {
	args, hasET := ba.GetArg(roachpb.EndTxn)
	if !hasET {
		return ba, nil
	}
	et := args.(*roachpb.EndTxnRequest)
	if len(et.IntentSpans) > 0 {
		return ba, roachpb.NewErrorf("client must not pass intents to EndTxn")
	}
	if len(et.InFlightWrites) > 0 {
		return ba, roachpb.NewErrorf("client must not pass in-flight writes to EndTxn")
	}

	// Populate et.IntentSpans and et.InFlightWrites.
	if !tp.footprint.empty() {
		et.IntentSpans = append([]roachpb.Span(nil), tp.footprint.asSlice()...)
	}
	if inFlight := tp.ifWrites.len(); inFlight != 0 {
		et.InFlightWrites = make([]roachpb.SequencedWrite, 0, inFlight)
		tp.ifWrites.ascend(func(w *inFlightWrite) {
			et.InFlightWrites = append(et.InFlightWrites, w.SequencedWrite)
		})
	}

	// Augment et.IntentSpans and et.InFlightWrites with writes from
	// the current batch.
	for _, ru := range ba.Requests[:len(ba.Requests)-1] {
		req := ru.GetInner()
		h := req.Header()
		if roachpb.IsTransactionWrite(req) {
			// Ranged writes are added immediately to the intent spans because
			// it's not clear where they will actually leave intents. Point
			// writes are added to the in-flight writes set.
			//
			// If we see any ranged writes then we know that the txnCommitter
			// will fold the in-flight writes into the intent spans immediately
			// and forgo a parallel commit, but let's not break that abstraction
			// boundary here.
			if roachpb.IsRange(req) {
				et.IntentSpans = append(et.IntentSpans, h.Span())
			} else {
				w := roachpb.SequencedWrite{Key: h.Key, Sequence: h.Sequence}
				et.InFlightWrites = append(et.InFlightWrites, w)
			}
		}
	}

	// Sort both sets and condense the intent spans.
	et.IntentSpans, _ = roachpb.MergeSpans(et.IntentSpans)
	sort.Sort(roachpb.SequencedWriteBySeq(et.InFlightWrites))

	if log.V(3) {
		for _, intent := range et.IntentSpans {
			log.Infof(ctx, "intent: [%s,%s)", intent.Key, intent.EndKey)
		}
		for _, write := range et.InFlightWrites {
			log.Infof(ctx, "in-flight: %d:%s", write.Sequence, write.Key)
		}
	}
	return ba, nil
}

// chainToInFlightWrites ensures that we "chain" on to any in-flight writes that
// overlap the keys we're trying to read/write. We do this by prepending
// QueryIntent requests with the ErrorIfMissing option before each request that
// touches any of the in-flight writes. In effect, this allows us to prove that
// a write succeeded before depending on its existence. We later prune down the
// list of writes we proved to exist that are no longer "in-flight" in
// updateWriteTracking.
func (tp *txnPipeliner) chainToInFlightWrites(ba roachpb.BatchRequest) roachpb.BatchRequest {
	asyncConsensus := pipelinedWritesEnabled.Get(&tp.st.SV) && !tp.disabled

	// We provide a setting to bound the size of in-flight writes that the
	// pipeliner is tracking. If this batch would push us over this setting,
	// don't allow it to perform async consensus.
	addedIFBytes := int64(0)
	maxIFBytes := pipelinedWritesMaxInFlightSize.Get(&tp.st.SV)

	// We provide a setting to bound the number of writes we permit in a batch
	// that uses async consensus. This is useful because we'll have to prove
	// each write that uses async consensus using a QueryIntent, so there's a
	// point where it makes more sense to just perform consensus for the entire
	// batch synchronously and avoid all of the overhead of pipelining.
	if maxBatch := pipelinedWritesMaxBatchSize.Get(&tp.st.SV); maxBatch > 0 {
		batchSize := int64(len(ba.Requests))
		if batchSize > maxBatch {
			asyncConsensus = false
		}
	}

	forked := false
	oldReqs := ba.Requests
	// TODO(nvanbenschoten): go 1.11 includes an optimization to quickly clear
	// out an entire map. That might make it cost effective to maintain a single
	// chainedKeys map between calls to this function.
	var chainedKeys map[string]struct{}
	for i, ru := range oldReqs {
		if !asyncConsensus && !forked && tp.ifWrites.len() == len(chainedKeys) {
			// If there are no in-flight writes or all in-flight writes
			// have been chained onto and async consensus is disallowed,
			// short-circuit immediately.
			break
		}
		req := ru.GetInner()

		if asyncConsensus {
			// If we're currently planning on performing the batch with
			// performing async consensus, determine whether this request
			// changes that.
			if !roachpb.IsTransactionWrite(req) || roachpb.IsRange(req) {
				// Only allow batches consisting of solely transactional point
				// writes to perform consensus asynchronously.
				// TODO(nvanbenschoten): We could allow batches with reads and point
				// writes to perform async consensus, but this would be a bit
				// tricky. Any read would need to chain on to any write that came
				// before it in the batch and overlaps. For now, it doesn't seem
				// worth it.
				asyncConsensus = false
			} else {
				// Only allow batches that would not push us over the maximum
				// in-flight write size limit to perform consensus asynchronously.
				//
				// NB: this estimation is conservative because it doesn't factor
				// in that some writes may be proven by this batch and removed
				// from the in-flight write set. The real accounting in
				// inFlightWriteSet.{insert,remove} gets this right.
				addedIFBytes += keySize(req.Header().Key)
				asyncConsensus = (tp.ifWrites.byteSize() + addedIFBytes) <= maxIFBytes
			}
		}

		if tp.ifWrites.len() > len(chainedKeys) {
			// For each conflicting in-flight write, add a QueryIntent request
			// to the batch to assert that it has succeeded and "chain" onto it.
			writeIter := func(w *inFlightWrite) {
				// We don't want to modify the batch's request slice directly,
				// so fork it before modifying it.
				if !forked {
					ba.Requests = append([]roachpb.RequestUnion(nil), ba.Requests[:i]...)
					forked = true
				}

				if _, ok := chainedKeys[string(w.Key)]; !ok {
					// The write has not already been chained onto by an earlier
					// request in this batch. Add a QueryIntent request to the
					// batch (before the conflicting request) to ensure that we
					// chain on to the success of the in-flight write.
					meta := ba.Txn.TxnMeta
					meta.Sequence = w.Sequence
					ba.Add(&roachpb.QueryIntentRequest{
						RequestHeader: roachpb.RequestHeader{
							Key: w.Key,
						},
						Txn:            meta,
						ErrorIfMissing: true,
					})

					// Record that the key has been chained onto at least once
					// in this batch so that we don't chain onto it again.
					if chainedKeys == nil {
						chainedKeys = make(map[string]struct{})
					}
					chainedKeys[string(w.Key)] = struct{}{}
				}
			}

			if !roachpb.IsTransactional(req) {
				// Non-transactional requests require that we stall the entire
				// pipeline by chaining on to all in-flight writes. This is
				// because their request header is often insufficient to
				// determine all of the keys that they will interact with.
				tp.ifWrites.ascend(writeIter)
			} else if et, ok := req.(*roachpb.EndTxnRequest); ok {
				if et.Commit {
					// EndTxns need to prove all in-flight writes before being
					// allowed to succeed themselves.
					tp.ifWrites.ascend(writeIter)
				}
			} else {
				// Transactional reads and writes needs to chain on to any
				// overlapping in-flight writes.
				s := req.Header().Span()
				tp.ifWrites.ascendRange(s.Key, s.EndKey, writeIter)
			}
		}

		// If the BatchRequest's slice of requests has been forked from the original,
		// append the request to the new slice.
		if forked {
			ba.Add(req)
		}
	}

	// Set the batch's AsyncConsensus flag based on whether AsyncConsensus is
	// permitted for the batch.
	ba.AsyncConsensus = asyncConsensus
	return ba
}

// updateWriteTracking reads the response for the given request and uses it to
// update the tracked in-flight write set and write footprint. It does so by
// performing three actions: 1. it adds all async writes that the request
// performed to the in-flight
//    write set.
// 2. it adds all non-async writes that the request performed to the write
//    footprint.
// 3. it moves all in-flight writes that the request proved to exist from
//    the in-flight writes set to the write footprint.
//
// After updating the write sets, the write footprint is condensed to ensure
// that it remains under its memory limit.
//
// If no response is provided (indicating an error), all writes from the batch
// are added directly to the write footprint to avoid leaking any intents when
// the transaction cleans up.
func (tp *txnPipeliner) updateWriteTracking(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) {
	// After adding new writes to the write footprint, check whether we need to
	// condense the set to stay below memory limits.
	defer tp.footprint.maybeCondense(ctx, tp.riGen, trackedWritesMaxSize.Get(&tp.st.SV))

	// If the request failed, add all intent writes directly to the write
	// footprint. This reduces the likelihood of dangling intents blocking
	// concurrent writers for extended periods of time. See #3346.
	if br == nil {
		// The transaction cannot continue in this epoch whether this is
		// a retryable error or not.
		ba.IntentSpanIterate(nil, tp.footprint.insert)
		return
	}

	// Similarly, if the transaction is now finalized, we don't need to
	// accurately update the write tracking.
	if br.Txn.Status.IsFinalized() {
		switch br.Txn.Status {
		case roachpb.ABORTED:
			// If the transaction is now ABORTED, add all intent writes from
			// the batch directly to the write footprint. We don't know which
			// of these succeeded.
			ba.IntentSpanIterate(nil, tp.footprint.insert)
		case roachpb.COMMITTED:
			// If the transaction is now COMMITTED, it must not have any more
			// in-flight writes, so clear them. Technically we should move all
			// of these to the write footprint, but since the transaction is
			// already committed, there's no reason to.
			tp.ifWrites.clear(
				/* reuse - we're not going to use this Btree again, so there's no point in
				   moving the nodes to a free list */
				false)
		default:
			panic("unexpected")
		}
		return
	}

	for i, ru := range ba.Requests {
		req := ru.GetInner()
		resp := br.Responses[i].GetInner()

		if qiReq, ok := req.(*roachpb.QueryIntentRequest); ok {
			// Remove any in-flight writes that were proven to exist.
			// It shouldn't be possible for a QueryIntentRequest with
			// the ErrorIfMissing option set to return without error
			// and with with FoundIntent=false, but we handle that
			// case here because it happens a lot in tests.
			if resp.(*roachpb.QueryIntentResponse).FoundIntent {
				tp.ifWrites.remove(qiReq.Key, qiReq.Txn.Sequence)
				// Move to write footprint.
				tp.footprint.insert(roachpb.Span{Key: qiReq.Key})
			}
		} else if roachpb.IsTransactionWrite(req) {
			// If the request was a transactional write, track its intents.
			if ba.AsyncConsensus {
				// Record any writes that were performed asynchronously. We'll
				// need to prove that these succeeded sometime before we commit.
				header := req.Header()
				tp.ifWrites.insert(header.Key, header.Sequence)
			} else {
				// If the writes weren't performed asynchronously then add them
				// directly to our write footprint.
				if sp, ok := roachpb.ActualSpan(req, resp); ok {
					tp.footprint.insert(sp)
				}
			}
		}
	}
}

// stripQueryIntents adjusts the BatchResponse to hide the fact that this
// interceptor added new requests to the batch. It returns an adjusted batch
// response without the responses that correspond to these added requests.
func (tp *txnPipeliner) stripQueryIntents(br *roachpb.BatchResponse) *roachpb.BatchResponse {
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
	ctx context.Context, ba roachpb.BatchRequest, pErr *roachpb.Error,
) *roachpb.Error {
	// Fix the error index to hide the impact of any QueryIntent requests.
	if pErr.Index != nil {
		before := int32(0)
		for _, ru := range ba.Requests[:int(pErr.Index.Index)] {
			req := ru.GetInner()
			if req.Method() == roachpb.QueryIntent {
				before++
			}
		}
		pErr.Index.Index -= before
	}

	// Turn an IntentMissingError into a transactional retry error.
	if ime, ok := pErr.GetDetail().(*roachpb.IntentMissingError); ok {
		log.VEventf(ctx, 2, "transforming intent missing error into retry: %v", ime)
		err := roachpb.NewTransactionRetryError(
			roachpb.RETRY_ASYNC_WRITE_FAILURE, fmt.Sprintf("missing intent on: %s", ime.Key))
		retryErr := roachpb.NewErrorWithTxn(err, pErr.GetTxn())
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
	if l := tp.ifWrites.len(); l > 0 {
		tis.InFlightWrites = make([]roachpb.SequencedWrite, 0, l)
		tp.ifWrites.ascend(func(w *inFlightWrite) {
			tis.InFlightWrites = append(tis.InFlightWrites, w.SequencedWrite)
		})
	}
}

// initializeLeaf loads the in-flight writes for a leaf transaction.
func (tp *txnPipeliner) initializeLeaf(tis *roachpb.LeafTxnInputState) {
	// Copy all in-flight writes into the inFlightWrite tree.
	for _, w := range tis.InFlightWrites {
		tp.ifWrites.insert(w.Key, w.Sequence)
	}
}

// populateLeafFinalState is part of the txnInterceptor interface.
func (tp *txnPipeliner) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (tp *txnPipeliner) importLeafFinalState(*roachpb.LeafTxnFinalState) {}

// epochBumpedLocked implements the txnReqInterceptor interface.
func (tp *txnPipeliner) epochBumpedLocked() {
	// Move all in-flight writes into the write footprint. These writes no
	// longer need to be tracked precisely, but we don't want to forget about
	// them and fail to clean them up.
	if tp.ifWrites.len() > 0 {
		tp.ifWrites.ascend(func(w *inFlightWrite) {
			tp.footprint.insert(roachpb.Span{Key: w.Key})
		})
		tp.footprint.mergeAndSort()
		tp.ifWrites.clear(true /* reuse */)
	}
}

// closeLocked implements the txnReqInterceptor interface.
func (tp *txnPipeliner) closeLocked() {}

// haveWrites returns whether the interceptor has observed any writes,
// including currently in-flight ones.
func (tp *txnPipeliner) haveWrites() bool {
	return tp.ifWrites.len() > 0 || !tp.footprint.empty()
}

// inFlightWrites represent a commitment to proving (via QueryIntent) that
// a point write succeeded in replicating an intent with a specific sequence
// number.
type inFlightWrite struct {
	roachpb.SequencedWrite
}

// Less implements the btree.Item interface.
func (a *inFlightWrite) Less(b btree.Item) bool {
	return a.Key.Compare(b.(*inFlightWrite).Key) < 0
}

// inFlightWriteSet is an ordered set of in-flight point writes. Given a set
// of n elements, the structure supports O(log n) insertion of new in-flight
// writes, O(log n) removal of existing in-flight writes, and O(m + log n)
// retrieval over m in-flight writes that overlap with a given key.
type inFlightWriteSet struct {
	t     *btree.BTree
	bytes int64

	// Avoids allocs.
	tmp1, tmp2 inFlightWrite
	alloc      inFlightWriteAlloc
}

// insert attempts to insert an in-flight write that has not been proven to have
// succeeded into the in-flight write set. If the write with an equal or larger
// sequence number already exists in the set, the method is a no-op.
func (s *inFlightWriteSet) insert(key roachpb.Key, seq enginepb.TxnSeq) {
	if s.t == nil {
		// Lazily initialize btree.
		s.t = btree.New(txnPipelinerBtreeDegree)
	}

	s.tmp1.Key = key
	item := s.t.Get(&s.tmp1)
	if item != nil {
		otherW := item.(*inFlightWrite)
		if seq > otherW.Sequence {
			// Existing in-flight write has old information.
			otherW.Sequence = seq
		}
		return
	}

	w := s.alloc.alloc(key, seq)
	s.t.ReplaceOrInsert(w)
	s.bytes += keySize(key)
}

// remove attempts to remove an in-flight write from the in-flight write set.
// The method will be a no-op if the write was already proved. Care is taken
// not to accidentally remove a write to the same key but at a later epoch or
// sequence number.
func (s *inFlightWriteSet) remove(key roachpb.Key, seq enginepb.TxnSeq) {
	if s.len() == 0 {
		// Set is empty.
		return
	}

	s.tmp1.Key = key
	item := s.t.Get(&s.tmp1)
	if item == nil {
		// The write was already proven or the txn epoch was incremented.
		return
	}

	w := item.(*inFlightWrite)
	if seq < w.Sequence {
		// The sequence might have changed, which means that a new write was
		// sent to the same key. This write would have been forced to prove
		// the existence of current write already.
		return
	}

	// Delete the write from the in-flight writes set.
	delItem := s.t.Delete(item)
	if delItem != nil {
		*delItem.(*inFlightWrite) = inFlightWrite{} // for GC
	}
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
	s.t.Ascend(func(i btree.Item) bool {
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
	if end == nil {
		// Point lookup.
		s.tmp1.Key = start
		if i := s.t.Get(&s.tmp1); i != nil {
			f(i.(*inFlightWrite))
		}
	} else {
		// Range lookup.
		s.tmp1.Key, s.tmp2.Key = start, end
		s.t.AscendRange(&s.tmp1, &s.tmp2, func(i btree.Item) bool {
			f(i.(*inFlightWrite))
			return true
		})
	}
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
// memory. The reuse flag indicates whether the caller is intending to reu-use
// the set or not.
func (s *inFlightWriteSet) clear(reuse bool) {
	if s.t == nil {
		return
	}
	s.t.Clear(reuse /* addNodesToFreelist */)
	s.bytes = 0
	s.alloc.clear()
}

// inFlightWriteAlloc provides chunk allocation of inFlightWrites,
// amortizing the overhead of each allocation.
type inFlightWriteAlloc []inFlightWrite

// alloc allocates a new inFlightWrite with the specified key and sequence
// number.
func (a *inFlightWriteAlloc) alloc(key roachpb.Key, seq enginepb.TxnSeq) *inFlightWrite {
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
	*w = inFlightWrite{
		SequencedWrite: roachpb.SequencedWrite{Key: key, Sequence: seq},
	}
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

// condensableSpanSet is a set of key spans that is condensable in order to
// stay below some maximum byte limit. Condensing of the set happens in two
// ways. Initially, overlapping spans are merged together to deduplicate
// redundant keys. If that alone isn't sufficient to stay below the byte limit,
// spans within the same Range will be merged together. This can cause the
// "footprint" of the set to grow, so the set should be thought of as on
// overestimate.
type condensableSpanSet struct {
	s     []roachpb.Span
	bytes int64
}

// insert adds a new span to the condensable span set. No attempt to condense
// the set or deduplicate the new span with existing spans is made.
func (s *condensableSpanSet) insert(sp roachpb.Span) {
	s.s = append(s.s, sp)
	s.bytes += spanSize(sp)
}

// mergeAndSort merges all overlapping spans. Calling this method will not
// increase the overall bounds of the span set, but will eliminate duplicated
// spans and combine overlapping spans.
//
// The method has the side effect of sorting the stable write set.
func (s *condensableSpanSet) mergeAndSort() (distinct bool) {
	s.s, distinct = roachpb.MergeSpans(s.s)
	if !distinct {
		// Recompute the size.
		s.bytes = 0
		for _, sp := range s.s {
			s.bytes += spanSize(sp)
		}
	}
	return distinct
}

// maybeCondense is similar in spirit to mergeAndSort, but it only adjusts the
// span set when the maximum byte limit is exceeded. However, when this limit is
// exceeded, the method is more aggressive in its attempt to reduce the memory
// footprint of the span set. Not only will it merge overlapping spans, but
// spans within the same range boundaries are also condensed.
func (s *condensableSpanSet) maybeCondense(
	ctx context.Context, riGen RangeIteratorGen, maxBytes int64,
) {
	if s.bytes < maxBytes {
		return
	}

	// Start by attempting to simply merge the spans within the set. This alone
	// may bring us under the byte limit. Even if it doesn't, this step has the
	// nice property that it sorts the spans by start key, which we rely on
	// lower in this method.
	s.mergeAndSort()
	if s.bytes < maxBytes {
		return
	}

	if riGen == nil {
		// If we were not given a RangeIteratorGen, we cannot condense the spans.
		return
	}
	ri := riGen()

	// Divide spans by range boundaries and condense. Iterate over spans
	// using a range iterator and add each to a bucket keyed by range
	// ID. Local keys are kept in a new slice and not added to buckets.
	type spanBucket struct {
		rangeID roachpb.RangeID
		bytes   int64
		spans   []roachpb.Span
	}
	var buckets []spanBucket
	var localSpans []roachpb.Span
	for _, sp := range s.s {
		if keys.IsLocal(sp.Key) {
			localSpans = append(localSpans, sp)
			continue
		}
		ri.Seek(ctx, roachpb.RKey(sp.Key), Ascending)
		if !ri.Valid() {
			// We haven't modified s.s yet, so it is safe to return.
			log.VEventf(ctx, 2, "failed to condense intent spans: %v", ri.Error())
			return
		}
		rangeID := ri.Desc().RangeID
		if l := len(buckets); l > 0 && buckets[l-1].rangeID == rangeID {
			buckets[l-1].spans = append(buckets[l-1].spans, sp)
		} else {
			buckets = append(buckets, spanBucket{
				rangeID: rangeID, spans: []roachpb.Span{sp},
			})
		}
		buckets[len(buckets)-1].bytes += spanSize(sp)
	}

	// Sort the buckets by size and collapse from largest to smallest
	// until total size of uncondensed spans no longer exceeds threshold.
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].bytes > buckets[j].bytes })
	s.s = localSpans // reset to hold just the local spans; will add newly condensed and remainder
	for _, bucket := range buckets {
		// Condense until we get to half the threshold.
		if s.bytes <= maxBytes/2 {
			// Collect remaining spans from each bucket into uncondensed slice.
			s.s = append(s.s, bucket.spans...)
			continue
		}
		s.bytes -= bucket.bytes
		// TODO(spencer): consider further optimizations here to create
		// more than one span out of a bucket to avoid overly broad span
		// combinations.
		cs := bucket.spans[0]
		for _, s := range bucket.spans[1:] {
			cs = cs.Combine(s)
			if !cs.Valid() {
				// If we didn't fatal here then we would need to ensure that the
				// spans were restored or a transaction could lose part of its
				// write footprint.
				log.Fatalf(ctx, "failed to condense intent spans: "+
					"combining span %s yielded invalid result", s)
			}
		}
		s.bytes += spanSize(cs)
		s.s = append(s.s, cs)
	}
}

// asSlice returns the set as a slice of spans.
func (s *condensableSpanSet) asSlice() []roachpb.Span {
	l := len(s.s)
	return s.s[:l:l] // immutable on append
}

// empty returns whether the set is empty or whether it contains spans.
func (s *condensableSpanSet) empty() bool {
	return len(s.s) == 0
}

func spanSize(sp roachpb.Span) int64 {
	return int64(len(sp.Key) + len(sp.EndKey))
}

func keySize(k roachpb.Key) int64 {
	return int64(len(k))
}
