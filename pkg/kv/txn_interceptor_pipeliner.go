// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/google/btree"
)

// The degree of the outstandingWrites btree.
const txnPipelinerBtreeDegree = 32

var pipelinedWritesEnabled = settings.RegisterBoolSetting(
	"kv.transaction.write_pipelining_enabled",
	"if enabled, transactional writes are pipelined through Raft consensus",
	true,
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

// txnPipeliner is a txnInterceptor that pipelines transactional writes by using
// asynchronous consensus. The interceptor then tracks all writes that have been
// asynchronously proposed through Raft and ensures that all interfering
// requests chain on to them by first proving that the async writes succeeded.
// The interceptor also ensures that when committing a transaction all writes
// that have been proposed but not proven to have succeeded are first checked
// before committing. These async writes are referred to as "outstanding writes"
// and this process of proving that an outstanding write succeeded is called
// "proving" the write.
//
// Chaining on to in-flight async writes is important for two main reasons to
// txnPipeliner:
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
// 2. transport layers beneath the txnPipeliner do not provide strong enough
//    ordering guarantees between concurrent requests in the same transaction to
//    avoid needing explicit chaining. For instance, DistSender uses unary gRPC
//    requests instead of gRPC streams, so it can't natively expose strong ordering
//    guarantees. Perhaps more importantly, even when a command has entered the
//    command queue and evaluated on a Replica, it is not guaranteed to be applied
//    before interfering commands. This is because the command may be retried
//    outside of the serialization of the command queue for any number of reasons,
//    such as leaseholder changes. When the command re-enters the command queue,
//    it's possible that interfering commands may jump ahead of it. To combat
//    this, the txnPipeliner uses chaining to throw an error when these
//    re-orderings would have affected the order that transactional requests
//    evaluate in.
//
// The interceptor proves all outstanding writes before committing a transaction
// by tacking on a QueryIntent request for each one to the front of an
// EndTransaction(Commit=true) requests. The result of this is that the
// EndTransaction needs to wait at the DistSender level for all of QueryIntent
// requests to succeed at before executing itself [1]. This is a little
// unfortunate because a transaction could have accumulated a large number of
// outstanding writes without proving any of them, and the more of these writes
// there are, the more chance querying one of them gets delayed and delays the
// overall transaction.
//
// Three approaches have been considered to address this, all of which revolve
// around the idea that earlier writes in a transaction may have finished
// consensus well before the EndTransaction is sent. Following this logic, it
// would be in the txnPipeliner's best interest to prove outstanding writes as
// early as possible, even if no other overlapping requests force them to be
// proven. The approaches are:
// 1. launch a background process after each successful async write to query its
//    intents and wait for it to succeed. This would effectively solve the issue,
//    but at the cost of many more goroutines and many more QueryIntent requests,
//    most of which would be redundant because their corresponding write wouldn't
//    complete until after an EndTransaction synchronously needed to prove them
//    anyway.
// 2. to address the issue of an unbounded number of background goroutines
//    proving writes in approach 1, a single background goroutine could be run
//    that repeatedly loops over all outstanding writes and attempts to prove
//    them. This approach was used in an early revision of #26599 and has the nice
//    property that only one batch of QueryIntent requests is ever active at a
//    given time. It may be revisited, but for now it is not used for the same
//    reason as approach 1: most of its QueryIntent requests will be useless
//    because a transaction will send an EndTransaction immediately after sending
//    all of its writes.
// 3. turn the KV interface into a streaming protocol (#8360) that could support
//    returning multiple results. This would allow clients to return immediately
//    after a writes "evaluation" phase completed but hold onto a handle to the
//    request and be notified immediately after its "replication" phase completes.
//    This would allow txnPipeliner to prove outstanding writes immediately after
//    they finish consensus without any extra RPCs.
// So far, none of these approaches have been integrated.
//
// [1] A proposal called "parallel commits" (#24194) exists that would allow all
//     QueryIntent requests and the EndTransaction request that they are prepended
//     to to be sent by the DistSender in parallel. This would help with this
//     issue by hiding the cost of the QueryIntent requests behind the cost of the
//     "staging" EndTransaction request.
//
type txnPipeliner struct {
	st       *cluster.Settings
	wrapped  lockedSender
	disabled bool

	outstandingWrites *btree.BTree
	owAlloc           outstandingWriteAlloc
	tmpOW1, tmpOW2    outstandingWrite // avoid allocs
}

// outstandingWrites represent a commitment to proving (via QueryIntent) that
// a point write succeeded in replicating an intent with a specific sequence
// number.
type outstandingWrite struct {
	roachpb.SequencedWrite
}

// Less implements the btree.Item interface.
func (a *outstandingWrite) Less(b btree.Item) bool {
	return a.Key.Compare(b.(*outstandingWrite).Key) < 0
}

// SendLocked implements the lockedSender interface.
func (tp *txnPipeliner) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Fast-path for 1PC transactions.
	_, hasBT := ba.GetArg(roachpb.BeginTransaction)
	_, hasET := ba.GetArg(roachpb.EndTransaction)
	if hasBT && hasET {
		return tp.wrapped.SendLocked(ctx, ba)
	}

	// Adjust the batch so that it doesn't miss any outstanding writes.
	ba = tp.chainToOutstandingWrites(ba)

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	br, pErr := tp.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, tp.adjustError(ctx, ba, pErr)
	}

	// TODO(nvanbenschoten): It's currently possible for this response to be
	// from an earlier epoch when txns are used concurrently. That's ok for now
	// because we always manually restart transactions once all concurrent
	// operations synchronize. Once we move away from that model to a txnAttempt
	// model, we'll need to reconsider how this works. It ~should~ just work.

	// Prove any outstanding writes that we proved to exist.
	br = tp.updateOutstandingWrites(ctx, ba, br)
	return br, nil
}

// chainToOutstandingWrites ensures that we "chain" on to any outstanding writes
// that overlap the keys we're trying to read/write. We do this by prepending
// QueryIntent requests with the THROW_ERROR behavior before each request that
// touches any of the outstanding writes. In effect, this allows us to prove
// that a write succeeded before depending on its existence. We later prune down
// the list of writes we proved to exist that are no longer "outstanding" in
// updateOutstandingWrites.
//
// TODO(nvanbenschoten): Consider placing an upper bound on the size of the
// outstandingWrites tree. Once this limit is hit, we'll either need to
// proactively prove outstanding writes or stop pipelining new writes.
func (tp *txnPipeliner) chainToOutstandingWrites(ba roachpb.BatchRequest) roachpb.BatchRequest {
	asyncConsensus := tp.st.Version.IsActive(cluster.VersionAsyncConsensus) &&
		pipelinedWritesEnabled.Get(&tp.st.SV) && !tp.disabled

	// We provide a setting to bound the number of writes we permit in a batch
	// that uses async consensus. This is useful because we'll have to prove
	// each write that uses async consensus using a QueryIntent, so there's a
	// point where it makes more sense to just perform consensus for the entire
	// batch synchronously and avoid all of the overhead of pipelining.
	if maxBatch := pipelinedWritesMaxBatchSize.Get(&tp.st.SV); maxBatch > 0 {
		batchSize := int64(len(ba.Requests))
		if _, hasBT := ba.GetArg(roachpb.BeginTransaction); hasBT {
			batchSize--
		}
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
		if !asyncConsensus && !forked && tp.outstandingWritesLen() == len(chainedKeys) {
			// If there are no outstanding writes or all outstanding writes
			// have been chained onto and async consensus is disallowed,
			// short-circuit immediately.
			break
		}

		req := ru.GetInner()
		if req.Method() == roachpb.BeginTransaction {
			// Ignore BeginTransaction requests. They'll always be the first
			// request in a batch and will never need to chain on any existing
			// writes.
			continue
		}
		if !roachpb.IsTransactionWrite(req) || roachpb.IsRange(req) {
			// Only allow batches consisting of solely transactional point
			// writes to perform consensus asynchronously.
			// TODO(nvanbenschoten): We could allow batches with reads and point
			// writes to perform async consensus, but this would be a bit
			// tricky. Any read would need to chain on to any write that came
			// before it in the batch and overlaps. For now, it doesn't seem
			// worth it.
			asyncConsensus = false
		}

		if tp.outstandingWritesLen() > len(chainedKeys) {
			// For each conflicting outstanding write, add a QueryIntent request
			// to the batch to assert that it has succeeded and "chain" onto it.
			itemIter := func(item btree.Item) bool {
				// We don't want to modify the batch's request slice directly,
				// so fork it before modifying it.
				if !forked {
					ba.Requests = append([]roachpb.RequestUnion(nil), ba.Requests[:i]...)
					forked = true
				}

				w := item.(*outstandingWrite)
				if _, ok := chainedKeys[string(w.Key)]; !ok {
					// The write has not already been chained onto by an earlier
					// request in this batch. Add a QueryIntent request to the
					// batch (before the conflicting request) to ensure that we
					// chain on to the success of the outstanding write.
					meta := ba.Txn.TxnMeta
					meta.Sequence = w.Sequence
					ba.Add(&roachpb.QueryIntentRequest{
						RequestHeader: roachpb.RequestHeader{
							Key: w.Key,
						},
						Txn: meta,
						// Set the IfMissing behavior to return an error if the
						// outstanding write is missing.
						IfMissing: roachpb.QueryIntentRequest_RETURN_ERROR,
					})

					// Record that the key has been chained onto at least once
					// in this batch so that we don't chain onto it again.
					if chainedKeys == nil {
						chainedKeys = make(map[string]struct{})
					}
					chainedKeys[string(w.Key)] = struct{}{}
				}
				return true
			}

			if !roachpb.IsTransactional(req) {
				// Non-transactional requests require that we stall the entire
				// pipeline by chaining on to all outstanding writes. This is
				// because their request header is often insufficient to
				// determine all of the keys that they will interact with.
				tp.outstandingWrites.Ascend(itemIter)
			} else if et, ok := req.(*roachpb.EndTransactionRequest); ok {
				if et.Commit {
					// EndTransactions need to prove all outstanding writes before
					// being allowed to succeed themselves.
					tp.outstandingWrites.Ascend(itemIter)
				}
			} else {
				// Transactional reads and writes needs to chain on to any
				// overlapping outstanding writes.
				r := req.Header().Span().AsRange()
				tp.tmpOW1.Key, tp.tmpOW2.Key = roachpb.Key(r.Start), roachpb.Key(r.End)
				tp.outstandingWrites.AscendRange(&tp.tmpOW1, &tp.tmpOW2, itemIter)
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

// updateOutstandingWrites reads the response for the given request and uses
// it to update the tracked outstanding write set. It does so by performing
// two actions:
// 1. it removes all outstanding writes that the request proved to exist from
//    the outstanding writes set.
// 2. it adds all async writes the the request performed to the outstanding
//    write set.
//
// While doing so, the method also strips all QueryIntent responses from the
// BatchResponse, hiding the fact that they were added in the first place.
func (tp *txnPipeliner) updateOutstandingWrites(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) *roachpb.BatchResponse {
	// If the transaction is no longer pending, clear the outstanding writes
	// tree. This will turn maybeRemoveProvenWriteLocked into a quick no-op.
	if br.Txn != nil && br.Txn.Status != roachpb.PENDING && tp.outstandingWrites != nil {
		tp.outstandingWrites.Clear(false /* addNodesToFreelist */)
	}

	j := 0
	for i, ru := range ba.Requests {
		req := ru.GetInner()
		resp := br.Responses[i].GetInner()

		if qiReq, ok := req.(*roachpb.QueryIntentRequest); ok {
			// Remove any outstanding writes that were proven to exist.
			// It shouldn't be possible for a QueryIntentRequest with
			// an IfMissing behavior of RETURN_ERROR to return without
			// error and with with FoundIntent=false, but we handle that
			// case here because it happens a lot in tests.
			if resp.(*roachpb.QueryIntentResponse).FoundIntent {
				tp.maybeRemoveProvenWriteLocked(qiReq.Key, qiReq.Txn.Sequence)
			}
		} else {
			// Hide the fact that this interceptor added new requests to the batch.
			br.Responses[j] = br.Responses[i]
			j++

			// Record any writes that were performed asynchronously. We'll
			// need to prove that these succeeded sometime before we commit.
			if ba.AsyncConsensus && req.Method() != roachpb.BeginTransaction {
				header := req.Header()
				tp.maybeInsertOutstandingWriteLocked(header.Key, header.Sequence)
			}
		}
	}
	// Hide the fact that this interceptor added new requests to the batch.
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
		err := roachpb.NewTransactionRetryError(roachpb.RETRY_ASYNC_WRITE_FAILURE)
		retryErr := roachpb.NewErrorWithTxn(err, pErr.GetTxn())
		retryErr.Index = pErr.Index
		return retryErr
	}
	return pErr
}

// setWrapped implements the txnInterceptor interface.
func (tp *txnPipeliner) setWrapped(wrapped lockedSender) { tp.wrapped = wrapped }

// populateMetaLocked implements the txnReqInterceptor interface.
func (tp *txnPipeliner) populateMetaLocked(meta *roachpb.TxnCoordMeta) {
	if l := tp.outstandingWritesLen(); l > 0 {
		meta.OutstandingWrites = make([]roachpb.SequencedWrite, 0, l)
		tp.outstandingWrites.Ascend(func(item btree.Item) bool {
			w := item.(*outstandingWrite)
			meta.OutstandingWrites = append(meta.OutstandingWrites, w.SequencedWrite)
			return true
		})
	}
}

// augmentMetaLocked implements the txnReqInterceptor interface.
func (tp *txnPipeliner) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
	// Copy all outstanding writes into the outstandingWrite tree.
	for _, w := range meta.OutstandingWrites {
		tp.maybeInsertOutstandingWriteLocked(w.Key, w.Sequence)
	}
}

// epochBumpedLocked implements the txnReqInterceptor interface.
func (tp *txnPipeliner) epochBumpedLocked() {
	// Clear out the outstandingWrites set and free associated memory.
	if tp.outstandingWrites != nil {
		// Add nodes to freelist so that next epoch can reuse btree memory.
		tp.outstandingWrites.Clear(true /* addNodesToFreelist */)
		tp.owAlloc.Clear()
	}
}

// closeLocked implements the txnReqInterceptor interface.
func (tp *txnPipeliner) closeLocked() {}

// outstandingWritesLen returns the number of writes that are outstanding.
func (tp *txnPipeliner) outstandingWritesLen() int {
	if tp.outstandingWrites == nil {
		return 0
	}
	return tp.outstandingWrites.Len()
}

// maybeInsertOutstandingWriteLocked attempts to insert an outstanding write
// that has not been proven to have succeeded into the txnPipeliners outstanding
// write map.
func (tp *txnPipeliner) maybeInsertOutstandingWriteLocked(key roachpb.Key, seq int32) {
	if tp.outstandingWrites == nil {
		// Lazily initialize btree.
		tp.outstandingWrites = btree.New(txnPipelinerBtreeDegree)
	}

	tp.tmpOW1.Key = key
	item := tp.outstandingWrites.Get(&tp.tmpOW1)
	if item != nil {
		otherW := item.(*outstandingWrite)
		if seq > otherW.Sequence {
			// Existing outstanding write has old information.
			otherW.Sequence = seq
		}
		return
	}

	w := tp.owAlloc.Alloc(key, seq)
	tp.outstandingWrites.ReplaceOrInsert(w)
}

// maybeRemoveProvenWriteLocked attempts to remove an outstanding write that
// was proven to have succeeded. The method will be a no-op if the write was
// already proved. Care is taken not to accidentally remove a write to the
// same key but at a later epoch or sequence number.
func (tp *txnPipeliner) maybeRemoveProvenWriteLocked(key roachpb.Key, seq int32) {
	tp.tmpOW1.Key = key
	item := tp.outstandingWrites.Get(&tp.tmpOW1)
	if item == nil {
		// The write was already proven or the txn epoch was incremented.
		return
	}

	w := item.(*outstandingWrite)
	if seq < w.Sequence {
		// The sequence might have changed, which means that a new write was
		// sent to the same key. This write would have been forced to prove
		// the existence of current write already.
		return
	}

	// Delete the write from the outstanding writes set.
	delItem := tp.outstandingWrites.Delete(item)
	if delItem != nil {
		*delItem.(*outstandingWrite) = outstandingWrite{} // for GC
	}
}

// outstandingWriteAlloc provides chunk allocation of outstandingWrites,
// amortizing the overhead of each allocation.
type outstandingWriteAlloc []outstandingWrite

// Alloc allocates a new outstandingWrite with the specified key and sequence
// number.
func (a *outstandingWriteAlloc) Alloc(key roachpb.Key, seq int32) *outstandingWrite {
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
		*a = make([]outstandingWrite, 0, allocSize)
	}

	*a = (*a)[:len(*a)+1]
	w := &(*a)[len(*a)-1]
	*w = outstandingWrite{
		SequencedWrite: roachpb.SequencedWrite{Key: key, Sequence: seq},
	}
	return w
}

// Clear removes all allocated outstanding writes and attempts to reclaim as
// much allocated memory as possible.
func (a *outstandingWriteAlloc) Clear() {
	for i := range *a {
		(*a)[i] = outstandingWrite{} // for GC
	}
	*a = (*a)[:0]
}
