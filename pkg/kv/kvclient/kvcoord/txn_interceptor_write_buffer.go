// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"encoding/binary"
	"slices"
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/mvccencoding"
	"github.com/cockroachdb/cockroach/pkg/storage/mvcceval"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
)

// BufferedWritesEnabled is used to enable write buffering.
var BufferedWritesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.write_buffering.enabled",
	"if enabled, transactional writes are buffered on the client",
	metamorphic.ConstantWithTestBool("kv.transactional.write_buffering.enabled", false /* defaultValue */),
	settings.WithPublic,
)

var bufferedWritesMaxBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"kv.transaction.write_buffering.max_buffer_size",
	"if non-zero, defines that maximum size of the "+
		"buffer that will be used to buffer transactional writes per-transaction",
	1<<22, // 4MB
	settings.NonNegativeInt,
	settings.WithPublic,
)

// txnWriteBuffer is a txnInterceptor that buffers transactional writes until
// commit time. Moreover, it also decomposes read-write KV operations (e.g.
// CPuts) into separate (locking) read and write operations, buffering the
// latter until commit time.
//
// Buffering writes until commit time has four main benefits:
//
// 1. It allows for more batching of writes, which can be more efficient.
// Instead of sending write batches one at a time, we can batch all write
// batches and send them in a single batch at commit time. This is a win even if
// writes would otherwise be pipelined through raft.
//
// 2. It allows for the elimination of redundant writes. If a client writes to
// the same key multiple times in a transaction, only the last write needs to be
// written to the KV layer.
//
// 3. It allows the client to serve read-your-own-writes locally, which can be
// much faster and cheaper than sending them to the leaseholder. This is
// especially true when the leaseholder isn't colocated with the client.
//
// By serving read-your-own-writes locally from the gateway, write buffering
// also avoids the problem of pipeline stalls that can occur when a client reads
// a pipelined write before it has finished replicating through raft. For
// details on pipeline stalls, see txnPipeliner.
//
// 4. It allows clients to passively hit the 1-phase commit fast path, instead
// of requiring clients to carefully construct "auto-commit" BatchRequests to
// make use of the optimization. By buffering writes on the client before
// commit, we avoid immediately disabling the fast path when the client issues
// their first write. Then, at commit time, we flush the buffer and will happen
// to hit the 1-phase commit fast path if all writes end up going to the same
// range.
//
// However, buffering writes comes with some challenges.
//
// The first challenge is that read-only requests need to be aware of any
// buffered writes, as they may need to serve some reads from the buffer
// instead of the KV layer (read-your-own-writes).
//
// Similarly, any read-write requests, such as CPuts, that we decompose into
// separate read and write operations, need to be aware of any buffered writes
// that may affect their read half. The read portion must be served from the
// buffer instead of the KV-layer if the key has already been written to
// previously. However, we aren't guaranteed to have acquired a corresponding
// lock on the key if a buffered write exists for the key -- as such, we must
// still send a locking read request to the KV layer to acquire a lock.
//
// The picture is further complicated when distributed execution is introduced
// into the mix. A read request that is distributed by the client also needs to
// be aware of the write buffer. As such, when constructing a leaf transaction
// to serve a distributed execution read, we must also ship[1] the write buffer
// along.
//
// The second challenge is around the handling of savepoints. In particular,
// when a savepoint is rolled back, we must clear out any writes that happened
// after the (now) rolled back savepoint. This means that if a key is written to
// multiple times in a transaction, we must retain all writes to the key. Note
// that this is only required when the transaction is in progress -- once the
// transaction is ready to commit, only the last value written to the key needs
// to be flushed to the KV layer.
//
// The third challenge is around memory limits and preventing OOMs. As writes
// are buffered in-memory, per-transaction, we need to be careful not to OOM
// nodes by buffering too many writes. To that end, a per-transaction memory
// limit on the write buffer must be enforced. If this limit is exceeded, no
// more writes are buffered, and the buffer (either in its entirety or
// partially[2]) must be force flushed. Force flushing entails sending all
// buffered writes to the KV layer prematurely (i.e. before commit time).
//
// [1] Instead of shipping the entire write buffer, we can constrain this to
// just the portion that overlaps with the span of the read request that's being
// distributed for evaluation by the client.
//
// [2] The decision to flush the buffer in its entirety vs. partially is a
// tradeoff. Flushing the entire buffer is simpler and frees up more memory.
// Flushing the buffer partially preserves some (all but the fourth) of the
// listed benefits of buffering writes for the unflushed portion of the buffer.
//
// TODO(arul): In various places below, there's potential to optimize things by
// batch allocating misc objects and pre-allocating some slices.
type txnWriteBuffer struct {
	st *cluster.Settings
	// enabled indicates whether write buffering is currently enabled for the
	// transaction or not. Write buffering may only be enabled on RootTxns, and
	// before the transaction has sent any requests. However, a transaction that
	// has previously buffered writes may flush its buffer and disable write
	// buffering for subsequent requests. This can happen for a few different
	// reasons:
	//
	// 1. If the buffer has exceeded its configured budget, or if the transaction
	// issues a DeleteRange request, we flush the buffer and disable write
	// buffering going forward. In either case, we're dealing with large writing
	// transactions, and there isn't much benefit from write buffering.
	// 2. If the transaction is performing a DDL operation, we flush the buffer
	// and disable write buffering going forward out of an abundance of caution.
	// This is opted into by SQL.
	//
	// As a result, we have a nice invariant: if write buffering is enabled, then
	// all writes performed by the transaction are buffered in memory. We can
	// never have the case where a part of the write set is buffered, and the
	// other part is replicated.
	//
	// In the future, the invariant above allows us to omit checking the AbortSpan
	// for transactions that have buffered writes enabled. The AbortSpan is used
	// to ensure we don't violate read-your-own-write semantics for transactions
	// that have been aborted by a conflicting transaction. As read-your-own-write
	// semantics are upheld by the client, not the server, for transactions that
	// use buffered writes, we can skip the AbortSpan check on the server.
	//
	// We currently track this via two state variables: `enabled` and `flushed`.
	// Writes are only buffered if enabled && !flushed.
	//
	// `enabled` tracks whether buffering has been enabled/disabled externally via
	// txn.SetBufferedWritesEnabled or because we are operating on a leaf
	// transaction.
	enabled bool
	//
	// `flushed` tracks whether the buffer has been previously flushed.
	flushed bool

	// flushOnNextBatch, if set, indicates that write buffering has just been
	// disabled, and the interceptor should flush any buffered writes when it
	// sees the next BatchRequest.
	flushOnNextBatch bool

	buffer        btree
	bufferIDAlloc uint64
	bufferSize    int64

	bufferSeek bufferedWrite // re-use while seeking

	wrapped    lockedSender
	txnMetrics *TxnMetrics

	// testingOverrideCPutEvalFn is used to mock the evaluation function for
	// conditional puts. Intended only for tests.
	testingOverrideCPutEvalFn func(expBytes []byte, actVal *roachpb.Value, actValPresent bool, allowNoExisting bool) *kvpb.ConditionFailedError
}

func (twb *txnWriteBuffer) setEnabled(enabled bool) {
	if !enabled && twb.buffer.Len() > 0 {
		// When disabling write buffering, if we evaluated any requests, we need
		// to ensure to flush the buffer.
		twb.flushOnNextBatch = true
	}
	if enabled {
		twb.txnMetrics.TxnWriteBufferEnabled.Inc(1)
	}
	twb.enabled = enabled
}

func (twb *txnWriteBuffer) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (_ *kvpb.BatchResponse, pErr *kvpb.Error) {
	if twb.flushOnNextBatch {
		twb.flushOnNextBatch = false
		return twb.flushBufferAndSendBatch(ctx, ba)
	}

	if !twb.shouldBuffer() {
		return twb.wrapped.SendLocked(ctx, ba)
	}

	if etArg, ok := ba.GetArg(kvpb.EndTxn); ok {
		if !etArg.(*kvpb.EndTxnRequest).Commit {
			// We're performing a rollback, so there is no point in flushing
			// anything.
			return twb.wrapped.SendLocked(ctx, ba)
		}
		return twb.flushBufferAndSendBatch(ctx, ba)
	}

	if twb.batchRequiresFlush(ctx, ba) {
		return twb.flushBufferAndSendBatch(ctx, ba)
	}

	// Check if buffering writes from the supplied batch will run us over
	// budget. If it will, we shouldn't buffer writes from the current batch,
	// and flush the buffer.
	maxSize := bufferedWritesMaxBufferSize.Get(&twb.st.SV)
	bufSize := twb.estimateSize(ba) + twb.bufferSize
	// NB: if bufferedWritesMaxBufferSize is set to 0 then we effectively disable
	// any buffer limiting.
	if maxSize != 0 && bufSize > maxSize {
		twb.txnMetrics.TxnWriteBufferMemoryLimitExceeded.Inc(1)
		log.VEventf(ctx, 2, "flushing buffer because buffer size (%s) exceeds max size (%s)",
			humanizeutil.IBytes(bufSize),
			humanizeutil.IBytes(maxSize))
		return twb.flushBufferAndSendBatch(ctx, ba)
	}

	if err := twb.validateBatch(ba); err != nil {
		// We could choose to twb.flushBufferAndSendBatch
		// here. For now, we return an error.
		return nil, kvpb.NewError(err)
	}

	transformedBa, rr, pErr := twb.applyTransformations(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	if len(transformedBa.Requests) == 0 {
		// Lower layers (the DistSender and the KVServer) do not expect/handle empty
		// batches. If all requests in the batch can be handled locally, and we're
		// left with an empty batch after applying transformations, eschew sending
		// anything to KV.
		br := ba.CreateReply()
		for i, record := range rr {
			br.Responses[i], pErr = record.toResp(ctx, twb, kvpb.ResponseUnion{}, ba.Txn)
			if pErr != nil {
				return nil, pErr
			}
		}
		twb.txnMetrics.TxnWriteBufferFullyHandledBatches.Inc(1)
		return br, nil
	}

	br, pErr := twb.wrapped.SendLocked(ctx, transformedBa)
	if pErr != nil {
		return nil, twb.adjustError(ctx, rr, pErr)
	}

	return twb.mergeResponseWithRequestRecords(ctx, rr, br)
}

func (twb *txnWriteBuffer) batchRequiresFlush(ctx context.Context, ba *kvpb.BatchRequest) bool {
	for _, ru := range ba.Requests {
		req := ru.GetInner()
		switch req.(type) {
		case *kvpb.IncrementRequest:
			// We don't typically see IncrementRequest in transactional batches that
			// haven't already had write buffering disabled becuase of DDL statements.
			//
			// However, we do have at least a few users of the NewTransactionalGenerator
			// in test code and builtins.
			//
			// We could handle this similar to how we handle ConditionalPut, but its
			// not clear there is much value in that.
			log.VEventf(ctx, 2, "%s forcing flush of write buffer", req.Method())
			return true
		case *kvpb.DeleteRangeRequest:
			// DeleteRange requests can delete an arbitrary number of keys over a
			// given keyspan. We won't know the exact scope of the delete until
			// we've scanned the keyspan, which must happen on the server. We've got
			// a couple of options here:
			//
			// 1. We decompose the DeleteRange request into a (potentially
			//    locking) Scan followed by buffered point Deletes for each
			//    key in the scan's result.
			//
			// 2. We flush the buffer[1] and send the DeleteRange request to
			//    the KV layer.
			//
			// We choose option 2, as typically the number of keys deleted is large,
			// and we may realize we're over budget after performing the initial
			// scan of the keyspan. At that point, we'll have to flush the buffer
			// anyway. Moreover, buffered writes are most impactful when a
			// transaction is writing to a small number of keys. As such, it's fine
			// to not optimize the DeleteRange case, as typically it results in a
			// large writing transaction.
			//
			// [1] Technically, we only need to flush the overlapping portion of the
			// buffer. However, for simplicity, the txnWriteBuffer doesn't support
			// transactions with partially buffered writes and partially flushed
			// writes. We could change this in the future if there's benefit to
			// doing so.

			log.VEventf(ctx, 2, "%s forcing flush of write buffer", req.Method())
			return true
		}
	}
	return false
}

// validateBatch returns an error if the batch is unsupported
// by the txnWriteBuffer.
func (twb *txnWriteBuffer) validateBatch(ba *kvpb.BatchRequest) error {
	if ba.WriteOptions != nil {
		// OriginTimestamp and OriginID are currently only used by Logical Data
		// Replication (LDR). These options are unsupported at the moment as we
		// don't store the inbound batch options in the buffer.
		if ba.WriteOptions.OriginTimestamp.IsSet() {
			return errors.AssertionFailedf("transaction write buffer does not support batches with OriginTimestamp set")
		}
		if ba.WriteOptions.OriginID != 0 {
			return errors.AssertionFailedf("transaction write buffer does not support batches with OriginID set")
		}
	}
	return twb.validateRequests(ba)
}

// validateRequests returns an error if any of the requests in the batch
// are unsupported by the txnWriteBuffer.
func (twb *txnWriteBuffer) validateRequests(ba *kvpb.BatchRequest) error {
	for _, ru := range ba.Requests {
		req := ru.GetInner()
		switch t := req.(type) {
		case *kvpb.ConditionalPutRequest:
			// Our client side ConditionalPutRequest evaluation does not know how to
			// handle the origin timestamp setting. Doing so would require sending a
			// GetRequest with RawMVCCValues set and parsing the MVCCValueHeader.
			if t.OriginTimestamp.IsSet() {
				return unsupportedOptionError(t.Method(), "OriginTimestamp")
			}
		case *kvpb.PutRequest:
		case *kvpb.DeleteRequest:
		case *kvpb.GetRequest:
			// ReturnRawMVCCValues is unsupported because we don't know how to serve
			// such reads from the write buffer currently.
			if t.ReturnRawMVCCValues {
				return unsupportedOptionError(t.Method(), "ReturnRawMVCCValues")
			}
		case *kvpb.ScanRequest:
			// ReturnRawMVCCValues is unsupported because we don't know how to serve
			// such reads from the write buffer currently.
			if t.ReturnRawMVCCValues {
				return unsupportedOptionError(t.Method(), "ReturnRawMVCCValues")
			}
			if t.ScanFormat == kvpb.COL_BATCH_RESPONSE {
				return unsupportedOptionError(t.Method(), "COL_BATCH_RESPONSE scan format")
			}
		case *kvpb.ReverseScanRequest:
			// ReturnRawMVCCValues is unsupported because we don't know how to serve
			// such reads from the write buffer currently.
			if t.ReturnRawMVCCValues {
				return unsupportedOptionError(t.Method(), "ReturnRawMVCCValues")
			}
			if t.ScanFormat == kvpb.COL_BATCH_RESPONSE {
				return unsupportedOptionError(t.Method(), "COL_BATCH_RESPONSE scan format")
			}
		case *kvpb.QueryLocksRequest, *kvpb.LeaseInfoRequest:
		default:
			// All other requests are unsupported. Note that we assume that requests
			// that should result in a buffer flush are handled explicitly before this
			// method was called.
			return unsupportedMethodError(t.Method())
		}
	}
	return nil
}

func unsupportedMethodError(m kvpb.Method) error {
	return errors.AssertionFailedf("transaction write buffer does not support %s requests", m)
}

func unsupportedOptionError(m kvpb.Method, option string) error {
	return errors.AssertionFailedf("transaction write buffer does not support %s requests with %s", m, option)
}

// estimateSize returns a conservative estimate by which the buffer will grow in
// size if the writes from the supplied batch request are buffered.
func (twb *txnWriteBuffer) estimateSize(ba *kvpb.BatchRequest) int64 {
	var scratch bufferedWrite
	estimate := int64(0)
	scratch.vals = make([]bufferedValue, 1)
	for _, ru := range ba.Requests {
		req := ru.GetInner()
		switch t := req.(type) {
		case *kvpb.ConditionalPutRequest:
			// At this point, we don't know whether the condition will evaluate
			// successfully or not, and by extension, whether the KV will be added to
			// the buffer. We therefore assume the worst case scenario (where the KV
			// is added to the buffer) in our estimate.
			scratch.key = t.Key
			scratch.vals[0] = bufferedValue{
				val: t.Value,
				seq: t.Sequence,
			}
			estimate += scratch.size()
		case *kvpb.PutRequest:
			// NB: when estimating, we're being conservative by assuming the Put is to
			// a key that isn't already present in the buffer. If it were, we could
			// omit the key's size from the estimate.
			scratch.key = t.Key
			scratch.vals[0] = bufferedValue{
				val: t.Value,
				seq: t.Sequence,
			}
			estimate += scratch.size()
		case *kvpb.DeleteRequest:
			// NB: Similar to Put, we're assuming we're deleting a key that isn't
			// already present in the buffer.
			scratch.key = t.Key
			scratch.vals[0] = bufferedValue{
				seq: t.Sequence,
			}
			estimate += scratch.size()
		}
		// No other request is buffered.
	}
	return estimate
}

// adjustError adjusts the provided error based on the transformations made by
// the txnWriteBuffer to the batch request before sending it to KV.
func (twb *txnWriteBuffer) adjustError(
	ctx context.Context, rr requestRecords, pErr *kvpb.Error,
) *kvpb.Error {
	// Fix the error index to hide the impact of any requests that were
	// transformed.
	if pErr.Index != nil {
		// We essentially want to find the number of stripped batch requests that
		// came before the request that caused the error in the original batch, and
		// therefore weren't sent to the KV layer. We can then adjust the error
		// index accordingly.
		numStripped := int32(0)
		baIdx := int32(0)
		// Note that all requests in the batch are guaranteed to be in the list of
		// request records.
		for _, record := range rr {
			if record.stripped {
				numStripped++
			} else {
				// If this is a transformed request (for example a LockingGet that was
				// sent instead of a Del), the error might be a bit confusing to the
				// client since the request that had an error isn't exactly the request
				// the user sent.
				//
				// For now, we handle this by logging and removing the error index.
				//
				// For requests that were not transformed, attributing an error to them
				// shouldn't confuse the client.
				if baIdx == pErr.Index.Index && record.transformed {
					log.Warningf(ctx, "error index %d is part of a transformed request", pErr.Index.Index)
					pErr.Index = nil
					return pErr
				} else if baIdx == pErr.Index.Index {
					break
				}
				baIdx++
			}
		}

		pErr.Index.Index += numStripped
	}

	return pErr
}

// adjustErrorUponFlush adjusts the provided error based on the number of
// pre-fixed writes due to flushing the buffer.
func (twb *txnWriteBuffer) adjustErrorUponFlush(
	ctx context.Context, numBuffered int, pErr *kvpb.Error,
) *kvpb.Error {
	if pErr.Index != nil {
		if pErr.Index.Index < int32(numBuffered) {
			// If the error belongs to a request because part of the buffer flush, nil
			// out the index.
			log.Warningf(ctx, "error index %d is part of the buffer flush", pErr.Index.Index)
			pErr.Index = nil
		} else {
			// Otherwise, adjust the error index to hide the impact of any flushed
			// write requests.
			pErr.Index.Index -= int32(numBuffered)
		}
	}
	return pErr
}

// setWrapped implements the txnInterceptor interface.
func (twb *txnWriteBuffer) setWrapped(wrapped lockedSender) {
	twb.wrapped = wrapped
}

// populateLeafInputState is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) populateLeafInputState(tis *roachpb.LeafTxnInputState) {
	// Note that we don't short-circuit this method if twb.enabled is false in
	// case write buffering was just disabled, yet we haven't flushed the
	// buffer.
	//
	// At the time of writing, this could only happen when we encountered a DDL
	// stmt which shouldn't need the LeafTxn support, but we choose to lean on
	// the safe side and simply ignore twb.enabled boolean.
	if twb.buffer.Len() == 0 {
		return
	}
	tis.BufferedWrites = make([]roachpb.BufferedWrite, 0, twb.buffer.Len())
	it := twb.buffer.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		bw := it.Cur()
		// TODO(yuzefovich): optimize allocation of vals slices.
		vals := make([]roachpb.BufferedWrite_Val, 0, len(bw.vals))
		for _, v := range bw.vals {
			vals = append(vals, roachpb.BufferedWrite_Val{
				Val: v.val,
				Seq: v.seq,
			})
		}
		tis.BufferedWrites = append(tis.BufferedWrites, roachpb.BufferedWrite{
			ID:   bw.id,
			Key:  bw.key,
			Vals: vals,
		})
	}
}

// initializeLeaf is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) initializeLeaf(tis *roachpb.LeafTxnInputState) {
	if len(tis.BufferedWrites) == 0 {
		// Regardless of whether the buffered writes are enabled on the root,
		// there are no actual buffered writes, so we can disable the
		// interceptor.
		twb.enabled = false
		return
	}
	// We have some buffered writes, so they must be enabled on the root.
	twb.enabled = true
	for _, bw := range tis.BufferedWrites {
		// TODO(yuzefovich): optimize allocation of vals slices.
		vals := make([]bufferedValue, 0, len(bw.Vals))
		for _, bv := range bw.Vals {
			vals = append(vals, bufferedValue{
				val: bv.Val,
				seq: bv.Seq,
			})
		}
		twb.buffer.Set(&bufferedWrite{
			id:   bw.ID,
			key:  bw.Key,
			vals: vals,
		})
	}
	// Note that we'll leave bufferIDAlloc field unchanged since we can't
	// perform any writes on the leaf, meaning that we won't add any new
	// buffered writes to the btree.
}

// populateLeafFinalState is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) epochBumpedLocked() {
	twb.resetBuffer()
}

func (twb *txnWriteBuffer) resetBuffer() {
	twb.buffer.Reset()
	twb.bufferSize = 0
}

// createSavepointLocked is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) rollbackToSavepointLocked(ctx context.Context, s savepoint) {
	toDelete := make([]*bufferedWrite, 0)
	it := twb.buffer.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		bufferedVals := it.Cur().vals
		// NB: the savepoint is being rolled back to s.seqNum (inclusive). So,
		// idx is the index of the first value that is considered rolled back.
		idx := sort.Search(len(bufferedVals), func(i int) bool {
			return bufferedVals[i].seq >= s.seqNum
		})
		if idx == len(bufferedVals) {
			// No writes are being rolled back.
			continue
		}
		// Update size bookkeeping for the values we're rolling back.
		for i := idx; i < len(bufferedVals); i++ {
			twb.bufferSize -= bufferedVals[i].size()
		}
		// Rollback writes by truncating the buffered values.
		it.Cur().vals = bufferedVals[:idx]
		if len(it.Cur().vals) == 0 {
			// All writes have been rolled back; we should remove this key from
			// the buffer entirely.
			toDelete = append(toDelete, it.Cur())
		}
	}
	for _, bw := range toDelete {
		twb.removeFromBuffer(bw)
	}
}

// closeLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) closeLocked() {}

// applyTransformations applies any applicable transformations to the supplied
// batch request. In doing so, a new batch request with transformations applied
// along with a list of requestRecords is returned. The caller must handle the
// transformations on the response path.
//
// Some examples of transformations include:
//
// 1. Blind writes (Put/Delete requests) are buffered locally. When the original
// request has MustAcquireExclusiveLock set, a locking Get is used to acquire
// the lock.
// 2. Point reads (Get requests) are served from the buffer and stripped from
// the batch iff the key has seen a buffered write.
// 3. Scans are always sent to the KV layer, but if the key span being scanned
// overlaps with any buffered writes, then the response from the KV layer needs
// to be merged with buffered writes. These are collected as requestRecords.
// 4. ReverseScans, similar to scans, are also always sent to the KV layer and
// their response needs to be merged with any buffered writes. The only
// difference is the direction in which the buffer is iterated when doing the
// merge. As a result, they're also collected as requestRecords.
// 5. Conditional Puts are decomposed into a locking Get followed by a Put. The
// Put is buffered locally if the condition evaluates successfully using the
// Get's response. Otherwise, a ConditionFailedError is returned.
//
// TODO(arul): Augment this comment as these expand.
func (twb *txnWriteBuffer) applyTransformations(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchRequest, requestRecords, *kvpb.Error) {
	baRemote := ba.ShallowCopy()
	// TODO(arul): We could improve performance here by pre-allocating
	// baRemote.Requests to the correct size by counting the number of Puts/Dels
	// in ba.Requests. We could also allocate the right number of ResponseUnion,
	// PutResponse, and DeleteResponse objects as well.
	baRemote.Requests = nil

	rr := make(requestRecords, 0, len(ba.Requests))
	for i, ru := range ba.Requests {
		req := ru.GetInner()
		// Track a requestRecord for the request regardless of the type, and
		// regardless of whether it was served from the buffer or not. For
		// transformed requests (e.g. CPut) this is expected. For Gets and Scans, we
		// need to track a requestRecord because we haven't buffered any writes
		// from our current batch in the buffer yet, so checking the buffer here, at
		// request time, isn't sufficient to determine whether the request needs to
		// serve a read from the buffer before returning a response or not.
		//
		// Only QueryLocksRequest and LeaseInfoRequest don't require a tracking
		// requestRecord, but it's harmless to add one, and it simplifies the code.
		//
		// The stripped and transformed fields will be set below for specific
		// requests.
		record := requestRecord{
			stripped:    false,
			transformed: false,
			index:       i,
			origRequest: req,
		}
		switch t := req.(type) {
		case *kvpb.ConditionalPutRequest:
			record.transformed = true
			// NB: Regardless of whether there is already a buffered write on
			// this key or not, we need to send a locking Get to the KV layer to
			// acquire a lock. However, if we had knowledge of what locks the
			// transaction already holds, we could avoid the locking Get in some
			// cases.
			getReq := &kvpb.GetRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:      t.Key,
					Sequence: t.Sequence,
				},
				LockNonExisting:    len(t.ExpBytes) == 0 || t.AllowIfDoesNotExist,
				KeyLockingStrength: lock.Exclusive,
			}
			var getReqU kvpb.RequestUnion
			getReqU.MustSetInner(getReq)
			// Send a locking Get request to the KV layer; we'll evaluate the
			// condition locally based on the response.
			baRemote.Requests = append(baRemote.Requests, getReqU)

		case *kvpb.PutRequest:
			// If the MustAcquireExclusiveLock flag is set on the Put, then we
			// need to add a locking Get to the BatchRequest, including if the
			// key doesn't exist.
			if t.MustAcquireExclusiveLock {
				// TODO(yuzefovich,ssd): ensure that we elide the lock
				// acquisition whenever possible (e.g. blind UPSERT in an
				// implicit txn).
				var getReqU kvpb.RequestUnion
				getReqU.MustSetInner(&kvpb.GetRequest{
					RequestHeader: kvpb.RequestHeader{
						Key:      t.Key,
						Sequence: t.Sequence,
					},
					LockNonExisting:    true,
					KeyLockingStrength: lock.Exclusive,
				})
				baRemote.Requests = append(baRemote.Requests, getReqU)
			}
			record.stripped = !t.MustAcquireExclusiveLock
			record.transformed = t.MustAcquireExclusiveLock

		case *kvpb.DeleteRequest:
			// If MustAcquireExclusiveLock flag is set on the DeleteRequest,
			// then we need to add a locking Get to the BatchRequest, including
			// if the key doesn't exist.
			if t.MustAcquireExclusiveLock {
				// TODO(ssd): ensure that we elide the lock acquisition
				// whenever possible.
				var getReqU kvpb.RequestUnion
				getReqU.MustSetInner(&kvpb.GetRequest{
					RequestHeader: kvpb.RequestHeader{
						Key:      t.Key,
						Sequence: t.Sequence,
					},
					LockNonExisting:    true,
					KeyLockingStrength: lock.Exclusive,
				})
				baRemote.Requests = append(baRemote.Requests, getReqU)
			}
			record.stripped = !t.MustAcquireExclusiveLock
			record.transformed = t.MustAcquireExclusiveLock

		case *kvpb.GetRequest:
			// If the key is in the buffer, we must serve the read from the buffer.
			// The actual serving of the read will happen on the response path though.
			stripped := false
			_, served := twb.maybeServeRead(t.Key, t.Sequence)
			if served {
				if t.KeyLockingStrength != lock.None {
					// Even though the Get request must be served from the buffer, as the
					// transaction performed a previous write to the key, we still need to
					// acquire a lock at the leaseholder. As a result, we can't strip the
					// request from the remote batch.
					//
					// TODO(arul): we could eschew sending this request if we knew there
					// was a sufficiently strong lock already present on the key.
					log.VEventf(ctx, 2, "locking %s on key %s must be sent to the server", t.Method(), t.Key)
					baRemote.Requests = append(baRemote.Requests, ru)
				} else {
					// We'll synthesize the response from the buffer on the response path;
					// eschew sending the request to the KV layer as we don't need to
					// acquire a lock.
					stripped = true
					log.VEventf(
						ctx, 2, "non-locking %s on key %s can be fully served by the client; not sending to KV", t.Method(), t.Key,
					)
				}
			} else {
				// Wasn't served locally; send the request to the KV layer.
				baRemote.Requests = append(baRemote.Requests, ru)
			}
			record.stripped = stripped

		case *kvpb.ScanRequest, *kvpb.ReverseScanRequest:
			// Regardless of whether the scan overlaps with any writes in the buffer
			// or not, we must send the request to the KV layer. We can't know for
			// sure that there's nothing else to read.
			baRemote.Requests = append(baRemote.Requests, ru)

		case *kvpb.QueryLocksRequest, *kvpb.LeaseInfoRequest:
			// These requests don't interact with buffered writes, so we simply
			// let them through.
			baRemote.Requests = append(baRemote.Requests, ru)

		default:
			return nil, nil, kvpb.NewError(unsupportedMethodError(t.Method()))
		}
		rr = append(rr, record)
	}
	return baRemote, rr, nil
}

// seekItemForSpan returns a bufferedWrite appropriate for use with a
// write-buffer iterator. Point lookups should use a nil end key.
func (twb *txnWriteBuffer) seekItemForSpan(key, endKey roachpb.Key) *bufferedWrite {
	seek := &twb.bufferSeek
	seek.key = key
	seek.endKey = endKey
	return seek
}

// maybeServeRead serves the supplied read request from the buffer if a write or
// deletion tombstone on the key is present in the buffer. Additionally, a
// boolean indicating whether the read request was served or not is also
// returned.
func (twb *txnWriteBuffer) maybeServeRead(
	key roachpb.Key, seq enginepb.TxnSeq,
) (*roachpb.Value, bool) {
	it := twb.buffer.MakeIter()
	seek := twb.seekItemForSpan(key, nil)
	it.FirstOverlap(seek)
	if it.Valid() {
		bufferedVals := it.Cur().vals
		// In the common case, we're reading the most recently buffered write. That
		// is, the sequence number we're reading at is greater than or equal to the
		// sequence number of the last write that was buffered. The list of buffered
		// values is stored in ascending order; we can therefore start iterating
		// from the end of the slice. In the common case, there won't be much
		// "iteration" happening here.
		//
		// TODO(arul): explore adding special treatment for the common case and
		// using a binary search here instead.
		for i := len(bufferedVals) - 1; i >= 0; i-- {
			if seq >= bufferedVals[i].seq {
				return bufferedVals[i].valPtr(), true
			}
		}
		// We've iterated through the buffer, but it seems like our sequence number
		// is smaller than any buffered write performed by our transaction. We can't
		// serve the read locally.
		return nil, false
	}
	return nil, false
}

// mergeWithScanResp takes a ScanRequest, that was sent to the KV layer, and the
// response returned by the KV layer, and merges it with any writes that were
// buffered by the transaction to correctly uphold read-your-own-write
// semantics.
func (twb *txnWriteBuffer) mergeWithScanResp(
	req *kvpb.ScanRequest, resp *kvpb.ScanResponse,
) (*kvpb.ScanResponse, error) {
	if req.ScanFormat == kvpb.COL_BATCH_RESPONSE {
		return nil, errors.AssertionFailedf("unexpectedly called mergeWithScanResp on a ScanRequest " +
			"with COL_BATCH_RESPONSE scan format")
	}

	respIter := newScanRespIter(req, resp)
	// First, calculate the size of the merged response. This then allows us to
	// exactly pre-allocate the response slice when constructing the respMerger.
	h := makeRespSizeHelper(respIter)
	twb.mergeBufferAndResp(respIter, h.acceptBuffer, h.acceptResp, false /* reverse */)

	respIter.reset()
	rm := makeRespMerger(respIter, h)
	twb.mergeBufferAndResp(respIter, rm.acceptKV, rm.acceptServerResp, false /* reverse */)
	return rm.toScanResp(resp, h), nil
}

// mergeWithReverseScanResp takes a ReverseScanRequest, that was sent to the KV
// layer, and the response returned by the KV layer, and merges it with any
// writes that were buffered by the transaction to correctly uphold
// read-your-own-write semantics.
func (twb *txnWriteBuffer) mergeWithReverseScanResp(
	req *kvpb.ReverseScanRequest, resp *kvpb.ReverseScanResponse,
) (*kvpb.ReverseScanResponse, error) {
	if req.ScanFormat == kvpb.COL_BATCH_RESPONSE {
		return nil, errors.AssertionFailedf("unexpectedly called mergeWithReverseScanResp on a " +
			"ReverseScanRequest with COL_BATCH_RESPONSE scan format")
	}

	respIter := newReverseScanRespIter(req, resp)
	// First, calculate the size of the merged response. This then allows us to
	// exactly pre-allocate the response slice when constructing the respMerger.
	h := makeRespSizeHelper(respIter)
	twb.mergeBufferAndResp(respIter, h.acceptBuffer, h.acceptResp, true /* reverse */)

	respIter.reset()
	rm := makeRespMerger(respIter, h)
	twb.mergeBufferAndResp(respIter, rm.acceptKV, rm.acceptServerResp, true /* reverse */)
	return rm.toReverseScanResp(resp, h), nil
}

// mergeBufferAndScanResp merges (think the merge step from merge sort) the
// buffer and the server's response to a Scan or ReverseScan request. It does so
// by iterating over both, in-order[1], and calling the appropriate accept
// function, based on which KV pair should be preferred[2] by the combined
// response.
//
// Note that acceptBuffer and acceptResp functions should not advance the
// iterator. acceptResp will only be called when respIter is in valid state.
//
// [1] Forward or reverse order, depending on the direction of the scan.
// [2] See inline comments for more details on what "preferred" means.
func (twb *txnWriteBuffer) mergeBufferAndResp(
	respIter *respIter,
	acceptBuffer func(roachpb.Key, *roachpb.Value),
	acceptResp func(),
	reverse bool,
) {
	it := twb.buffer.MakeIter()
	seek := twb.seekItemForSpan(respIter.startKey(), respIter.endKey())

	if reverse {
		it.LastOverlap(seek)
	} else {
		it.FirstOverlap(seek)
	}
	bufferNext := func() {
		// NB: we must reset seek before every use, as it's shared across multiple
		// methods on the txnWriteBuffer. In particular, it's used by
		// maybeServeRead, which we may call below.
		seek = twb.seekItemForSpan(respIter.startKey(), respIter.endKey())
		if reverse {
			it.PrevOverlap(seek)
		} else {
			it.NextOverlap(seek)
		}
	}

	for respIter.valid() && it.Valid() {
		k := respIter.peekKey()
		cmp := it.Cur().key.Compare(k)
		if reverse {
			// The comparison between keys in the buffer and the response is
			// inverted when scanning in reverse order, as we want to prefer the
			// larger of the two keys.
			cmp = cmp * -1
		}
		switch cmp {
		case -1:
			// The key in the buffer is less than the next key in the server's
			// response, so we prefer it.
			val, served := twb.maybeServeRead(it.Cur().key, respIter.seq())
			if served && val.IsPresent() {
				// NB: Only include a buffered value in the response if it hasn't been
				// deleted by the transaction previously. This matches the behaviour
				// of MVCCScan, which configures Pebble to not return deletion
				// tombstones. See pebbleMVCCScanner.add().
				acceptBuffer(it.Cur().key, val)
			}
			bufferNext()

		case 0:
			// The key exists in the buffer. We must serve the read from the buffer,
			// assuming it is visible to the sequence number of the request.
			val, served := twb.maybeServeRead(it.Cur().key, respIter.seq())
			if served {
				if val.IsPresent() {
					// NB: Only include a buffered value in the response if it hasn't been
					// deleted by the transaction previously. This matches the behaviour
					// of MVCCScan, which configures Pebble to not return deletion
					// tombstones. See pebbleMVCCScanner.add().
					acceptBuffer(it.Cur().key, val)
				}
			} else {
				// Even though the key was in the buffer, its sequence number was higher
				// than the request's. Accept the response from server.
				acceptResp()
			}
			// Move on to the next key, both in the buffer and the response.
			respIter.next()
			bufferNext()

		case 1:
			// The key in the buffer is greater than the current key in the
			// server's response, so we prefer the row from the server's
			// response.
			acceptResp()
			respIter.next()
		}
	}

	for respIter.valid() {
		acceptResp()
		respIter.next()
	}
	for it.Valid() {
		val, served := twb.maybeServeRead(it.Cur().key, respIter.seq())
		if served && val.IsPresent() {
			// Like above, we'll only include the value in the response if the Scan's
			// sequence number requires us to see it and it isn't a deletion
			// tombstone.
			acceptBuffer(it.Cur().key, val)
		}
		bufferNext()
	}
}

// mergeResponseWithRequestRecords merges responses from the KV layer with the
// requestRecords and potential transformations applied by the txnWriteBuffer
// before sending the batch request. As a result, interceptors above the
// txnWriteBuffer remain oblivious to its decision to buffer any writes.
func (twb *txnWriteBuffer) mergeResponseWithRequestRecords(
	ctx context.Context, rr requestRecords, br *kvpb.BatchResponse,
) (_ *kvpb.BatchResponse, pErr *kvpb.Error) {
	if rr.Empty() && br == nil {
		log.Fatal(ctx, "unexpectedly found no transformations and no batch response")
	} else if rr.Empty() {
		return br, nil
	}

	// All original requests are guaranteed to be in the list of requestRecords,
	// so the length of the merged responses is the same length as rr.
	mergedResps := make([]kvpb.ResponseUnion, 0, len(rr))
	for _, record := range rr {
		brResp := kvpb.ResponseUnion{}
		if !record.stripped {
			if len(br.Responses) == 0 {
				log.Fatal(ctx, "unexpectedly found a non-stripped request and no batch response")
			}
			// If the request wasn't stripped from the batch we sent to KV, we
			// received a response for it, which then needs to be combined with
			// what's in the write buffer.
			brResp = br.Responses[0]
			br.Responses = br.Responses[1:]
		}
		resp, pErr := record.toResp(ctx, twb, brResp, br.Txn)
		if pErr != nil {
			return nil, pErr
		}
		mergedResps = append(mergedResps, resp)
	}

	br.Responses = mergedResps
	return br, nil
}

// requestRecord stores a set of metadata fields about potential transformations
// applied by the txnWriteBuffer on a batch request that needs to be accounted
// for when returning the response.
type requestRecord struct {
	// stripped, if true, indicates that the request was stripped from the batch
	// and never sent to the KV layer.
	stripped bool
	// transformed, if true, indicates that the request was transformed into a
	// different request to be sent to the KV layer. If stripped is true, then
	// transformed is always false; i.e. if the request was completely dropped,
	// then it's not considered transformed.
	transformed bool
	// index of the request in the original batch to which the requestRecord
	// applies.
	index int
	// origRequest is the original request that was transformed.
	origRequest kvpb.Request
}

// toResp returns the response that should be added to the batch response as
// a result of applying the requestRecord.
func (rr requestRecord) toResp(
	ctx context.Context, twb *txnWriteBuffer, br kvpb.ResponseUnion, txn *roachpb.Transaction,
) (kvpb.ResponseUnion, *kvpb.Error) {
	var ru kvpb.ResponseUnion
	switch req := rr.origRequest.(type) {
	case *kvpb.ConditionalPutRequest:
		// Evaluate the condition.
		evalFn := mvcceval.MaybeConditionFailedError
		if twb.testingOverrideCPutEvalFn != nil {
			evalFn = twb.testingOverrideCPutEvalFn
		}

		var val *roachpb.Value
		var served bool
		val, served = twb.maybeServeRead(req.Key, req.Sequence)
		if !served {
			// We only use the response from KV if there wasn't already a
			// buffered value for this key that our transaction wrote
			// previously.
			val = br.GetInner().(*kvpb.GetResponse).Value
		}

		condFailedErr := evalFn(
			req.ExpBytes,
			val,
			val.IsPresent(),
			req.AllowIfDoesNotExist,
		)
		if condFailedErr != nil {
			pErr := kvpb.NewErrorWithTxn(condFailedErr, txn)
			pErr.SetErrorIndex(int32(rr.index))
			return kvpb.ResponseUnion{}, pErr
		}
		// The condition was satisfied; buffer the write and return a
		// synthesized response.
		ru.MustSetInner(&kvpb.ConditionalPutResponse{})
		twb.addToBuffer(req.Key, req.Value, req.Sequence, req.KVNemesisSeq)

	case *kvpb.PutRequest:
		ru.MustSetInner(&kvpb.PutResponse{})
		twb.addToBuffer(req.Key, req.Value, req.Sequence, req.KVNemesisSeq)

	case *kvpb.DeleteRequest:
		// To correctly populate FoundKey in the response, we must prefer any
		// buffered values (if they exist).
		var foundKey bool
		val, served := twb.maybeServeRead(req.Key, req.Sequence)
		if served {
			log.VEventf(ctx, 2, "serving read portion of %s on key %s from the buffer", req.Method(), req.Key)
			foundKey = val.IsPresent()
		} else if req.MustAcquireExclusiveLock {
			// We sent a GetRequest to the KV layer to acquire an exclusive lock
			// on the key, regardless of whether the key already exists or not.
			// Populate FoundKey using the response.
			getResp := br.GetInner().(*kvpb.GetResponse)
			if log.ExpensiveLogEnabled(ctx, 2) {
				log.Eventf(ctx, "synthesizing DeleteResponse from GetResponse: %#v", getResp)
			}
			foundKey = getResp.Value.IsPresent()
		} else {
			// NB: If MustAcquireExclusiveLock wasn't set by the client then we
			// eschew sending a Get request to the KV layer just to populate
			// FoundKey correctly. So we're assuming that callers who care
			// whether a key is found or not also want to acquire an exclusive
			// lock on it. While this is true as of the time of writing, the
			// behaviour here is less than ideal.
			//
			// TODO(arul): improve the FoundKey semantics to have callers opt
			// into whether the care about the key being found. Alternatively,
			// clarify the behaviour on DeleteRequest.
			foundKey = false
		}
		ru.MustSetInner(&kvpb.DeleteResponse{
			FoundKey: foundKey,
		})
		twb.addToBuffer(req.Key, roachpb.Value{}, req.Sequence, req.KVNemesisSeq)

	case *kvpb.GetRequest:
		val, served := twb.maybeServeRead(req.Key, req.Sequence)
		if served {
			getResp := &kvpb.GetResponse{}
			if val.IsPresent() {
				getResp.Value = val
			}
			ru.MustSetInner(getResp)
			log.VEventf(ctx, 2, "serving %s on key %s from the buffer", req.Method(), req.Key)
		} else {
			// The request wasn't served from the buffer; return the response from the
			// KV layer.
			assertTrue(!rr.stripped, "we shouldn't be stripping requests that aren't served from the buffer")
			ru = br
		}

	case *kvpb.ScanRequest:
		scanResp, err := twb.mergeWithScanResp(
			rr.origRequest.(*kvpb.ScanRequest), br.GetInner().(*kvpb.ScanResponse),
		)
		if err != nil {
			return kvpb.ResponseUnion{}, kvpb.NewError(err)
		}
		ru.MustSetInner(scanResp)

	case *kvpb.ReverseScanRequest:
		reverseScanResp, err := twb.mergeWithReverseScanResp(
			rr.origRequest.(*kvpb.ReverseScanRequest), br.GetInner().(*kvpb.ReverseScanResponse),
		)
		if err != nil {
			return kvpb.ResponseUnion{}, kvpb.NewError(err)
		}
		ru.MustSetInner(reverseScanResp)

	case *kvpb.QueryLocksRequest, *kvpb.LeaseInfoRequest:
		// These requests don't interact with buffered writes, so we simply
		// let the response through unchanged.
		ru = br

	default:
		return ru, kvpb.NewError(unsupportedMethodError(req.Method()))
	}

	if buildutil.CrdbTestBuild {
		if ru.GetInner() == nil {
			panic(errors.AssertionFailedf("expected response to be set for type %T", rr.origRequest))
		}
	}

	return ru, nil
}

// requestRecords is a slice of requestRecord.
type requestRecords []requestRecord

func (rr requestRecords) Empty() bool {
	return len(rr) == 0
}

// addToBuffer adds a write to the given key to the buffer.
func (twb *txnWriteBuffer) addToBuffer(
	key roachpb.Key, val roachpb.Value, seq enginepb.TxnSeq, kvNemSeq kvnemesisutil.Container,
) {
	it := twb.buffer.MakeIter()
	seek := twb.seekItemForSpan(key, nil)

	it.FirstOverlap(seek)
	if it.Valid() {
		// We've already seen a write for this key.
		bw := it.Cur()
		val := bufferedValue{val: val, seq: seq, kvNemesisSeq: kvNemSeq}
		bw.vals = append(bw.vals, val)
		twb.bufferSize += val.size()
	} else {
		twb.bufferIDAlloc++
		bw := &bufferedWrite{
			id:   twb.bufferIDAlloc,
			key:  key,
			vals: []bufferedValue{{val: val, seq: seq, kvNemesisSeq: kvNemSeq}},
		}
		twb.buffer.Set(bw)
		twb.bufferSize += bw.size()
	}
}

// removeFromBuffer removes all buffered writes on a given key from the buffer.
func (twb *txnWriteBuffer) removeFromBuffer(bw *bufferedWrite) {
	twb.buffer.Delete(bw)
	twb.bufferSize -= bw.size()
}

// flushBufferAndSendBatch flushes all buffered writes when sending the supplied
// batch request to the KV layer. This is done by pre-pending the buffered
// writes to the requests in the batch.
//
// The response is transformed to hide the fact that requests were added to the
// batch to flush the buffer. Upper layers remain oblivious to the flush and any
// buffering in general.
func (twb *txnWriteBuffer) flushBufferAndSendBatch(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	defer func() {
		assertTrue(twb.buffer.Len() == 0, "buffer should be empty after flush")
		assertTrue(twb.bufferSize == 0, "buffer size should be 0 after flush")
		assertTrue(twb.flushed, "flushed should be true after flush")
	}()

	// Once we've flushed the buffer, we disable write buffering going forward. We
	// do this even if the buffer is empty since once we've called this function,
	// our buffer no longer represents all of the writes in the transaction.
	log.VEventf(ctx, 2, "disabling write buffering for this epoch")
	twb.flushed = true

	numBuffered := twb.buffer.Len()
	if numBuffered == 0 {
		return twb.wrapped.SendLocked(ctx, ba) // nothing to flush
	}

	if _, ok := ba.GetArg(kvpb.EndTxn); !ok {
		// We're flushing the buffer even though the batch doesn't contain an EndTxn
		// request. That means we buffered some writes and decided to disable write
		// buffering mid-way through the transaction, thus necessitating this flush.
		twb.txnMetrics.TxnWriteBufferDisabledAfterBuffering.Inc(1)
	}

	// Flush all buffered writes by pre-pending them to the requests being sent
	// in the batch.
	reqs := make([]kvpb.RequestUnion, 0, numBuffered+len(ba.Requests))
	it := twb.buffer.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		reqs = append(reqs, it.Cur().toRequest())
	}
	twb.resetBuffer()

	// Layers below us expect that writes inside a batch are in sequence number
	// order but the iterator above returns data in key order. Here we re-sort it
	// which is unfortunate but required unless we make a change to the pipeliner.
	slices.SortFunc(reqs, func(a kvpb.RequestUnion, b kvpb.RequestUnion) int {
		aHeader := a.GetInner().Header()
		bHeader := b.GetInner().Header()
		if aHeader.Sequence == bHeader.Sequence {
			return aHeader.Key.Compare(bHeader.Key)
		} else if aHeader.Sequence < bHeader.Sequence {
			return -1
		} else {
			return 1
		}
	})

	ba = ba.ShallowCopy()
	ba.Requests = append(reqs, ba.Requests...)
	br, pErr := twb.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, twb.adjustErrorUponFlush(ctx, numBuffered, pErr)
	}

	// Strip out responses for all the flushed buffered writes.
	br.Responses = br.Responses[numBuffered:]
	return br, nil
}

// hasBufferedWrites returns whether the interceptor has buffered any writes
// locally.
func (twb *txnWriteBuffer) hasBufferedWrites() bool {
	return twb.buffer.Len() > 0
}

// shouldBuffer returns true if SendLocked() should attempt to buffer parts of
// the batch.
func (twb *txnWriteBuffer) shouldBuffer() bool {
	return twb.enabled && !twb.flushed
}

// testingBufferedWritesAsSlice returns all buffered writes, in key order, as a
// slice.
func (twb *txnWriteBuffer) testingBufferedWritesAsSlice() []bufferedWrite {
	var writes []bufferedWrite
	it := twb.buffer.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		bw := *it.Cur()
		// Scrub the id/endKey for the benefit of tests.
		bw.id = 0
		bw.endKey = nil
		writes = append(writes, bw)
	}
	return writes
}

const bufferedWriteStructOverhead = int64(unsafe.Sizeof(bufferedWrite{}))

// bufferedWrite is a buffered write operation to a given key. It maps a key to
// possibly multiple values[1], each with an associated sequence number.
//
// [1] A transaction is allowed to write to a single key multiple times. Of
// this, only the final write needs to be flushed to the KV layer. However, we
// track intermediate values in the buffer to support read-your-own-writes and
// savepoint rollbacks.
type bufferedWrite struct {
	id  uint64
	key roachpb.Key
	// TODO(arul): explore the possibility of using a b-tree which doesn't use an
	// endKey as a comparator. We could then remove this unnecessary field here,
	// and also in the keyLocks struct.
	endKey roachpb.Key // used in btree iteration
	// TODO(arul): instead of this slice, consider adding a small (fixed size,
	// maybe 1) array instead.
	vals []bufferedValue // sorted in increasing sequence number order
}

func (bw *bufferedWrite) size() int64 {
	size := keySize(bw.key) + keySize(bw.endKey) + bufferedWriteStructOverhead
	for _, v := range bw.vals {
		size += v.size()
	}
	return size
}

const bufferedValueStructOverhead = int64(unsafe.Sizeof(bufferedValue{}))

// bufferedValue is a value written to a key at a given sequence number.
type bufferedValue struct {
	// NB: Keep this at the start of the struct so that it is zero (size) cost in
	// production.
	kvNemesisSeq kvnemesisutil.Container
	val          roachpb.Value
	seq          enginepb.TxnSeq
}

// valPtr returns a pointer to the buffered value.
func (bv *bufferedValue) valPtr() *roachpb.Value {
	// TODO(arul): add a knob to return a pointer into the buffer instead of
	// creating a copy. As long as the caller doesn't modify the value we should
	// be fine; just have them opt into it.
	valCpy := bv.val
	return &valCpy
}

func (bv *bufferedValue) size() int64 {
	return int64(len(bv.val.RawBytes)) + bufferedValueStructOverhead
}

//go:generate ../../../util/interval/generic/gen.sh *bufferedWrite kvcoord

// Methods required by util/interval/generic type contract.

func (bw *bufferedWrite) ID() uint64          { return bw.id }
func (bw *bufferedWrite) Key() []byte         { return bw.key }
func (bw *bufferedWrite) EndKey() []byte      { return bw.endKey }
func (bw *bufferedWrite) String() string      { return bw.key.String() }
func (bw *bufferedWrite) New() *bufferedWrite { return new(bufferedWrite) }
func (bw *bufferedWrite) SetID(v uint64)      { bw.id = v }
func (bw *bufferedWrite) SetKey(v []byte)     { bw.key = v }
func (bw *bufferedWrite) SetEndKey(v []byte)  { bw.endKey = v }

func (bw *bufferedWrite) toRequest() kvpb.RequestUnion {
	var ru kvpb.RequestUnion
	// A key may be written to multiple times during the course of a transaction.
	// However, when flushing to KV, we only need to flush the most recent write
	// (read: the one with the highest sequence number). As we store values in
	// increasing sequence number order, this should be the last value in the
	// slice.
	val := bw.vals[len(bw.vals)-1]
	if val.val.IsPresent() {
		// TODO(arul): we could allocate PutRequest objects all at once when we're
		// about to flush the buffer. We'll probably want to keep track of the
		// number of each request type in the btree to avoid iterating and counting
		// each request type.
		//
		// TODO(arul): should we use a sync.Pool here?
		putAlloc := new(struct {
			put   kvpb.PutRequest
			union kvpb.RequestUnion_Put
		})
		putAlloc.put.Key = bw.key
		putAlloc.put.Value = val.val
		putAlloc.put.Sequence = val.seq
		putAlloc.put.KVNemesisSeq = val.kvNemesisSeq
		putAlloc.union.Put = &putAlloc.put
		ru.Value = &putAlloc.union
	} else {
		delAlloc := new(struct {
			del   kvpb.DeleteRequest
			union kvpb.RequestUnion_Delete
		})
		delAlloc.del.Key = bw.key
		delAlloc.del.Sequence = val.seq
		delAlloc.del.KVNemesisSeq = val.kvNemesisSeq
		delAlloc.union.Delete = &delAlloc.del
		ru.Value = &delAlloc.union
	}
	return ru
}

// getKey reads the key for the next KV from a slice of BatchResponses field of
// {,Reverse}ScanResponse. The KV is encoded in the following format:
//
//	<lenValue:Uint32><lenKey:Uint32><Key><Value>
//
// Furthermore, MVCC timestamp might be included in the suffix of <Key> part, so
// we need to split it away.
//
// The method assumes that the encoding is valid.
func getKey(br []byte) []byte {
	lenKey := int(binary.LittleEndian.Uint32(br[4:8]))
	key, _, _ := enginepb.SplitMVCCKey(br[8 : 8+lenKey])
	return key
}

// getFirstKVLength returns the number of bytes used to encode the first KV from
// the given slice (which is assumed to have come from BatchResponses field of
// {,Reverse}ScanResponse).
func getFirstKVLength(br []byte) int {
	// See comment on getKey for more details.
	lenValue := int(binary.LittleEndian.Uint32(br[0:4]))
	lenKey := int(binary.LittleEndian.Uint32(br[4:8]))
	return 8 + lenKey + lenValue
}

// encKVLength returns the number of bytes that will be required to encode the
// given key/value pair as well as just encoding length of the key (including
// the timestamp).
func encKVLength(key roachpb.Key, value *roachpb.Value) (lenKV, lenKey int) {
	// See comment on getKey for more details.
	lenKey = mvccencoding.EncodedMVCCKeyLength(key, value.Timestamp)
	lenKV = 8 + lenKey + len(value.RawBytes)
	return lenKV, lenKey
}

// appendKV appends the given key/value pair to the provided slice. It is
// assumed that the slice already has enough capacity. The updated slice is
// returned.
func appendKV(toAppend []byte, key roachpb.Key, value *roachpb.Value) []byte {
	lenKV, lenKey := encKVLength(key, value)
	buf := toAppend[len(toAppend) : len(toAppend)+lenKV]
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(value.RawBytes)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(lenKey))
	mvccencoding.EncodeMVCCKeyToBufSized(buf[8:8+lenKey], key, value.Timestamp, lenKey)
	copy(buf[8+lenKey:], value.RawBytes)
	return toAppend[:len(toAppend)+lenKV]
}

// respIter is an iterator over a scan or reverse scan response returned by
// the KV layer.
type respIter struct {
	// One and only one of scanReq/reverseScanReq should ever be set.
	scanReq        *kvpb.ScanRequest
	reverseScanReq *kvpb.ReverseScanRequest
	// scanFormat indicates the ScanFormat of the request. Only KEY_VALUES and
	// BATCH_RESPONSE are supported right now.
	scanFormat kvpb.ScanFormat

	// resumeSpan, if set, is the ResumeSpan of the response. When non-nil, it
	// means that the response is being paginated, so we need to overlap the
	// buffer only with the part of the original span that was actually scanned.
	resumeSpan *roachpb.Span

	// rows is the Rows field of the corresponding response.
	//
	// Only set with KEY_VALUES scan format.
	rows []roachpb.KeyValue
	// batchResponses is the BatchResponses field of the corresponding response.
	//
	// Only set with BATCH_RESPONSE scan format.
	batchResponses [][]byte

	// Fields below will be modified when advancing the iterator.

	// rowsIndex is the current index into Rows field of the response.
	//
	// Used in the KEY_VALUES scan format.
	rowsIndex int
	// brIndex and brOffset describe the current position within BatchResponses
	// field of the response. The next KV starts at
	// batchResponses[brIndex][brOffset]. When the end of the slice is reached,
	// brIndex is incremented and brOffset is reset to 0.
	//
	// Additionally, brIndex controls which BatchResponses[i] slice KVs are
	// being written into when merging the response with the buffered writes.
	//
	// Used in the BATCH_RESPONSE scan format.
	brIndex  int
	brOffset int
}

// newScanRespIter constructs and returns a new iterator to iterate over a
// ScanRequest/Response.
func newScanRespIter(req *kvpb.ScanRequest, resp *kvpb.ScanResponse) *respIter {
	if req.ScanFormat != kvpb.KEY_VALUES && req.ScanFormat != kvpb.BATCH_RESPONSE {
		panic("unexpected")
	}
	return &respIter{
		scanReq:        req,
		scanFormat:     req.ScanFormat,
		resumeSpan:     resp.ResumeSpan,
		rows:           resp.Rows,
		batchResponses: resp.BatchResponses,
	}
}

// newReverseScanRespIter constructs and returns a new iterator to iterate over
// a ReverseScanRequest/Response.
func newReverseScanRespIter(
	req *kvpb.ReverseScanRequest, resp *kvpb.ReverseScanResponse,
) *respIter {
	if req.ScanFormat != kvpb.KEY_VALUES && req.ScanFormat != kvpb.BATCH_RESPONSE {
		panic("unexpected")
	}
	return &respIter{
		reverseScanReq: req,
		scanFormat:     req.ScanFormat,
		resumeSpan:     resp.ResumeSpan,
		rows:           resp.Rows,
		batchResponses: resp.BatchResponses,
	}
}

// peekKey returns the key at the current iterator position.
//
// peekKey should only be called if the iterator is in valid state (i.e.
// valid() returned true).
func (s *respIter) peekKey() roachpb.Key {
	if s.scanFormat == kvpb.KEY_VALUES {
		return s.rows[s.rowsIndex].Key
	}
	return getKey(s.batchResponses[s.brIndex][s.brOffset:])
}

// next moves the iterator forward.
//
// next should only be called if the iterator is in valid state (i.e. valid()
// returned true).
func (s *respIter) next() {
	if s.scanFormat == kvpb.KEY_VALUES {
		s.rowsIndex++
		return
	}
	s.brOffset += getFirstKVLength(s.batchResponses[s.brIndex][s.brOffset:])
	if s.brOffset >= len(s.batchResponses[s.brIndex]) {
		s.brIndex++
		s.brOffset = 0
	}
}

// valid returns whether the iterator is (still) positioned to a valid index.
func (s *respIter) valid() bool {
	if s.scanFormat == kvpb.KEY_VALUES {
		return s.rowsIndex < len(s.rows)
	}
	return s.brIndex < len(s.batchResponses)
}

// reset re-positions the iterator to the beginning of the response.
func (s *respIter) reset() {
	s.rowsIndex = 0
	s.brIndex = 0
	s.brOffset = 0
}

// startKey returns the start key of the request in response to which the
// iterator was created.
func (s *respIter) startKey() roachpb.Key {
	if s.scanReq != nil {
		return s.scanReq.Key
	}
	// For ReverseScans, the EndKey of the ResumeSpan is updated to indicate the
	// start key for the "next" page, which is exactly the last key that was
	// reverse-scanned for the current response.
	// TODO(yuzefovich): we should have some unit tests that exercise the
	// ResumeSpan case.
	if s.resumeSpan != nil {
		return s.resumeSpan.EndKey
	}
	return s.reverseScanReq.Key
}

// endKey returns the end key of the request in response to which the iterator
// was created.
func (s *respIter) endKey() roachpb.Key {
	if s.scanReq != nil {
		// For Scans, the Key of the ResumeSpan is updated to indicate the start
		// key for the "next" page, which is exactly the last key that was
		// scanned for the current response.
		if s.resumeSpan != nil {
			return s.resumeSpan.Key
		}
		return s.scanReq.EndKey
	}
	return s.reverseScanReq.EndKey
}

// seq returns the sequence number of the request in response to which the
// iterator was created.
func (s *respIter) seq() enginepb.TxnSeq {
	if s.scanReq != nil {
		return s.scanReq.Sequence
	}
	return s.reverseScanReq.Sequence
}

type respSizeHelper struct {
	it *respIter

	// numKeys and numBytes track the values that NumKeys and NumBytes fields of
	// the merged {,Reverse}ScanResponse should be set to.
	numKeys  int64
	numBytes int64

	// rowsSize tracks the total number of KVs that we'll include in the Rows
	// field of the merged {,Reverse}ScanResponse when KEY_VALUES scan format is
	// used.
	rowsSize int
	// batchResponseSize tracks the lengths of each []byte that we'll include in
	// the BatchResponses field of the merged {,Reverse}ScanResponse when
	// BATCH_RESPONSE scan format is used.
	//
	// At the moment, we'll rely on the "structure" produced by the server
	// meaning that we'll "inject" the buffered KVs into responses from the
	// server while maintaining the "layering" of slices. In the extreme case
	// when the server produced an empty response this means that we'll include
	// all buffered KVs in a single slice.
	// TODO(yuzefovich): add better sizing heuristic which will allow for faster
	// garbage collection of already processed KVs by the SQL layer.
	//
	// Length of this slice is always 1 greater than the length of the
	// BatchResponses field from the server response in order to include a
	// "spill-over" slice - this is done to accommodate any buffered writes
	// after the server response is fully processed.
	batchResponseSize []int
}

func makeRespSizeHelper(it *respIter) respSizeHelper {
	h := respSizeHelper{it: it}
	if it.scanFormat == kvpb.BATCH_RESPONSE {
		h.batchResponseSize = make([]int, len(it.batchResponses)+1)
	}
	return h
}

func (h *respSizeHelper) acceptBuffer(key roachpb.Key, value *roachpb.Value) {
	h.numKeys++
	lenKV, _ := encKVLength(key, value)
	h.numBytes += int64(lenKV)
	if h.it.scanFormat == kvpb.KEY_VALUES {
		h.rowsSize++
		return
	}
	// Note that this will always be in bounds even when h.it is no longer
	// valid (due to the "spill-over" slice).
	h.batchResponseSize[h.it.brIndex] += lenKV
}

func (h *respSizeHelper) acceptResp() {
	h.numKeys++
	if h.it.scanFormat == kvpb.KEY_VALUES {
		kv := h.it.rows[h.it.rowsIndex]
		lenKV, _ := encKVLength(kv.Key, &kv.Value)
		h.numBytes += int64(lenKV)
		h.rowsSize++
		return
	}
	br := h.it.batchResponses[h.it.brIndex][h.it.brOffset:]
	lenKV := getFirstKVLength(br)
	h.numBytes += int64(lenKV)
	h.batchResponseSize[h.it.brIndex] += lenKV
}

// respMerger encapsulates state to combine a {,Reverse}ScanResponse, returned
// by the KV layer, with any overlapping buffered writes to correctly uphold
// read-your-own-write semantics. It can be used to accumulate a response when
// merging a {,Reverse}ScanResponse with buffered writes.
type respMerger struct {
	serverRespIter *respIter

	// rows is the Rows field of the corresponding response. The merged response
	// will be accumulated here first before being injected into one of the
	// response structs.
	//
	// Only populated with KEY_VALUES scan format.
	rows []roachpb.KeyValue

	// rowsIdx tracks the position within rows slice of the response to be
	// populated next.
	rowsIdx int

	// batchResponses is the BatchResponses field of the corresponding response.
	// The merged response will be accumulated here first before being injected
	// into one of the response structs.
	//
	// Only populated with BATCH_RESPONSE scan format.
	//
	// Note that unlike for rows, we don't have any position tracking in this
	// struct for batchResponses -- this is because we reuse respIter.brIndex to
	// indicate which []byte to write into.
	batchResponses [][]byte
}

// makeRespMerger constructs and returns a new respMerger.
func makeRespMerger(serverSideRespIter *respIter, h respSizeHelper) respMerger {
	m := respMerger{
		serverRespIter: serverSideRespIter,
	}
	if serverSideRespIter.scanFormat == kvpb.KEY_VALUES {
		m.rows = make([]roachpb.KeyValue, h.rowsSize)
	} else {
		m.batchResponses = make([][]byte, len(h.batchResponseSize))
		for i, size := range h.batchResponseSize {
			m.batchResponses[i] = make([]byte, 0, size)
		}
	}
	return m
}

// acceptKV takes a key and a value (presumably from the write buffer) and adds
// it to the result set.
func (m *respMerger) acceptKV(key roachpb.Key, value *roachpb.Value) {
	it := m.serverRespIter
	if it.scanFormat == kvpb.KEY_VALUES {
		m.rows[m.rowsIdx] = roachpb.KeyValue{
			Key:   key,
			Value: *value,
		}
		m.rowsIdx++
		return
	}
	// Note that this will always be in bounds even when the server resp
	// iterator is no longer valid (due to the "spill-over" slice).
	m.batchResponses[it.brIndex] = appendKV(m.batchResponses[it.brIndex], key, value)
}

// acceptServerResp accepts the current server response and adds it to the
// result set.
func (m *respMerger) acceptServerResp() {
	it := m.serverRespIter
	if it.scanFormat == kvpb.KEY_VALUES {
		m.rows[m.rowsIdx] = it.rows[it.rowsIndex]
		m.rowsIdx++
		return
	}
	br := it.batchResponses[it.brIndex][it.brOffset:]
	toAppend := br[:getFirstKVLength(br)]
	m.batchResponses[it.brIndex] = append(m.batchResponses[it.brIndex], toAppend...)
}

// toScanResp populates a copy of the given response with the final merged
// state.
func (m *respMerger) toScanResp(resp *kvpb.ScanResponse, h respSizeHelper) *kvpb.ScanResponse {
	assertTrue(m.serverRespIter.scanReq != nil, "weren't accumulating a scan resp")
	result := resp.ShallowCopy().(*kvpb.ScanResponse)
	result.NumKeys = h.numKeys
	result.NumBytes = h.numBytes
	if m.serverRespIter.scanFormat == kvpb.KEY_VALUES {
		// If we've done everything correctly, resIdx == len(response rows).
		assertTrue(m.rowsIdx == len(m.rows), "did not fill in all rows; did we miscount?")
		result.Rows = m.rows
		return result
	} else {
		// If we've done everything correctly, then each BatchResponses[i] slice
		// should've been filled up to capacity.
		for _, br := range m.batchResponses {
			assertTrue(len(br) == cap(br), "incorrect calculation of BatchResponses[i] slice capacity")
		}
		if lastIdx := len(m.batchResponses) - 1; lastIdx > 0 && len(m.batchResponses[lastIdx]) == 0 {
			// If we didn't use the "spill-over" slice, then remove it.
			m.batchResponses = m.batchResponses[:lastIdx]
		}
		result.BatchResponses = m.batchResponses
		return result
	}
}

// toReverseScanResp populates a copy of the given response with the final
// merged state.
func (m *respMerger) toReverseScanResp(
	resp *kvpb.ReverseScanResponse, h respSizeHelper,
) *kvpb.ReverseScanResponse {
	assertTrue(m.serverRespIter.scanReq == nil, "weren't accumulating a reverse scan resp")
	result := resp.ShallowCopy().(*kvpb.ReverseScanResponse)
	result.NumKeys = h.numKeys
	result.NumBytes = h.numBytes
	if m.serverRespIter.scanFormat == kvpb.KEY_VALUES {
		// If we've done everything correctly, resIdx == len(response rows).
		assertTrue(m.rowsIdx == len(m.rows), "did not fill in all rows; did we miscount?")
		result.Rows = m.rows
		return result
	} else {
		// If we've done everything correctly, then each BatchResponses[i] slice
		// should've been filled up to capacity.
		for _, br := range m.batchResponses {
			assertTrue(len(br) == cap(br), "incorrect calculation of BatchResponses[i] slice capacity")
		}
		if lastIdx := len(m.batchResponses) - 1; lastIdx > 0 && len(m.batchResponses[lastIdx]) == 0 {
			// If we didn't use the "spill-over" slice, then remove it.
			m.batchResponses = m.batchResponses[:lastIdx]
		}
		result.BatchResponses = m.batchResponses
		return result
	}
}

// assertTrue panics with a message if the supplied condition isn't true.
func assertTrue(cond bool, msg string) {
	if !cond && buildutil.CrdbTestBuild {
		panic(msg)
	}
}
