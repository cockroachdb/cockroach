// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
)

// BufferedWritesEnabled is used to enable write buffering.
var BufferedWritesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.write_buffering.enabled",
	"if enabled, transactional writes are buffered on the client",
	false,
	settings.WithPublic,
)

// txnWriteBuffer is a txnInterceptor that buffers transactional writes until
// commit time. Moreover, it also decomposes read-write KV operations (e.g.
// CPuts, InitPuts) into separate (locking) read and write operations, buffering
// the latter until commit time.
//
// Buffering writes until commit time has four main benefits:
//
// 1. It allows for more batching of writes, which can be more efficient.
// Instead of sending write batches one at a time, we can batch all write
// batches and send the in a single batch at commit time. This is a win even if
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
type txnWriteBuffer struct {
	enabled bool

	buffer        btree
	bufferSeek    bufferedWrite // re-use while seeking
	bufferIDAlloc uint64

	wrapped lockedSender
}

func (twb *txnWriteBuffer) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if !twb.enabled {
		return twb.wrapped.SendLocked(ctx, ba)
	}

	if _, ok := ba.GetArg(kvpb.EndTxn); ok {
		// TODO(arul): should we only flush if the transaction is being committed?
		// If the transaction is being rolled back, we shouldn't needlessly flush
		// writes.
		return twb.flushWithEndTxn(ctx, ba)
	}

	ba, transformations := twb.applyTransformations(ctx, ba)

	br, pErr := twb.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	return twb.mergeResponseWithTransformations(ctx, transformations, br), nil
}

// setWrapped implements the txnInterceptor interface.
func (twb *txnWriteBuffer) setWrapped(wrapped lockedSender) {
	twb.wrapped = wrapped
}

// populateLeafInputState is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) populateLeafInputState(*roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) epochBumpedLocked() {}

// createSavepointLocked is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (twb *txnWriteBuffer) rollbackToSavepointLocked(ctx context.Context, s savepoint) {}

// closeLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) closeLocked() {}

// applyTransformations applies any applicable transformations to the supplied
// batch request. In doing so, the batch request is modified in place, and a
// list of transformations that were applied is returned. The caller must handle
// these on the response path.
//
// Some examples of transformations include:
//
// 1. Blind writes (Put/Delete requests) are stripped from the batch and
// buffered locally.
//
// TODO(arul): Augment this comment as these expand.
func (twb *txnWriteBuffer) applyTransformations(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchRequest, transformations) {
	baRemote := ba.ShallowCopy()
	baRemote.Requests = nil

	var transformations transformations

	for i, ru := range ba.Requests {
		req := ru.GetInner()
		switch t := req.(type) {
		case *kvpb.PutRequest:
			var ru kvpb.ResponseUnion
			ru.MustSetInner(&kvpb.PutResponse{})
			transformations = append(transformations, transformation{
				stripped: true,
				index:    i,
				resp:     ru,
			})

			twb.addToBuffer(t.Key, t.Value, t.Sequence)
		case *kvpb.DeleteRequest:
			var ru kvpb.ResponseUnion
			ru.MustSetInner(&kvpb.DeleteResponse{
				// TODO(arul): We need to add a flag to DeleteRequests to indicate
				// whether we care about the return value or not. If we do, we need to
				// decompose the Delete into a read-write phase. Otherwise, we can
				// consider the Delete a blind write, and return false here.
				FoundKey: false,
			})
			transformations = append(transformations, transformation{
				stripped: true,
				index:    i,
				resp:     ru,
			})
			twb.addToBuffer(t.Key, roachpb.Value{}, t.Sequence)

		default:
			baRemote.Requests = append(baRemote.Requests, ru)
		}
	}
	return baRemote, transformations
}

// mergeResponsesWithTransformations merges responses from the KV layer with the
// transformations that were applied by the txnWriteBuffer before sending the
// batch request. As a result, interceptors above the txnWriteBuffer remain
// oblivious to its decision to buffer any writes.
func (twb *txnWriteBuffer) mergeResponseWithTransformations(
	ctx context.Context, transformations transformations, br *kvpb.BatchResponse,
) *kvpb.BatchResponse {
	if transformations.Empty() {
		return br
	}
	mergedResps := make([]kvpb.ResponseUnion, len(br.Responses)+len(transformations))
	for i := range mergedResps {
		if len(transformations) > 0 && transformations[0].index == i {
			// The transformation applies at this index.
			mergedResps[i] = transformations[0].toResp()
			transformations = transformations[1:]

			// TODO(arul): we'll also need to handle !transformation.stripped case here
			// as well. In particular, if a transformation didn't strip the request,
			// but instead modified it, we'll need to trim from br.Responses and trim
			// the mergedResps slice -- we'd otherwise be overcounting.
			continue
		}

		// No transformation applies at this index. Copy over the response as is.
		mergedResps[i] = br.Responses[0]
		br.Responses = br.Responses[1:]
	}
	br.Responses = mergedResps
	return br
}

// transformation is a modification applied by the txnWriteBuffer on a batch
// request that needs to be accounted for when returning the response.
type transformation struct {
	// stripped, if true, indicates that the request was stripped from the batch
	// and never sent to the KV layer.
	stripped bool
	// index of the request in the original batch to which the transformation applies
	index int
	// resp is locally produced response that needs to be merged with any
	// responses returned by the KV layer. This is set for requests that can be
	// evaluated locally (e.g. blind writes, reads that can be served entirely
	// from the buffer). If non-empty, stripped must also be true.
	resp kvpb.ResponseUnion
}

// toResp returns the response that should be added to the batch response as
// a result of applying the transformation.
func (t transformation) toResp() kvpb.ResponseUnion {
	if t.stripped {
		return t.resp
	}

	// This is only possible once we start decomposing read-write requests into
	// separate bits.
	panic("unimplemented")

	// TODO(arul): in the future, when we'll evaluate CPuts locally, we'll have
	// this function take in the result of the KVGet, save the CPut function
	// locally on the transformation, and use these two things to evaluate the
	// condition here, on the client. We'll then construct and return the
	// appropriate response.
}

// transformations is a list of transformations applied by the txnWriteBuffer.
type transformations []transformation

func (t transformations) Empty() bool {
	return len(t) == 0
}

// addToBuffer adds a write to the given key to the buffer.
func (twb *txnWriteBuffer) addToBuffer(key roachpb.Key, val roachpb.Value, seq enginepb.TxnSeq) {
	it := twb.buffer.MakeIter()
	seek := &twb.bufferSeek
	seek.key = key

	it.FirstOverlap(seek)
	if it.Valid() {
		// We've already seen a write for this key.
		bw := it.Cur()
		bw.vals = append(bw.vals, bufferedValue{val: val, seq: seq})
	} else {
		twb.bufferIDAlloc++
		twb.buffer.Set(&bufferedWrite{
			id:   twb.bufferIDAlloc,
			key:  key,
			vals: []bufferedValue{{val: val, seq: seq}},
		})
	}
}

// flushWithEndTxn flushes all buffered writes to the KV layer along with the
// EndTxn request. Responses from the flushing are stripped before returning.
func (twb *txnWriteBuffer) flushWithEndTxn(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	numBuffered := twb.buffer.Len()
	if numBuffered == 0 {
		return twb.wrapped.SendLocked(ctx, ba) // nothing to flush
	}
	// Iterate over the buffered writes and flush all buffered writes to the KV
	// layer by adding them to the batch.
	reqs := make([]kvpb.RequestUnion, 0, numBuffered+len(ba.Requests))
	it := twb.buffer.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		reqs = append(reqs, it.Cur().toRequest())
	}

	reqs = append(reqs, ba.Requests...)
	ba.Requests = reqs

	br, pErr := twb.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}
	// Strip out responses for all the flushed buffered writes.
	br.Responses = br.Responses[numBuffered:]
	return br, nil
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

// bufferedWrite is a buffered write operation to a given key. It maps a key to
// possibly multiple values[1], each with an associated sequence number.
//
// [1] A transaction is allowed to write to a single key multiple times. Of
// this, only the final write needs to be flushed to the KV layer. However, we
// track intermediate values in the buffer to support read-your-own-writes and
// savepoint rollbacks.
type bufferedWrite struct {
	id     uint64
	key    roachpb.Key
	endKey roachpb.Key     // used in btree iteration
	vals   []bufferedValue // sorted in increasing sequence number order
}

// bufferedValue is a value written to a key at a given sequence number.
type bufferedValue struct {
	val roachpb.Value
	seq enginepb.TxnSeq
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
		putAlloc := new(struct {
			put   kvpb.PutRequest
			union kvpb.RequestUnion_Put
		})
		// TODO(arul): should we use a sync.Pool here?
		putAlloc.put.Key = bw.key
		putAlloc.put.Value = val.val
		putAlloc.put.Sequence = val.seq
		putAlloc.union.Put = &putAlloc.put
		ru.Value = &putAlloc.union
	} else {
		delAlloc := new(struct {
			del   kvpb.DeleteRequest
			union kvpb.RequestUnion_Delete
		})
		delAlloc.del.Key = bw.key
		delAlloc.del.Sequence = val.seq
		delAlloc.union.Delete = &delAlloc.del
		ru.Value = &delAlloc.union
	}
	return ru
}
