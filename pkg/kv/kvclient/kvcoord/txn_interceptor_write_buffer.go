// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var bufferedWritesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.buffered_writes.enabled",
	"if enabled, transactional writes are buffered on the gateway",
	true,
	settings.WithPublic,
)

// txnWriteBuffer is a txnInterceptor that buffers writes for a transaction
// before sending them during commit to the wrapped lockedSender. Buffering
// writes client side has four main benefits:
//
//  1. It allows for more batching of writes, which can be more efficient.
//     Instead of sending writes one at a time, we can batch them up and send
//     them all at once. This is a win even if writes would otherwise be
//     pipelined through consensus.
//
//  2. It allows for the elimination of redundant writes. If a client writes to
//     the same key multiple times in a transaction, only the last write needs
//     to be written to the key-value layer.
//
//  3. It allows the client to serve read-your-writes locally, which can be much
//     faster and cheaper than sending them to the leaseholder. This is
//     especially true when the leaseholder is not collocated with the client.
//
//     By serving read-your-writes locally from the gateway, write buffering
//     also avoids the problem of pipeline stalls that can occur when a client
//     reads a pipelined intent write before the write has completed consensus.
//     For details on pipeline stalls, see txnPipeliner.
//
//  4. It allows clients to passively hit the 1-phase commit fast-path, instead
//     of requiring clients to carefully construct "auto-commit" BatchRequests
//     to make us of the optimization. By buffering writes on the client before
//     commit, we avoid immediately disabling the fast-path when the client
//     issues their first write. Then, at commit time, we flush the buffer and
//     will happen to hit the 1-phase commit fast path if all writes go to the
//     same range.
//
// However, buffering writes comes with some challenges.
//
// The first is that read-only and read-write requests need to be aware of the
// buffered writes, as they may need to serve reads from the buffer.
// TODO: discuss distributed execution.
//
// The second is that savepoints need to be handled correctly. TODO...
//
// The third is that the buffer needs to adhere to memory limits. TODO ...
type txnWriteBuffer struct {
	wrapped lockedSender
	enabled bool

	buf        btree
	bufSeek    bufferedWrite
	bufIDAlloc uint64
}

// SendLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if !twb.enabled {
		return twb.wrapped.SendLocked(ctx, ba)
	}

	if _, ok := ba.GetArg(kvpb.EndTxn); ok {
		return twb.flushWithEndTxn(ctx, ba)
	}

	baRemote, brLocal, localPositions, pErr := twb.servePointReadsAndAddWritesToBuffer(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}
	if len(baRemote.Requests) == 0 {
		return brLocal, nil
	}

	brRemote, pErr := twb.wrapped.SendLocked(ctx, baRemote)
	if pErr != nil {
		return nil, pErr
	}

	twb.augmentRangedReadsFromBuffer(ctx, baRemote, brRemote)
	br := twb.mergeLocalAndRemoteResponses(ctx, brLocal, brRemote, localPositions)
	return br, nil
}

// flushWithEndTxn flushes the write buffer with the EndTxn request.
func (twb *txnWriteBuffer) flushWithEndTxn(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	flushed := twb.buf.Len()
	if flushed > 0 {
		reqs := make([]kvpb.RequestUnion, 0, flushed+len(ba.Requests))
		it := twb.buf.MakeIter()
		for it.First(); it.Valid(); it.Next() {
			reqs = append(reqs, it.Cur().toRequest())
		}
		sort.SliceStable(reqs, func(i, j int) bool {
			return reqs[i].GetInner().Header().Sequence < reqs[j].GetInner().Header().Sequence
		})
		reqs = append(reqs, ba.Requests...)
		ba.Requests = reqs
	}

	br, pErr := twb.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}
	br.Responses = br.Responses[flushed:]
	return br, nil
}

// servePointReadsAndAddWritesToBuffer serves point reads from the buffer and
// adds blind writes to the buffer. It returns a BatchRequest with the locally
// serviceable requests removed, a BatchResponse with the locally serviceable
// requests' responses, and a slice of the positions of the locally serviceable
// requests in the original BatchRequest.
func (twb *txnWriteBuffer) servePointReadsAndAddWritesToBuffer(
	ctx context.Context, ba *kvpb.BatchRequest,
) (
	baRemote *kvpb.BatchRequest,
	brLocal *kvpb.BatchResponse,
	localPositions []int,
	pErr *kvpb.Error,
) {
	baRemote = ba.ShallowCopy()
	baRemote.Requests = nil
	brLocal = &kvpb.BatchResponse{}
	for i, ru := range ba.Requests {
		req := ru.GetInner()
		seek := twb.makeBufferSeekFor(req.Header())
		prevLocalLen := len(brLocal.Responses)
		switch t := req.(type) {
		case *kvpb.GetRequest:
			it := twb.buf.MakeIter()
			it.FirstOverlap(seek)
			if it.Valid() {
				var ru kvpb.ResponseUnion
				getResp := &kvpb.GetResponse{}
				if it.Cur().val.IsPresent() {
					getResp.Value = it.Cur().valPtr()
				}
				ru.MustSetInner(getResp)
				brLocal.Responses = append(brLocal.Responses, ru)
			} else {
				baRemote.Requests = append(baRemote.Requests, ru)
			}

		case *kvpb.ScanRequest:
			// Hack: just flush overlapping writes for now.
			var baFlush *kvpb.BatchRequest
			for {
				it := twb.buf.MakeIter()
				it.FirstOverlap(seek)
				if !it.Valid() {
					break
				}
				if baFlush == nil {
					baFlush = baRemote.ShallowCopy()
					baFlush.Requests = nil
					baFlush.MaxSpanRequestKeys = 0
					baFlush.TargetBytes = 0
				}
				baFlush.Requests = append(baFlush.Requests, it.Cur().toRequest())
				twb.buf.Delete(it.Cur())
			}
			if baFlush != nil {
				sort.SliceStable(baFlush.Requests, func(i, j int) bool {
					return baFlush.Requests[i].GetInner().Header().Sequence < baFlush.Requests[j].GetInner().Header().Sequence
				})
				brFlush, pErr := twb.wrapped.SendLocked(ctx, baFlush)
				if pErr != nil {
					return nil, nil, nil, pErr
				}
				baRemote.Txn.Update(brFlush.Txn)
			}
			// Send the request, then augment the response.
			baRemote.Requests = append(baRemote.Requests, ru)

		case *kvpb.ReverseScanRequest:
			// Hack: just flush overlapping writes for now.
			var baFlush *kvpb.BatchRequest
			for {
				it := twb.buf.MakeIter()
				it.FirstOverlap(seek)
				if !it.Valid() {
					break
				}
				if baFlush == nil {
					baFlush = baRemote.ShallowCopy()
					baFlush.Requests = nil
					baFlush.MaxSpanRequestKeys = 0
					baFlush.TargetBytes = 0
				}
				baFlush.Requests = append(baFlush.Requests, it.Cur().toRequest())
				twb.buf.Delete(it.Cur())
			}
			if baFlush != nil {
				sort.SliceStable(baFlush.Requests, func(i, j int) bool {
					return baFlush.Requests[i].GetInner().Header().Sequence < baFlush.Requests[j].GetInner().Header().Sequence
				})
				brFlush, pErr := twb.wrapped.SendLocked(ctx, baFlush)
				if pErr != nil {
					return nil, nil, nil, pErr
				}
				baRemote.Txn.Update(brFlush.Txn)
			}
			// Send the request, then augment the response.
			baRemote.Requests = append(baRemote.Requests, ru)

		case *kvpb.PutRequest:
			var ru kvpb.ResponseUnion
			ru.MustSetInner(&kvpb.PutResponse{})
			brLocal.Responses = append(brLocal.Responses, ru)

			twb.addToBuffer(t.Key, t.Value, t.Sequence)

		case *kvpb.DeleteRequest:
			it := twb.buf.MakeIter()
			it.FirstOverlap(seek)
			var ru kvpb.ResponseUnion
			ru.MustSetInner(&kvpb.DeleteResponse{
				// NOTE: this is incorrect. We aren't considering values that are
				// present in the KV store. This is fine for the prototype.
				FoundKey: it.Valid() && it.Cur().val.IsPresent(),
			})
			brLocal.Responses = append(brLocal.Responses, ru)

			twb.addToBuffer(t.Key, roachpb.Value{}, t.Sequence)

		case *kvpb.ConditionalPutRequest:
			it := twb.buf.MakeIter()
			it.FirstOverlap(seek)
			if it.Valid() {
				expBytes := t.ExpBytes
				existVal := it.Cur().val
				if expValPresent, existValPresent := len(expBytes) != 0, existVal.IsPresent(); expValPresent && existValPresent {
					if !bytes.Equal(expBytes, existVal.TagAndDataBytes()) {
						return nil, nil, nil, kvpb.NewError(&kvpb.ConditionFailedError{
							ActualValue: it.Cur().valPtr(),
						})
					}
				} else if expValPresent != existValPresent && (existValPresent || !t.AllowIfDoesNotExist) {
					return nil, nil, nil, kvpb.NewError(&kvpb.ConditionFailedError{
						ActualValue: it.Cur().valPtr(),
					})
				}
				var ru kvpb.ResponseUnion
				ru.MustSetInner(&kvpb.ConditionalPutResponse{})
				brLocal.Responses = append(brLocal.Responses, ru)

				twb.addToBuffer(t.Key, t.Value, t.Sequence)
			} else {
				baRemote.Requests = append(baRemote.Requests, ru)
			}

		case *kvpb.InitPutRequest:
			it := twb.buf.MakeIter()
			it.FirstOverlap(seek)
			if it.Valid() {
				failOnTombstones := t.FailOnTombstones
				existVal := it.Cur().val
				if failOnTombstones && !existVal.IsPresent() {
					// We found a tombstone and failOnTombstones is true: fail.
					return nil, nil, nil, kvpb.NewError(&kvpb.ConditionFailedError{
						ActualValue: it.Cur().valPtr(),
					})
				}
				if existVal.IsPresent() && !existVal.EqualTagAndData(t.Value) {
					// The existing value does not match the supplied value.
					return nil, nil, nil, kvpb.NewError(&kvpb.ConditionFailedError{
						ActualValue: it.Cur().valPtr(),
					})
				}
				var ru kvpb.ResponseUnion
				ru.MustSetInner(&kvpb.InitPutResponse{})
				brLocal.Responses = append(brLocal.Responses, ru)

				twb.addToBuffer(t.Key, t.Value, t.Sequence)
			} else {
				baRemote.Requests = append(baRemote.Requests, ru)
			}

		case *kvpb.IncrementRequest:
			it := twb.buf.MakeIter()
			it.FirstOverlap(seek)
			if it.Valid() {
				log.Fatalf(ctx, "unhandled buffered write overlap with increment")
			}
			baRemote.Requests = append(baRemote.Requests, ru)

		case *kvpb.DeleteRangeRequest:
			it := twb.buf.MakeIter()
			for it.FirstOverlap(seek); it.Valid(); it.NextOverlap(seek) {
				log.Fatalf(ctx, "unhandled buffered write overlap with delete range")
			}
			baRemote.Requests = append(baRemote.Requests, ru)

		default:
			log.Fatalf(ctx, "unexpected request type: %T", req)
		}
		if len(brLocal.Responses) != prevLocalLen {
			localPositions = append(localPositions, i)
		}
	}
	return baRemote, brLocal, localPositions, nil
}

// augmentRangedReadsFromBuffer augments the responses to ranged reads in the
// BatchResponse with reads from the buffer.
func (twb *txnWriteBuffer) augmentRangedReadsFromBuffer(
	ctx context.Context, baRemote *kvpb.BatchRequest, brRemote *kvpb.BatchResponse,
) {
	for _, ru := range baRemote.Requests {
		req := ru.GetInner()
		switch req.(type) {
		case *kvpb.ScanRequest, *kvpb.ReverseScanRequest:
			// Send the request, then augment the response.
			seek := twb.makeBufferSeekFor(req.Header())
			it := twb.buf.MakeIter()
			for it.FirstOverlap(seek); it.Valid(); it.NextOverlap(seek) {
				log.Fatalf(ctx, "unhandled buffered write overlap with scan / reverse scan")
			}
		}
	}
}

// mergeLocalAndRemoteResponses merges the responses to locally serviceable
// requests with the responses to remotely serviceable requests. It returns the
// merged BatchResponse.
func (twb *txnWriteBuffer) mergeLocalAndRemoteResponses(
	ctx context.Context, brLocal, brRemote *kvpb.BatchResponse, localPositions []int,
) *kvpb.BatchResponse {
	if brLocal == nil {
		return brRemote
	}
	mergedResps := make([]kvpb.ResponseUnion, len(brLocal.Responses)+len(brRemote.Responses))
	for i := range mergedResps {
		if len(localPositions) > 0 && i == localPositions[0] {
			mergedResps[i] = brLocal.Responses[0]
			brLocal.Responses = brLocal.Responses[1:]
			localPositions = localPositions[1:]
		} else {
			mergedResps[i] = brRemote.Responses[0]
			brRemote.Responses = brRemote.Responses[1:]
		}
	}
	if len(brRemote.Responses) > 0 || len(brLocal.Responses) > 0 || len(localPositions) > 0 {
		log.Fatalf(ctx, "unexpected leftover responses: %d remote, %d local, %d positions",
			len(brRemote.Responses), len(brLocal.Responses), len(localPositions))
	}
	brRemote.Responses = mergedResps
	return brRemote
}

// setWrapped implements the txnInterceptor interface.
func (twb *txnWriteBuffer) setWrapped(wrapped lockedSender) {
	twb.wrapped = wrapped
}

// populateLeafInputState implements the txnInterceptor interface.
func (twb *txnWriteBuffer) populateLeafInputState(*roachpb.LeafTxnInputState) {
	// TODO(nvanbenschoten): send buffered writes to LeafTxns.
}

// populateLeafFinalState implements the txnInterceptor interface.
func (twb *txnWriteBuffer) populateLeafFinalState(*roachpb.LeafTxnFinalState) {
	// TODO(nvanbenschoten): ingest buffered writes in LeafTxns.
}

// importLeafFinalState implements the txnInterceptor interface.
func (twb *txnWriteBuffer) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) epochBumpedLocked() {
	twb.buf.Reset()
}

// createSavepointLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) createSavepointLocked(ctx context.Context, s *savepoint) {}

// rollbackToSavepointLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) rollbackToSavepointLocked(ctx context.Context, s savepoint) {
	// TODO(nvanbenschoten): clear out writes after the savepoint. This means that
	// we need to retain multiple writes on the same key, so that we can roll one
	// back if a savepoint is rolled back. That complicates logic above, but not
	// overly so.
}

// closeLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) closeLocked() {
	twb.buf.Reset()
}

func (twb *txnWriteBuffer) makeBufferSeekFor(rh kvpb.RequestHeader) *bufferedWrite {
	seek := &twb.bufSeek
	seek.key = rh.Key
	seek.endKey = rh.EndKey
	return seek
}

func (twb *txnWriteBuffer) addToBuffer(key roachpb.Key, val roachpb.Value, seq enginepb.TxnSeq) {
	it := twb.buf.MakeIter()
	seek := &twb.bufSeek
	seek.key = key
	it.FirstOverlap(seek)
	if it.Valid() {
		// If we have a write for the same key, update it. This is incorrect, as
		// it does not handle savepoints, but it makes the prototype simpler.
		bw := it.Cur()
		bw.val = val
		bw.seq = seq
	} else {
		twb.bufIDAlloc++
		twb.buf.Set(&bufferedWrite{
			id:  twb.bufIDAlloc,
			key: key,
			val: val,
			seq: seq,
		})
	}
}

// bufferedWrite is a key-value pair with an associated sequence number.
type bufferedWrite struct {
	id     uint64
	key    roachpb.Key
	endKey roachpb.Key // used in btree iteration
	val    roachpb.Value
	seq    enginepb.TxnSeq
}

//go:generate ../../../util/interval/generic/gen.sh *bufferedWrite kvcoord

// Methods required by util/interval/generic type contract.
func (bw *bufferedWrite) ID() uint64          { return bw.id }
func (bw *bufferedWrite) Key() []byte         { return bw.key }
func (bw *bufferedWrite) EndKey() []byte      { return bw.endKey }
func (bw *bufferedWrite) String() string      { return "todo" }
func (bw *bufferedWrite) New() *bufferedWrite { return new(bufferedWrite) }
func (bw *bufferedWrite) SetID(v uint64)      { bw.id = v }
func (bw *bufferedWrite) SetKey(v []byte)     { bw.key = v }
func (bw *bufferedWrite) SetEndKey(v []byte)  { bw.endKey = v }

func (bw *bufferedWrite) toRequest() kvpb.RequestUnion {
	var ru kvpb.RequestUnion
	if bw.val.IsPresent() {
		putAlloc := new(struct {
			put   kvpb.PutRequest
			union kvpb.RequestUnion_Put
		})
		putAlloc.put.Key = bw.key
		putAlloc.put.Value = bw.val
		putAlloc.put.Sequence = bw.seq
		putAlloc.union.Put = &putAlloc.put
		ru.Value = &putAlloc.union
	} else {
		delAlloc := new(struct {
			del   kvpb.DeleteRequest
			union kvpb.RequestUnion_Delete
		})
		delAlloc.del.Key = bw.key
		delAlloc.del.Sequence = bw.seq
		delAlloc.union.Delete = &delAlloc.del
		ru.Value = &delAlloc.union
	}
	return ru
}

func (bw *bufferedWrite) valPtr() *roachpb.Value {
	valCpy := bw.val
	return &valCpy
}
