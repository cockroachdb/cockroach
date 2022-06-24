// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// KVFetcher wraps KVBatchFetcher, providing a NextKV interface that returns the
// next kv from its input.
type KVFetcher struct {
	KVBatchFetcher

	kvs []roachpb.KeyValue

	batchResponse []byte
	spanID        int

	// Observability fields.
	// Note: these need to be read via an atomic op.
	atomics struct {
		bytesRead int64
	}
}

// NewKVFetcher creates a new KVFetcher.
// If acc is non-nil, this fetcher will track its fetches and must be Closed.
//
// The fetcher takes ownership of the spans slice - it can modify the slice and
// will perform the memory accounting accordingly (if acc is non-nil). The
// caller can only reuse the spans slice after the fetcher has been closed, and
// if the caller does, it becomes responsible for the memory accounting.
//
// The fetcher also takes ownership of the spanIDs slice - it can modify the
// slice, but it will **not** perform the memory accounting. It is the caller's
// responsibility to track the memory under the spanIDs slice, and the slice
// can only be reused once the fetcher has been closed. Notably, the capacity of
// the slice will not be increased by the fetcher.
//
// If spanIDs is non-nil, then it must be of the same length as spans.
func NewKVFetcher(
	txn *kv.Txn,
	bsHeader *roachpb.BoundedStalenessHeader,
	reverse bool,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	lockTimeout time.Duration,
	acc *mon.BoundAccount,
	forceProductionKVBatchSize bool,
) *KVFetcher {
	var sendFn sendFunc
	// Avoid the heap allocation by allocating sendFn specifically in the if.
	if bsHeader == nil {
		sendFn = makeKVBatchFetcherDefaultSendFunc(txn)
	} else {
		negotiated := false
		sendFn = func(ctx context.Context, ba roachpb.BatchRequest) (br *roachpb.BatchResponse, _ error) {
			ba.RoutingPolicy = roachpb.RoutingPolicy_NEAREST
			var pErr *roachpb.Error
			// Only use NegotiateAndSend if we have not yet negotiated a timestamp.
			// If we have, fallback to Send which will already have the timestamp
			// fixed.
			if !negotiated {
				ba.BoundedStaleness = bsHeader
				br, pErr = txn.NegotiateAndSend(ctx, ba)
				negotiated = true
			} else {
				br, pErr = txn.Send(ctx, ba)
			}
			if pErr != nil {
				return nil, pErr.GoError()
			}
			return br, nil
		}
	}

	fetcherArgs := kvBatchFetcherArgs{
		sendFn:                     sendFn,
		reverse:                    reverse,
		lockStrength:               lockStrength,
		lockWaitPolicy:             lockWaitPolicy,
		lockTimeout:                lockTimeout,
		acc:                        acc,
		forceProductionKVBatchSize: forceProductionKVBatchSize,
	}
	if txn != nil {
		// In most cases, the txn is non-nil; however, in some code paths (e.g.
		// when executing EXPLAIN (VEC)) it might be nil, so we need to have
		// this check.
		fetcherArgs.requestAdmissionHeader = txn.AdmissionHeader()
		fetcherArgs.responseAdmissionQ = txn.DB().SQLKVResponseAdmissionQ
	}
	return newKVFetcher(newKVBatchFetcher(fetcherArgs))
}

// NewStreamingKVFetcher returns a new KVFetcher that utilizes the provided
// kvstreamer.Streamer to perform KV reads.
//
// If maintainOrdering is true, then diskBuffer must be non-nil.
func NewStreamingKVFetcher(
	distSender *kvcoord.DistSender,
	stopper *stop.Stopper,
	txn *kv.Txn,
	st *cluster.Settings,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	lockStrength descpb.ScanLockingStrength,
	streamerBudgetLimit int64,
	streamerBudgetAcc *mon.BoundAccount,
	maintainOrdering bool,
	singleRowLookup bool,
	maxKeysPerRow int,
	diskBuffer kvstreamer.ResultDiskBuffer,
) *KVFetcher {
	streamer := kvstreamer.NewStreamer(
		distSender,
		stopper,
		txn,
		st,
		getWaitPolicy(lockWaitPolicy),
		streamerBudgetLimit,
		streamerBudgetAcc,
	)
	mode := kvstreamer.OutOfOrder
	if maintainOrdering {
		mode = kvstreamer.InOrder
	}
	streamer.Init(
		mode,
		kvstreamer.Hints{
			UniqueRequests:  true,
			SingleRowLookup: singleRowLookup,
		},
		maxKeysPerRow,
		diskBuffer,
	)
	return newKVFetcher(newTxnKVStreamer(streamer, lockStrength))
}

func newKVFetcher(batchFetcher KVBatchFetcher) *KVFetcher {
	return &KVFetcher{
		KVBatchFetcher: batchFetcher,
	}
}

// GetBytesRead returns the number of bytes read by this fetcher. It is safe for
// concurrent use and is able to handle a case of uninitialized fetcher.
func (f *KVFetcher) GetBytesRead() int64 {
	if f == nil {
		return 0
	}
	return atomic.LoadInt64(&f.atomics.bytesRead)
}

// ResetBytesRead resets the number of bytes read by this fetcher and returns
// the number before the reset. It is safe for concurrent use and is able to
// handle a case of uninitialized fetcher.
func (f *KVFetcher) ResetBytesRead() int64 {
	if f == nil {
		return 0
	}
	return atomic.SwapInt64(&f.atomics.bytesRead, 0)
}

// MVCCDecodingStrategy controls if and how the fetcher should decode MVCC
// timestamps from returned KV's.
type MVCCDecodingStrategy int

const (
	// MVCCDecodingNotRequired is used when timestamps aren't needed.
	MVCCDecodingNotRequired MVCCDecodingStrategy = iota
	// MVCCDecodingRequired is used when timestamps are needed.
	MVCCDecodingRequired
)

// NextKV returns the next kv from this fetcher. Returns false if there are no
// more kvs to fetch, the kv that was fetched, the ID associated with the span
// that generated this kv (0 if nil spanIDs were provided when constructing the
// fetcher), and any errors that may have occurred.
//
// finalReferenceToBatch is set to true if the returned KV's byte slices are
// the last reference into a larger backing byte slice. This parameter allows
// calling code to control its memory usage: if finalReferenceToBatch is true,
// it means that the next call to NextKV might potentially allocate a big chunk
// of new memory, so the returned KeyValue should be copied into a small slice
// that the caller owns to avoid retaining two large backing byte slices at once
// unexpectedly.
func (f *KVFetcher) NextKV(
	ctx context.Context, mvccDecodeStrategy MVCCDecodingStrategy,
) (ok bool, kv roachpb.KeyValue, spanID int, finalReferenceToBatch bool, err error) {
	for {
		// Only one of f.kvs or f.batchResponse will be set at a given time. Which
		// one is set depends on the format returned by a given BatchRequest.
		nKvs := len(f.kvs)
		if nKvs != 0 {
			kv = f.kvs[0]
			f.kvs = f.kvs[1:]
			// We always return "false" for finalReferenceToBatch when returning data in the
			// KV format, because each of the KVs doesn't share any backing memory -
			// they are all independently garbage collectable.
			return true, kv, f.spanID, false, nil
		}
		if len(f.batchResponse) > 0 {
			var key []byte
			var rawBytes []byte
			var err error
			var ts hlc.Timestamp
			switch mvccDecodeStrategy {
			case MVCCDecodingRequired:
				key, ts, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValue(f.batchResponse)
			case MVCCDecodingNotRequired:
				key, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValueNoTS(f.batchResponse)
			}
			if err != nil {
				return false, kv, 0, false, err
			}
			// If we're finished decoding the batch response, nil our reference to it
			// so that the garbage collector can reclaim the backing memory.
			lastKey := len(f.batchResponse) == 0
			if lastKey {
				f.batchResponse = nil
			}
			return true, roachpb.KeyValue{
				Key: key[:len(key):len(key)],
				Value: roachpb.Value{
					RawBytes:  rawBytes[:len(rawBytes):len(rawBytes)],
					Timestamp: ts,
				},
			}, f.spanID, lastKey, nil
		}

		resp, err := f.nextBatch(ctx)
		if err != nil || !resp.moreKVs {
			return resp.moreKVs, roachpb.KeyValue{}, 0, false, err
		}
		f.kvs = resp.kvs
		f.batchResponse = resp.batchResponse
		f.spanID = resp.spanID
		nBytes := len(f.batchResponse)
		for i := range f.kvs {
			nBytes += len(f.kvs[i].Key)
			nBytes += len(f.kvs[i].Value.RawBytes)
		}
		atomic.AddInt64(&f.atomics.bytesRead, int64(nBytes))
	}
}

// SetupNextFetch overrides the same method from the wrapped KVBatchFetcher in
// order to reset this KVFetcher.
func (f *KVFetcher) SetupNextFetch(
	ctx context.Context,
	spans roachpb.Spans,
	spanIDs []int,
	batchBytesLimit rowinfra.BytesLimit,
	firstBatchKeyLimit rowinfra.KeyLimit,
) error {
	f.kvs = nil
	f.batchResponse = nil
	f.spanID = 0
	return f.KVBatchFetcher.SetupNextFetch(
		ctx, spans, spanIDs, batchBytesLimit, firstBatchKeyLimit,
	)
}

// Close releases the resources held by this KVFetcher. It must be called
// at the end of execution if the fetcher was provisioned with a memory
// monitor.
func (f *KVFetcher) Close(ctx context.Context) {
	f.KVBatchFetcher.close(ctx)
}

// SpanKVFetcher is a KVBatchFetcher that returns a set slice of kvs.
type SpanKVFetcher struct {
	KVs []roachpb.KeyValue
}

var _ KVBatchFetcher = &SpanKVFetcher{}

// nextBatch implements the KVBatchFetcher interface.
func (f *SpanKVFetcher) nextBatch(ctx context.Context) (kvBatchFetcherResponse, error) {
	if len(f.KVs) == 0 {
		return kvBatchFetcherResponse{moreKVs: false}, nil
	}
	res := f.KVs
	f.KVs = nil
	return kvBatchFetcherResponse{
		moreKVs: true,
		kvs:     res,
	}, nil
}

// SetupNextFetch implements the KVBatchFetcher interface.
func (f *SpanKVFetcher) SetupNextFetch(
	context.Context, roachpb.Spans, []int, rowinfra.BytesLimit, rowinfra.KeyLimit,
) error {
	return nil
}

func (f *SpanKVFetcher) close(context.Context) {}

// BackupSSTKVFetcher is a KVBatchFetcher that wraps storage.SimpleMVCCIterator
// and returns a batch of kv from backupSST.
type BackupSSTKVFetcher struct {
	iter          storage.SimpleMVCCIterator
	endKeyMVCC    storage.MVCCKey
	startTime     hlc.Timestamp
	endTime       hlc.Timestamp
	withRevisions bool
}

var _ KVBatchFetcher = &BackupSSTKVFetcher{}

// MakeBackupSSTKVFetcher creates a BackupSSTKVFetcher and
// advances the iter to the first key >= startKeyMVCC
func MakeBackupSSTKVFetcher(
	startKeyMVCC, endKeyMVCC storage.MVCCKey,
	iter storage.SimpleMVCCIterator,
	startTime hlc.Timestamp,
	endTime hlc.Timestamp,
	withRev bool,
) BackupSSTKVFetcher {
	res := BackupSSTKVFetcher{
		iter,
		endKeyMVCC,
		startTime,
		endTime,
		withRev,
	}
	res.iter.SeekGE(startKeyMVCC)
	return res
}

func (f *BackupSSTKVFetcher) nextBatch(ctx context.Context) (kvBatchFetcherResponse, error) {
	res := make([]roachpb.KeyValue, 0)

	copyKV := func(mvccKey storage.MVCCKey, value []byte) roachpb.KeyValue {
		keyCopy := make([]byte, len(mvccKey.Key))
		copy(keyCopy, mvccKey.Key)
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		return roachpb.KeyValue{
			Key:   keyCopy,
			Value: roachpb.Value{RawBytes: valueCopy, Timestamp: mvccKey.Timestamp},
		}
	}

	for {
		valid, err := f.iter.Valid()
		if err != nil {
			err = errors.Wrapf(err, "iter key value of table data")
			return kvBatchFetcherResponse{}, err
		}

		if !valid || !f.iter.UnsafeKey().Less(f.endKeyMVCC) {
			break
		}

		if !f.endTime.IsEmpty() {
			if f.endTime.Less(f.iter.UnsafeKey().Timestamp) {
				f.iter.Next()
				continue
			}
		}

		if f.withRevisions {
			if f.iter.UnsafeKey().Timestamp.Less(f.startTime) {
				f.iter.NextKey()
				continue
			}
		} else {
			if len(f.iter.UnsafeValue()) == 0 {
				if f.endTime.IsEmpty() || f.iter.UnsafeKey().Timestamp.Less(f.endTime) {
					// Value is deleted at endTime.
					f.iter.NextKey()
					continue
				} else {
					// Otherwise we call Next to trace back the correct revision.
					f.iter.Next()
					continue
				}
			}
		}

		res = append(res, copyKV(f.iter.UnsafeKey(), f.iter.UnsafeValue()))

		if f.withRevisions {
			f.iter.Next()
		} else {
			f.iter.NextKey()
		}

	}
	if len(res) == 0 {
		return kvBatchFetcherResponse{moreKVs: false}, nil
	}
	return kvBatchFetcherResponse{
		moreKVs: true,
		kvs:     res,
	}, nil
}

// SetupNextFetch implements the KVBatchFetcher interface.
func (f *BackupSSTKVFetcher) SetupNextFetch(
	context.Context, roachpb.Spans, []int, rowinfra.BytesLimit, rowinfra.KeyLimit,
) error {
	return nil
}

func (f *BackupSSTKVFetcher) close(context.Context) {
	f.iter.Close()
}
