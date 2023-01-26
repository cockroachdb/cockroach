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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// KVFetcher wraps KVBatchFetcher, providing a NextKV interface that returns the
// next kv from its input.
type KVFetcher struct {
	KVBatchFetcher

	kv  roachpb.KeyValue
	kvs []roachpb.KeyValue

	batchResponse []byte
	spanID        int
}

var _ storage.NextKVer = &KVFetcher{}

// newTxnKVFetcher creates a new txnKVFetcher.
//
// If acc is non-nil, this fetcher will track its fetches and must be Closed.
// The fetcher only grows and shrinks the account according to its own use, so
// the memory account can be shared by the caller with other components (as long
// as there is no concurrency).
func newTxnKVFetcher(
	txn *kv.Txn,
	bsHeader *roachpb.BoundedStalenessHeader,
	reverse bool,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	lockTimeout time.Duration,
	acc *mon.BoundAccount,
	forceProductionKVBatchSize bool,
) *txnKVFetcher {
	var sendFn sendFunc
	var batchRequestsIssued int64
	// Avoid the heap allocation by allocating sendFn specifically in the if.
	if bsHeader == nil {
		sendFn = makeTxnKVFetcherDefaultSendFunc(txn, &batchRequestsIssued)
	} else {
		negotiated := false
		sendFn = func(ctx context.Context, ba *roachpb.BatchRequest) (br *roachpb.BatchResponse, _ error) {
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
			batchRequestsIssued++
			return br, nil
		}
	}

	fetcherArgs := newTxnKVFetcherArgs{
		sendFn:                     sendFn,
		reverse:                    reverse,
		lockStrength:               lockStrength,
		lockWaitPolicy:             lockWaitPolicy,
		lockTimeout:                lockTimeout,
		acc:                        acc,
		forceProductionKVBatchSize: forceProductionKVBatchSize,
		batchRequestsIssued:        &batchRequestsIssued,
	}
	if txn != nil {
		// In most cases, the txn is non-nil; however, in some code paths (e.g.
		// when executing EXPLAIN (VEC)) it might be nil, so we need to have
		// this check.
		fetcherArgs.requestAdmissionHeader = txn.AdmissionHeader()
		fetcherArgs.responseAdmissionQ = txn.DB().SQLKVResponseAdmissionQ
	}
	return newTxnKVFetcherInternal(fetcherArgs)
}

// NewDirectKVBatchFetcher creates a new KVBatchFetcher that uses the
// COL_BATCH_RESPONSE scan format for Scans (or ReverseScans, if reverse is
// true).
//
// If acc is non-nil, this fetcher will track its fetches and must be Closed.
// The fetcher only grows and shrinks the account according to its own use, so
// the memory account can be shared by the caller with other components (as long
// as there is no concurrency).
func NewDirectKVBatchFetcher(
	txn *kv.Txn,
	bsHeader *roachpb.BoundedStalenessHeader,
	spec *fetchpb.IndexFetchSpec,
	reverse bool,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	lockTimeout time.Duration,
	acc *mon.BoundAccount,
	forceProductionKVBatchSize bool,
) KVBatchFetcher {
	f := newTxnKVFetcher(
		txn, bsHeader, reverse, lockStrength, lockWaitPolicy,
		lockTimeout, acc, forceProductionKVBatchSize,
	)
	f.scanFormat = roachpb.COL_BATCH_RESPONSE
	f.indexFetchSpec = spec
	return f
}

// NewKVFetcher creates a new KVFetcher.
//
// If acc is non-nil, this fetcher will track its fetches and must be Closed.
// The fetcher only grows and shrinks the account according to its own use, so
// the memory account can be shared by the caller with other components (as long
// as there is no concurrency).
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
	return newKVFetcher(newTxnKVFetcher(
		txn, bsHeader, reverse, lockStrength, lockWaitPolicy,
		lockTimeout, acc, forceProductionKVBatchSize,
	))
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
	kvFetcherMemAcc *mon.BoundAccount,
) *KVFetcher {
	var batchRequestsIssued int64
	streamer := kvstreamer.NewStreamer(
		distSender,
		stopper,
		txn,
		st,
		getWaitPolicy(lockWaitPolicy),
		streamerBudgetLimit,
		streamerBudgetAcc,
		&batchRequestsIssued,
		GetKeyLockingStrength(lockStrength),
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
	return newKVFetcher(newTxnKVStreamer(streamer, lockStrength, kvFetcherMemAcc, &batchRequestsIssued))
}

func newKVFetcher(batchFetcher KVBatchFetcher) *KVFetcher {
	return &KVFetcher{KVBatchFetcher: batchFetcher}
}

// nextKV returns the next kv from this fetcher. Returns false if there are no
// more kvs to fetch, the kv that was fetched, the ID associated with the span
// that generated this kv (0 if nil spanIDs were provided when constructing the
// fetcher), and any errors that may have occurred.
//
// The returned kv is stable meaning that it will not be invalidated on the
// following nextKV call.
func (f *KVFetcher) nextKV(
	ctx context.Context, mvccDecodeStrategy storage.MVCCDecodingStrategy,
) (ok bool, kv roachpb.KeyValue, spanID int, err error) {
	for {
		// Only one of f.kvs or f.batchResponse will be set at a given time. Which
		// one is set depends on the format returned by a given BatchRequest.
		nKvs := len(f.kvs)
		if nKvs != 0 {
			kv = f.kvs[0]
			f.kvs = f.kvs[1:]
			// We always return "false" for needsCopy when returning data in the
			// KV format, because each of the KVs doesn't share any backing memory -
			// they are all independently garbage collectable.
			return true, kv, f.spanID, nil
		}
		if len(f.batchResponse) > 0 {
			var key []byte
			var rawBytes []byte
			var err error
			var ts hlc.Timestamp
			switch mvccDecodeStrategy {
			case storage.MVCCDecodingRequired:
				key, ts, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValue(f.batchResponse)
			case storage.MVCCDecodingNotRequired:
				key, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValueNoTS(f.batchResponse)
			}
			if err != nil {
				return false, kv, 0, err
			}
			key = key[:len(key):len(key)]
			rawBytes = rawBytes[:len(rawBytes):len(rawBytes)]
			// By default, use the references to the key and the value directly.
			f.kv = roachpb.KeyValue{
				Key: key,
				Value: roachpb.Value{
					RawBytes:  rawBytes,
					Timestamp: ts,
				},
			}
			// If we're finished decoding the batch response, nil our reference to it
			// so that the garbage collector can reclaim the backing memory.
			lastKey := len(f.batchResponse) == 0
			if lastKey {
				f.batchResponse = nil
				// If we've made it to the very last key in the batch, copy out
				// the key so that the GC can reclaim the large backing slice
				// before nextKV() is called again.
				f.kv.Key = make(roachpb.Key, len(key))
				copy(f.kv.Key, key)
				f.kv.Value.RawBytes = make([]byte, len(rawBytes))
				copy(f.kv.Value.RawBytes, rawBytes)
			}
			return true, f.kv, f.spanID, nil
		}

		resp, err := f.NextBatch(ctx)
		if err != nil || !resp.MoreKVs {
			return resp.MoreKVs, roachpb.KeyValue{}, 0, err
		}
		f.kvs = resp.KVs
		f.batchResponse = resp.BatchResponse
		f.spanID = resp.spanID
	}
}

// Init implements the storage.NextKVer interface.
func (f *KVFetcher) Init(storage.FirstKeyOfRowGetter) (stableKVs bool) {
	// nextKV never invalidates the returned kv, so it is always stable.
	return true
}

// NextKV implements the storage.NextKVer interface.
// gcassert:inline
func (f *KVFetcher) NextKV(
	ctx context.Context, mvccDecodeStrategy storage.MVCCDecodingStrategy,
) (ok bool, partialRow bool, kv roachpb.KeyValue, err error) {
	ok, kv, _, err = f.nextKV(ctx, mvccDecodeStrategy)
	// nextKV never splits rows.
	//
	// Generally speaking, this is _not_ achieved via the WholeRowsOfSize option
	// (although that option is used the txnKVStreamer). Instead, if one
	// BatchResponse stops in the middle of a SQL row, then a follow-up
	// BatchRequest is issued with the corresponding ResumeSpan, so nextKV()
	// provides a stream of KVs that never stops in the middle of a SQL row.
	partialRow = false
	return ok, partialRow, kv, err
}

// SetupNextFetch overrides the same method from the wrapped KVBatchFetcher in
// order to reset this KVFetcher.
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

// KVProvider is a KVBatchFetcher that returns a set slice of kvs.
type KVProvider struct {
	KVs []roachpb.KeyValue
}

var _ KVBatchFetcher = &KVProvider{}

// NextBatch implements the KVBatchFetcher interface.
func (f *KVProvider) NextBatch(context.Context) (KVBatchFetcherResponse, error) {
	if len(f.KVs) == 0 {
		return KVBatchFetcherResponse{MoreKVs: false}, nil
	}
	res := f.KVs
	f.KVs = nil
	return KVBatchFetcherResponse{
		MoreKVs: true,
		KVs:     res,
	}, nil
}

// SetupNextFetch implements the KVBatchFetcher interface.
func (f *KVProvider) SetupNextFetch(
	context.Context, roachpb.Spans, []int, rowinfra.BytesLimit, rowinfra.KeyLimit,
) error {
	return nil
}

// GetBytesRead implements the KVBatchFetcher interface.
func (f *KVProvider) GetBytesRead() int64 {
	return 0
}

// GetBatchRequestsIssued implements the KVBatchFetcher interface.
func (f *KVProvider) GetBatchRequestsIssued() int64 {
	return 0
}

// Close implements the KVBatchFetcher interface.
func (f *KVProvider) Close(context.Context) {}
