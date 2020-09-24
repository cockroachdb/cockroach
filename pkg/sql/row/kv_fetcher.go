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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// KVFetcher wraps kvBatchFetcher, providing a NextKV interface that returns the
// next kv from its input.
type KVFetcher struct {
	kvBatchFetcher

	kvs []roachpb.KeyValue

	batchResponse []byte
	bytesRead     int64
	Span          roachpb.Span
	newSpan       bool
	acc           mon.BoundAccount
}

// NewKVFetcher creates a new KVFetcher.
// If mon is non-nil, this fetcher will track its fetches and must be Closed.
func NewKVFetcher(
	txn *kv.Txn,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	lockStr sqlbase.ScanLockingStrength,
	returnRangeInfo bool,
	mon *mon.BytesMonitor,
) (*KVFetcher, error) {
	kvBatchFetcher, err := makeKVBatchFetcher(
		txn, spans, reverse, useBatchLimit, firstBatchLimit, lockStr, returnRangeInfo, mon,
	)
	return newKVFetcher(&kvBatchFetcher, mon), err
}

func newKVFetcher(batchFetcher kvBatchFetcher, mon *mon.BytesMonitor) *KVFetcher {
	ret := &KVFetcher{
		kvBatchFetcher: batchFetcher,
	}
	if mon != nil {
		ret.acc = mon.MakeBoundAccount()
	}
	return ret
}

// NextKV returns the next kv from this fetcher. Returns false if there are no
// more kvs to fetch, the kv that was fetched, and any errors that may have
// occurred.
func (f *KVFetcher) NextKV(
	ctx context.Context,
) (ok bool, kv roachpb.KeyValue, newSpan bool, err error) {
	for {
		newSpan = f.newSpan
		f.newSpan = false
		if len(f.kvs) != 0 {
			kv = f.kvs[0]
			f.kvs = f.kvs[1:]
			return true, kv, newSpan, nil
		}
		if len(f.batchResponse) > 0 {
			var key []byte
			var rawBytes []byte
			var err error
			key, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValueNoTS(f.batchResponse)
			if err != nil {
				return false, kv, false, err
			}
			return true, roachpb.KeyValue{
				Key: key,
				Value: roachpb.Value{
					RawBytes: rawBytes,
				},
			}, newSpan, nil
		}

		monitoring := f.acc.Monitor() != nil

		const tokenFetchAllocation = 1 << 10
		if monitoring && f.acc.Used() < tokenFetchAllocation {
			// Pre-reserve a token fraction of the maximum amount of memory this scan
			// could return. Most of the time, scans won't use this amount of memory,
			// so it's unnecessary to reserve it all. We reserve something rather than
			// nothing at all to preserve some accounting.
			if err := f.acc.ResizeTo(ctx, tokenFetchAllocation); err != nil {
				return ok, kv, false, err
			}
		}
		ok, f.kvs, f.batchResponse, f.Span, err = f.nextBatch(ctx)
		if err != nil {
			return ok, kv, false, err
		}
		returnedBytes := int64(len(f.batchResponse))
		if monitoring && returnedBytes > f.acc.Used() {
			// Resize up to the actual amount of bytes we got back from the fetch,
			// but don't ratchet down. We would much prefer to over-account, and the
			// worst we can over-account by is around 10 MB, the maximum fetch size.
			//
			// The reason we don't want to precisely account here is to hopefully
			// protect ourselves from "slop" in our memory handling. In general, we
			// expect that all SQL operators that buffer data for longer than a single
			// call to Next do their own accounting, so theoretically, by the time
			// this fetch method is called again, all memory will either be released
			// from the system or accounted for elsewhere. In reality, though, Go's
			// garbage collector has some lag between when the memory is no longer
			// referenced and when it is freed. Also, we're not perfect with
			// accounting by any means. When we start doing large fetches, it's more
			// likely that we'll expose ourselves to OOM conditions, so that's the
			// reasoning for why we never ratchet this account down - only up, toward
			// the maximum fetch size (maxScanResponseBytes).
			if err := f.acc.ResizeTo(ctx, returnedBytes); err != nil {
				return ok, kv, false, err
			}
		}
		if !ok {
			return false, kv, false, nil
		}
		f.newSpan = true
		f.bytesRead += returnedBytes
	}
}

// Close releases the resources held by this KVFetcher. It must be called
// at the end of execution if the fetcher was provisioned with a memory
// monitor.
func (f *KVFetcher) Close(ctx context.Context) {
	f.acc.Close(ctx)
}
