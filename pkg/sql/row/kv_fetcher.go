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
	"github.com/cockroachdb/errors"
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
}

// NewKVFetcher creates a new KVFetcher.
func NewKVFetcher(
	txn *kv.Txn,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	lockStr sqlbase.ScanLockingStrength,
	returnRangeInfo bool,
) (*KVFetcher, error) {
	kvBatchFetcher, err := makeKVBatchFetcher(
		txn, spans, reverse, useBatchLimit, firstBatchLimit, lockStr, returnRangeInfo,
	)
	return newKVFetcher(&kvBatchFetcher), err
}

func newKVFetcher(batchFetcher kvBatchFetcher) *KVFetcher {
	return &KVFetcher{
		kvBatchFetcher: batchFetcher,
	}
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

		ok, f.kvs, f.batchResponse, f.Span, err = f.nextBatch(ctx)
		if err != nil {
			return ok, kv, false, err
		}
		if !ok {
			return false, kv, false, nil
		}
		f.newSpan = true
		f.bytesRead += int64(len(f.batchResponse))
	}
}

// SpanKVFetcher is a kvBatchFetcher that returns a set slice of kvs.
type SpanKVFetcher struct {
	KVs []roachpb.KeyValue
}

// nextBatch implements the kvBatchFetcher interface.
func (f *SpanKVFetcher) nextBatch(
	_ context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, span roachpb.Span, err error) {
	if len(f.KVs) == 0 {
		return false, nil, nil, roachpb.Span{}, nil
	}
	res := f.KVs
	f.KVs = nil
	return true, res, nil, roachpb.Span{}, nil
}

// GetRangesInfo implements the kvBatchFetcher interface.
func (f *SpanKVFetcher) GetRangesInfo() []roachpb.RangeInfo {
	panic(errors.AssertionFailedf("GetRangesInfo() called on SpanKVFetcher"))
}
