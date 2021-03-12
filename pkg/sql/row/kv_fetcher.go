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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// KVFetcher wraps kvBatchFetcher, providing a NextKV interface that returns the
// next kv from its input.
type KVFetcher struct {
	kvBatchFetcher

	kvs []roachpb.KeyValue

	batchResponse []byte
	Span          roachpb.Span
	newSpan       bool

	// Observability fields.
	mu struct {
		syncutil.Mutex
		bytesRead int64
	}
}

// NewKVFetcher creates a new KVFetcher.
// If mon is non-nil, this fetcher will track its fetches and must be Closed.
func NewKVFetcher(
	txn *kv.Txn,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	mon *mon.BytesMonitor,
	forceProductionKVBatchSize bool,
) (*KVFetcher, error) {
	kvBatchFetcher, err := makeKVBatchFetcher(
		txn, spans, reverse, useBatchLimit, firstBatchLimit, lockStrength,
		lockWaitPolicy, mon, forceProductionKVBatchSize,
	)
	return newKVFetcher(&kvBatchFetcher), err
}

func newKVFetcher(batchFetcher kvBatchFetcher) *KVFetcher {
	ret := &KVFetcher{
		kvBatchFetcher: batchFetcher,
	}
	return ret
}

// GetBytesRead returns the number of bytes read by this fetcher. It is safe for
// concurrent use and is able to handle a case of uninitialized fetcher.
func (f *KVFetcher) GetBytesRead() int64 {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.bytesRead
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
// more kvs to fetch, the kv that was fetched, and any errors that may have
// occurred.
func (f *KVFetcher) NextKV(
	ctx context.Context, mvccDecodeStrategy MVCCDecodingStrategy,
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
			var ts hlc.Timestamp
			switch mvccDecodeStrategy {
			case MVCCDecodingRequired:
				key, ts, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValue(f.batchResponse)
			case MVCCDecodingNotRequired:
				key, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValueNoTS(f.batchResponse)
			}
			if err != nil {
				return false, kv, false, err
			}
			return true, roachpb.KeyValue{
				Key: key,
				Value: roachpb.Value{
					RawBytes:  rawBytes,
					Timestamp: ts,
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
		f.mu.Lock()
		f.mu.bytesRead += int64(len(f.batchResponse))
		f.mu.Unlock()
	}
}

// Close releases the resources held by this KVFetcher. It must be called
// at the end of execution if the fetcher was provisioned with a memory
// monitor.
func (f *KVFetcher) Close(ctx context.Context) {
	f.kvBatchFetcher.close(ctx)
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

func (f *SpanKVFetcher) close(context.Context) {}
