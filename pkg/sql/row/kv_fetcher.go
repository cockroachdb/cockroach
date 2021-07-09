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
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// KVFetcher wraps kvBatchFetcher, providing a NextKV interface that returns the
// next kv from its input.
type KVFetcher struct {
	kvBatchFetcher

	kvs []roachpb.KeyValue

	batchResponse []byte
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
// finalReferenceToBatch is set to true if the returned KV's byte slices are
// the last reference into a larger backing byte slice. This parameter allows
// calling code to control its memory usage: if finalReferenceToBatch is true,
// it means that the next call to NextKV might potentially allocate a big chunk
// of new memory, so the returned KeyValue should be copied into a small slice
// that the caller owns to avoid retaining two large backing byte slices at once
// unexpectedly.
func (f *KVFetcher) NextKV(
	ctx context.Context, mvccDecodeStrategy MVCCDecodingStrategy,
) (moreKVs bool, kv roachpb.KeyValue, finalReferenceToBatch bool, err error) {
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
			return true, kv, false, nil
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
			}, lastKey, nil
		}

		moreKVs, f.kvs, f.batchResponse, err = f.nextBatch(ctx)
		if err != nil {
			return moreKVs, kv, false, err
		}
		if !moreKVs {
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
	ctx context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, err error) {
	if len(f.KVs) == 0 {
		return false, nil, nil, nil
	}
	res := f.KVs
	f.KVs = nil
	return true, res, nil, nil
}

func (f *SpanKVFetcher) close(context.Context) {}

// BackupSSTKVFetcher is a kvBatchFetcher that wraps storage.SimpleMVCCIterator
// and returns a batch of kv from backupSST.
type BackupSSTKVFetcher struct {
	iter          storage.SimpleMVCCIterator
	endKeyMVCC    storage.MVCCKey
	startTime     hlc.Timestamp
	endTime       hlc.Timestamp
	withRevisions bool
}

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

func (f *BackupSSTKVFetcher) nextBatch(
	ctx context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, err error) {
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
			return false, nil, nil, err
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
		return false, nil, nil, err
	}
	return true, res, nil, nil
}

func (f *BackupSSTKVFetcher) close(context.Context) {
	f.iter.Close()
}
