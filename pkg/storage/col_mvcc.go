// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// CFetcherWrapper is a wrapper around a CFetcher that returns a serialized set
// of bytes, a column-oriented batch.
type CFetcherWrapper interface {
	// NextBatch gives back the next column-oriented batch.
	NextBatch(ctx context.Context) ([]byte, error)
}

// NextKVer can fetch a new KV from somewhere. If MVCCDecodingStrategy is set
// to required, the returned KV will include a timestamp.
type NextKVer interface {
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
	NextKV(
		ctx context.Context, strategy MVCCDecodingStrategy,
	) (
		moreKVs bool, kv roachpb.KeyValue, finalReferenceToBatch bool, err error,
	)
	// GetBytesRead returns the number of bytes read by this fetcher. It is safe for
	// concurrent use and is able to handle a case of uninitialized fetcher.
	GetBytesRead() int64
	// Close releases the resources held by this NextKVer.
	Close(ctx context.Context)
}

// GetCFetcherWrapper returns a CFetcherWrapper. It's injected from
// pkg/sql/colfetcher to avoid circular dependencies, since storage can't depend
// on higher levels of the system.
var GetCFetcherWrapper func(
	ctx context.Context,
	acc *mon.BoundAccount,
	codec keys.SQLCodec,
	specMessage proto.Message,
	nextKVer NextKVer,
) (CFetcherWrapper, error)

// mvccScanFetchAdapter is a NextKVer that is implemented directly by a
// pebbleMVCCScanner. Each time its NextKV is called, it iterates the pebble
// scanner and returns the result. Note that the returned KV is only valid until
// the next call to NextKV.
type mvccScanFetchAdapter struct {
	scanner    *pebbleMVCCScanner
	results    singleResults
	noMoreKeys bool

	// needToAdvanceBeforeGet is true if the last time we called NextKV, we only
	// retrieved the current value of the pebble iterator, without advancing it,
	// and as a result we'll need to advance the iterator next time we call NextKV.
	needToAdvanceBeforeGet bool
}

var _ NextKVer = &mvccScanFetchAdapter{}

// Close implements the NextKVer interface.
func (f *mvccScanFetchAdapter) Close(ctx context.Context) {}

// GetBytesRead implements the NextKVer interface.
func (f *mvccScanFetchAdapter) GetBytesRead() int64 {
	return f.scanner.results.getBytes()
}

// NextKV implements the NextKVer interface.
func (f *mvccScanFetchAdapter) NextKV(
	ctx context.Context, mvccDecodeStrategy MVCCDecodingStrategy,
) (ok bool, kv roachpb.KeyValue, finalReferenceToBatch bool, err error) {
	if f.needToAdvanceBeforeGet {
		if !f.scanner.advanceKey() {
			// No more keys in the scan.
			return false, roachpb.KeyValue{}, false, nil
		}
	}
	// Set the scanner to "get" mode, which prevents it from advancing after it's
	// finished retrieving the current key. This way, we have a chance to copy
	// the current key into our columnar batch before the scanner advances.
	f.scanner.isGet = true
	if !f.scanner.getAndAdvance(ctx) && f.scanner.resumeReason != roachpb.RESUME_UNKNOWN {
		// We hit our scan limit of some sort.
		return false, roachpb.KeyValue{}, false, nil
	}
	f.scanner.isGet = false
	// Make sure we will advance the iterator before getting a key next time
	// NextKV is called.
	f.needToAdvanceBeforeGet = true
	lastKV := f.results.getLastKV()

	enc := lastKV.Key
	switch mvccDecodeStrategy {
	case MVCCDecodingRequired:
		lastKV.Key, lastKV.Value.Timestamp, err = enginepb.DecodeKey(enc)
		if err != nil {
			return false, lastKV, false, err
		}
	case MVCCDecodingNotRequired:
		lastKV.Key, _, ok = enginepb.SplitMVCCKey(enc)
		if !ok {
			return false, lastKV, false, errors.Errorf("invalid encoded mvcc key: %x", enc)
		}
	}
	// We always need to return true for finalReferenceToBatch, because each KV
	// that we return will be instantly overwritten next time we advance the
	// Pebble iterator.
	return true, lastKV, true, nil
}

// MVCCScanToCols is like MVCCScan, but it returns KVData in a serialized
// columnar batch suitable for reading by RecordBatchDeserializer.
func MVCCScanToCols(
	ctx context.Context,
	tenantID uint64,
	reader Reader,
	tableReaderSpec proto.Message,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	iter := reader.NewMVCCIterator(MVCCKeyIterKind, IterOptions{LowerBound: key, UpperBound: endKey})
	defer iter.Close()
	return mvccScanToCols(ctx, tenantID, iter, tableReaderSpec, key, endKey, timestamp, opts)
}

func mvccScanToCols(
	ctx context.Context,
	tenantID uint64,
	iter MVCCIterator,
	tableReaderSpec proto.Message,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	if len(endKey) == 0 {
		return MVCCScanResult{}, emptyKeyError()
	}
	if err := opts.validate(); err != nil {
		return MVCCScanResult{}, err
	}
	if opts.MaxKeys < 0 || opts.TargetBytes < 0 {
		resumeSpan := &roachpb.Span{Key: key, EndKey: endKey}
		return MVCCScanResult{ResumeSpan: resumeSpan}, nil
	}

	// If the iterator has a specialized implementation, defer to that.
	//if mvccIter, ok := iter.(MVCCIterator); ok && mvccIter.MVCCOpsSpecialized() {
	//	return mvccIter.MVCCScan(key, endKey, timestamp, opts)
	//}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer pebbleMVCCScannerPool.Put(mvccScanner)

	*mvccScanner = pebbleMVCCScanner{
		parent:                 iter,
		memAccount:             opts.MemoryAccount,
		reverse:                opts.Reverse,
		start:                  key,
		end:                    endKey,
		ts:                     timestamp,
		maxKeys:                opts.MaxKeys,
		targetBytes:            opts.TargetBytes,
		targetBytesAvoidExcess: opts.TargetBytesAvoidExcess,
		targetBytesAllowEmpty:  opts.TargetBytesAllowEmpty,
		maxIntents:             opts.MaxIntents,
		inconsistent:           opts.Inconsistent,
		tombstones:             opts.Tombstones,
		failOnMoreRecent:       opts.FailOnMoreRecent,
		keyBuf:                 mvccScanner.keyBuf,
	}

	adapter := mvccScanFetchAdapter{
		scanner: mvccScanner,
	}
	memMon := mon.NewUnlimitedMonitor(ctx,
		"mvcc-scan",
		mon.MemoryResource,
		nil,
		nil,
		-1,
		nil,
	)
	memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	acc := memMon.MakeBoundAccount()
	codec := keys.MakeSQLCodec(roachpb.MakeTenantID(tenantID))
	wrapper, err := GetCFetcherWrapper(
		ctx,
		&acc,
		codec,
		tableReaderSpec,
		&adapter,
	)
	if err != nil {
		return MVCCScanResult{}, err
	}

	mvccScanner.init(opts.Txn, opts.LocalUncertaintyLimit)
	mvccScanner.results = &adapter.results

	var res MVCCScanResult

	if err := mvccScanner.seekToStartOfScan(); err != nil {
		return res, err
	}

	for {
		batch, err := wrapper.NextBatch(ctx)
		if err != nil {
			return res, err
		}
		if batch == nil {
			break
		}
		res.KVData = append(res.KVData, batch)
	}

	mvccScanner.maybeFailOnMoreRecent()

	var resume *roachpb.Span
	if mvccScanner.maxKeys > 0 && mvccScanner.results.getCount() == mvccScanner.maxKeys && mvccScanner.advanceKey() {
		if mvccScanner.reverse {
			// curKey was not added to results, so it needs to be included in the
			// resume span.
			//
			// NB: this is equivalent to:
			//  append(roachpb.Key(nil), p.curKey.Key...).Next()
			// but with half the allocations.
			curKey := mvccScanner.curUnsafeKey.Key
			curKeyCopy := make(roachpb.Key, len(curKey), len(curKey)+1)
			copy(curKeyCopy, curKey)
			resume = &roachpb.Span{
				Key:    mvccScanner.start,
				EndKey: curKeyCopy.Next(),
			}
		} else {
			resume = &roachpb.Span{
				Key:    append(roachpb.Key(nil), mvccScanner.curUnsafeKey.Key...),
				EndKey: mvccScanner.end,
			}
		}
	}

	res.ResumeSpan = resume
	res.NumKeys = mvccScanner.results.getCount()
	res.NumBytes = mvccScanner.results.getBytes()

	res.Intents, err = buildScanIntents(mvccScanner.intentsRepr())
	if err != nil {
		return MVCCScanResult{}, err
	}

	if !opts.Inconsistent && len(res.Intents) > 0 {
		return MVCCScanResult{}, &roachpb.WriteIntentError{Intents: res.Intents}
	}
	return res, nil
}
