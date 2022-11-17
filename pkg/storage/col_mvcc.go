// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// NextKVer can fetch a new KV from somewhere. If MVCCDecodingStrategy is set
// to MVCCDecodingRequired, the returned KV will include a timestamp.
type NextKVer interface {
	// NextKV returns the next kv from this NextKVer. Returns false if there are
	// no more kvs to fetch, the kv that was fetched, and any errors that may
	// have occurred.
	//
	// needsCopy is set to true when the caller should copy the returned
	// KeyValue. One example of when this happens is when the returned KV's byte
	// slices are the last reference into a larger backing byte slice. In such a
	// case, the next call to NextKV might potentially allocate a big chunk of
	// new memory, and by copying the returned KeyValue into a small slice that
	// the caller owns, we avoid retaining two large backing byte slices at
	// once.
	NextKV(context.Context, MVCCDecodingStrategy) (
		ok bool, kv roachpb.KeyValue, needsCopy bool, err error,
	)

	// GetLastEncodedKey returns the key that was returned on the last NextKV()
	// call. This method allows callers to access the key at different layers of
	// abstraction.
	GetLastEncodedKey() roachpb.Key
}

// CFetcherWrapper is a wrapper around a colfetcher.cFetcher that returns a
// serialized set of bytes or a column-oriented batch.
type CFetcherWrapper interface {
	// NextBatch gives back the next column-oriented batch.
	//
	// If serialize is true, the returned batch will be the byte slice,
	// serialized in Arrow batch format. If serialize is false, the returned
	// batch will be the coldata.Batch.
	NextBatch(ctx context.Context, serialize bool) ([]byte, coldata.Batch, error)

	// Close release the resources held by this CFetcherWrapper. It *must* be
	// called after use of the wrapper.
	Close(ctx context.Context)
}

// GetCFetcherWrapper returns a CFetcherWrapper. It's injected from
// pkg/sql/colfetcher to avoid circular dependencies, since storage can't depend
// on higher levels of the system.
var GetCFetcherWrapper func(
	ctx context.Context,
	acc *mon.BoundAccount,
	indexFetchSpec *fetchpb.IndexFetchSpec,
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
	// and as a result we'll need to advance the iterator next time we call
	// NextKV.
	needToAdvanceBeforeGet bool
}

var _ NextKVer = &mvccScanFetchAdapter{}

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
	if len(enc) == 0 || len(lastKV.Value.RawBytes) == 0 {
		return false, lastKV, false, nil
		//return false, lastKV, false, errors.AssertionFailedf("unexpectedly received an empty lastKV")
	}
	switch mvccDecodeStrategy {
	case MVCCDecodingRequired:
		lastKV.Key, lastKV.Value.Timestamp, err = enginepb.DecodeKey(enc)
		if err != nil {
			return false, lastKV, false, err
		}
	case MVCCDecodingNotRequired:
		lastKV.Key, _, ok = enginepb.SplitMVCCKey(enc)
		if !ok {
			return false, lastKV, false, errors.AssertionFailedf("invalid encoded mvcc key: %x", enc)
		}
	}
	return true, lastKV, false, nil
}

// MVCCScanToCols is like MVCCScan, but it returns KVData in a serialized
// columnar batch suitable for reading by RecordBatchDeserializer.
func MVCCScanToCols(
	ctx context.Context,
	reader Reader,
	indexFetchSpec *fetchpb.IndexFetchSpec,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	iter := newMVCCIterator(
		reader, timestamp, !opts.Tombstones, opts.DontInterleaveIntents, IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: key,
			UpperBound: endKey,
		},
	)
	defer iter.Close()
	return mvccScanToCols(ctx, iter, indexFetchSpec, key, endKey, timestamp, opts)
}

func mvccScanToCols(
	ctx context.Context,
	iter MVCCIterator,
	indexFetchSpec *fetchpb.IndexFetchSpec,
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
	if opts.MaxKeys < 0 {
		return MVCCScanResult{
			ResumeSpan:   &roachpb.Span{Key: key, EndKey: endKey},
			ResumeReason: roachpb.RESUME_KEY_LIMIT,
		}, nil
	}
	if opts.TargetBytes < 0 {
		return MVCCScanResult{
			ResumeSpan:   &roachpb.Span{Key: key, EndKey: endKey},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
		}, nil
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer mvccScanner.release()

	*mvccScanner = pebbleMVCCScanner{
		parent:           iter,
		memAccount:       opts.MemoryAccount,
		lockTable:        opts.LockTable,
		reverse:          opts.Reverse,
		start:            key,
		end:              endKey,
		ts:               timestamp,
		maxKeys:          opts.MaxKeys,
		targetBytes:      opts.TargetBytes,
		allowEmpty:       opts.AllowEmpty,
		wholeRows:        opts.WholeRowsOfSize > 1, // single-KV rows don't need processing
		maxIntents:       opts.MaxIntents,
		inconsistent:     opts.Inconsistent,
		skipLocked:       opts.SkipLocked,
		tombstones:       opts.Tombstones,
		failOnMoreRecent: opts.FailOnMoreRecent,
		keyBuf:           mvccScanner.keyBuf,
	}

	var trackLastOffsets int
	if opts.WholeRowsOfSize > 1 {
		trackLastOffsets = int(opts.WholeRowsOfSize)
	}
	mvccScanner.init(opts.Txn, opts.Uncertainty, trackLastOffsets)

	adapter := mvccScanFetchAdapter{scanner: mvccScanner}
	unlimitedMonitor := mon.NewUnlimitedMonitor(
		ctx,
		"mvcc-scan-to-cols", /* name */
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		math.MaxInt64, /* noteworthy */
		nil,           /* settings */
	)
	unlimitedMonitor.Start(ctx, nil /* parent */, mon.NewStandaloneBudget(math.MaxInt64))
	defer unlimitedMonitor.Stop(ctx)
	acc := unlimitedMonitor.MakeBoundAccount()
	defer acc.Close(ctx)
	wrapper, err := GetCFetcherWrapper(
		ctx,
		&acc,
		indexFetchSpec,
		&adapter,
	)
	if err != nil {
		return MVCCScanResult{}, err
	}
	defer wrapper.Close(ctx)

	mvccScanner.results = &adapter.results

	var res MVCCScanResult

	if err := mvccScanner.seekToStartOfScan(); err != nil {
		return res, err
	}

	if grpcutil.IsLocalRequestContext(ctx) {
		for {
			_, batch, err := wrapper.NextBatch(ctx, false /* serialize */)
			if err != nil {
				return res, err
			}
			if batch == nil {
				break
			}
			res.ColBatches = append(res.ColBatches, batch)
			if mvccScanner.resumeReason != roachpb.RESUME_UNKNOWN {
				break
			}
		}
	} else {
		for {
			batch, _, err := wrapper.NextBatch(ctx, true /* serialize */)
			if err != nil {
				return res, err
			}
			if batch == nil {
				break
			}
			res.KVData = append(res.KVData, batch)
		}
	}

	mvccScanner.maybeFailOnMoreRecent()

	var resume *roachpb.Span
	if mvccScanner.advanceKey() {
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
	res.ResumeReason = mvccScanner.resumeReason
	res.ResumeNextBytes = mvccScanner.resumeNextBytes
	//res.NumKeys = mvccScanner.results.count
	//res.NumBytes = mvccScanner.results.bytes

	// If we have a trace, emit the scan stats that we produced.
	recordIteratorStats(ctx, mvccScanner.parent)

	res.Intents, err = buildScanIntents(mvccScanner.intentsRepr())
	if err != nil {
		return MVCCScanResult{}, err
	}

	if opts.errOnIntents() && len(res.Intents) > 0 {
		return MVCCScanResult{}, &roachpb.WriteIntentError{Intents: res.Intents}
	}
	return res, nil
}
