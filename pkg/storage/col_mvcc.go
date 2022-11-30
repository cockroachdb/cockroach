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
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// NextKVer can fetch a new KV from somewhere. If MVCCDecodingStrategy is set
// to required, the returned KV will include a timestamp.
type NextKVer interface {
	// NextKV returns the next kv from this NextKVer. Returns false if there are
	// no more kvs to fetch, the kv that was fetched, and any errors that may
	// have occurred.
	//
	// finalReferenceToBatch is set to true if the returned KV's byte slices are
	// the last reference into a larger backing byte slice. This parameter
	// allows calling code to control its memory usage: if finalReferenceToBatch
	// is true, it means that the next call to NextKV might potentially allocate
	// a big chunk of new memory, so the returned KeyValue should be copied into
	// a small slice that the caller owns to avoid retaining two large backing
	// byte slices at once unexpectedly.
	NextKV(context.Context, MVCCDecodingStrategy) (
		ok bool, kv roachpb.KeyValue, finalReferenceToBatch bool, err error,
	)
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
	indexFetchSpec proto.Message,
	nextKVer NextKVer,
) (CFetcherWrapper, error)

// mvccScanFetchAdapter is a NextKVer that is implemented directly by a
// pebbleMVCCScanner. Each time its NextKV is called, it iterates the pebble
// scanner and returns the result. Note that the returned KV is only valid until
// the next call to NextKV.
type mvccScanFetchAdapter struct {
	scanner           *pebbleMVCCScanner
	onNextKV          func() bool
	afterNoopOnNextKV func() bool
	results           singleResults
}

var _ NextKVer = &mvccScanFetchAdapter{}

func (f *mvccScanFetchAdapter) seek() bool {
	ok := f.scanner.seekToStartOfScan()
	if f.scanner.reverse {
		f.onNextKV = f.reverseAdvance
	} else {
		f.onNextKV = f.forwardAdvance
	}
	return ok
}

func (f *mvccScanFetchAdapter) forwardAdvance() bool {
	return f.scanner.nextKey()
}

func (f *mvccScanFetchAdapter) reverseAdvance() bool {
	return f.scanner.prevKey(f.scanner.curUnsafeKey.Key)
}

func (f *mvccScanFetchAdapter) noop() bool {
	f.onNextKV = f.afterNoopOnNextKV
	f.afterNoopOnNextKV = nil
	return true
}

func (f *mvccScanFetchAdapter) done() bool {
	return false
}

// NextKV implements the NextKVer interface.
func (f *mvccScanFetchAdapter) NextKV(
	ctx context.Context, mvccDecodeStrategy MVCCDecodingStrategy,
) (ok bool, kv roachpb.KeyValue, finalReferenceToBatch bool, err error) {
	if !f.onNextKV() {
		// No more keys in the scan.
		return false, roachpb.KeyValue{}, false, f.scanner.err
	}
	advanced := f.scanner.getAndAdvance(ctx)
	if advanced {
		if f.results.newPut {
			err = errors.AssertionFailedf("unexpectedly advanced the iterator with a new put")
			return false, roachpb.KeyValue{}, false, err
		}
		// We've just skipped a key for whatever reason and advanced to the next
		// key, so we want to do a noop once and then proceed as usual.
		f.afterNoopOnNextKV = f.onNextKV
		f.onNextKV = f.noop
		return f.NextKV(ctx, mvccDecodeStrategy)
	}
	if f.scanner.skippedTombstone {
		return f.NextKV(ctx, mvccDecodeStrategy)
	}
	if !f.results.newPut {
		// We didn't put a new KV into the result due to some limit.
		f.scanner.advanceKeyEnabled = true
		return false, roachpb.KeyValue{}, false, nil
	} else if f.scanner.resumeReason != roachpb.RESUME_UNKNOWN {
		// We've just reached some limit, so the current KV will be the last
		// one.
		f.onNextKV = f.done
		f.scanner.advanceKeyEnabled = true
	}
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
			return false, lastKV, false, errors.AssertionFailedf("invalid encoded mvcc key: %x", enc)
		}
	case MVCCDecodingNotRequired:
		lastKV.Key, _, ok = enginepb.SplitMVCCKey(enc)
		if !ok {
			return false, lastKV, false, errors.AssertionFailedf("invalid encoded mvcc key: %x", enc)
		}
	}
	// TODO: think through for how we need to handle the copying with multiple
	// column families (here or in the cFetcher).
	return true, lastKV, true, nil
}

// MVCCScanToCols is like MVCCScan, but it returns KVData in a serialized
// columnar batch suitable for reading by RecordBatchDeserializer.
func MVCCScanToCols(
	ctx context.Context,
	reader Reader,
	indexFetchSpec proto.Message,
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
	indexFetchSpec proto.Message,
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
	if opts.WholeRowsOfSize != 0 {
		// TODO(yuzefovich): add support for this.
		return MVCCScanResult{}, errors.AssertionFailedf(
			"WholeRowsOfSize option is not supported with COL_BATCH_RESPONSE scan format",
		)
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
		wholeRows:        false,
		maxIntents:       opts.MaxIntents,
		inconsistent:     opts.Inconsistent,
		skipLocked:       opts.SkipLocked,
		tombstones:       opts.Tombstones,
		failOnMoreRecent: opts.FailOnMoreRecent,
		keyBuf:           mvccScanner.keyBuf,
	}

	adapter := mvccScanFetchAdapter{scanner: mvccScanner}
	mvccScanner.init(opts.Txn, opts.Uncertainty, &adapter.results)
	// Set the scanner to "get" mode, which prevents it from advancing after
	// it's finished retrieving the current key. This way, we have a chance to
	// copy the current key into our columnar batch before the scanner advances.
	mvccScanner.advanceKeyEnabled = false

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

	var res MVCCScanResult

	//if !mvccScanner.seekToStartOfScan() {
	//	return res, mvccScanner.err
	//}
	adapter.onNextKV = adapter.seek
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
			// We need to make a copy since the wrapper reuses underlying bytes
			// buffer.
			b := make([]byte, len(batch))
			copy(b, batch)
			res.KVData = append(res.KVData, b)
		}
	}

	res.ResumeSpan, res.ResumeReason, res.ResumeNextBytes, err = mvccScanner.afterScan()
	if err != nil {
		return MVCCScanResult{}, err
	}

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
