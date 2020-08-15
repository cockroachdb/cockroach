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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/gogo/protobuf/proto"
)

type CFetcherWrapper interface {
	NextBatch(ctx context.Context) ([]byte, error)
}

type NextKVer interface {
	NextKV(
		ctx context.Context, strategy MVCCDecodingStrategy,
	) (
		ok bool, retKv roachpb.KeyValue, newSpan bool, err error,
	)
	// GetCurSpan returns the most recent Span that was processed by this fetcher.
	GetCurSpan() roachpb.Span
	GetBytesRead() int64
}

var GetMeACFetcher func(
	ctx context.Context,
	acc *mon.BoundAccount,
	codec keys.SQLCodec,
	specMessage proto.Message,
	isProjection bool,
	neededCols util.FastIntSet,
	nextKVer NextKVer,
) (CFetcherWrapper, error)

type mvccScanFetchAdapter struct {
	scanner    *pebbleMVCCScanner
	results    singleResults
	noMoreKeys bool
}

func (f *mvccScanFetchAdapter) GetCurSpan() roachpb.Span {
	return roachpb.Span{
		Key:    f.scanner.start,
		EndKey: f.scanner.end,
	}
}

func (f *mvccScanFetchAdapter) GetBytesRead() int64 {
	return f.scanner.results.getBytes()
}

func (f *mvccScanFetchAdapter) NextKV(
	ctx context.Context, mvccDecodeStrategy MVCCDecodingStrategy,
) (ok bool, kv roachpb.KeyValue, newSpan bool, err error) {
	if f.noMoreKeys {
		return false, roachpb.KeyValue{}, false, nil
	}
	f.noMoreKeys = !f.scanner.getAndAdvance()
	lastKV := f.results.getLastKV()
	return true, lastKV, false, nil
}

// MVCCScanToCols is like MVCCScan, but it returns ???
// TODO(jordan): fill me out
func MVCCScanToCols(
	ctx context.Context,
	tenantID uint64,
	reader Reader,
	tableReaderSpec proto.Message,
	projection bool,
	neededCols []uint32,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	iter := reader.NewIterator(IterOptions{LowerBound: key, UpperBound: endKey})
	defer iter.Close()
	return mvccScanToCols(ctx, tenantID, iter, tableReaderSpec, projection, neededCols, key, endKey, timestamp, opts)
}

func mvccScanToCols(
	ctx context.Context,
	tenantID uint64,
	iter Iterator,
	spec proto.Message,
	projection bool,
	neededCols []uint32,
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
	if mvccIter, ok := iter.(MVCCIterator); ok && mvccIter.MVCCOpsSpecialized() {
		return mvccIter.MVCCScan(key, endKey, timestamp, opts)
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer pebbleMVCCScannerPool.Put(mvccScanner)

	*mvccScanner = pebbleMVCCScanner{
		parent:           iter,
		reverse:          opts.Reverse,
		start:            key,
		end:              endKey,
		ts:               timestamp,
		maxKeys:          opts.MaxKeys,
		targetBytes:      opts.TargetBytes,
		inconsistent:     opts.Inconsistent,
		tombstones:       opts.Tombstones,
		failOnMoreRecent: opts.FailOnMoreRecent,
		keyBuf:           mvccScanner.keyBuf,
	}

	adapter := mvccScanFetchAdapter{
		scanner: mvccScanner,
	}
	var fastIntSet util.FastIntSet
	for _, c := range neededCols {
		fastIntSet.Add(int(c))
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
	wrapper, err := GetMeACFetcher(
		ctx,
		&acc,
		codec,
		spec,
		projection,
		fastIntSet,
		&adapter,
	)
	if err != nil {
		return MVCCScanResult{}, err
	}

	mvccScanner.init(opts.Txn)
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
			curKey := mvccScanner.curKey.Key
			curKeyCopy := make(roachpb.Key, len(curKey), len(curKey)+1)
			copy(curKeyCopy, curKey)
			resume = &roachpb.Span{
				Key:    mvccScanner.start,
				EndKey: curKeyCopy.Next(),
			}
		} else {
			resume = &roachpb.Span{
				Key:    append(roachpb.Key(nil), mvccScanner.curKey.Key...),
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
