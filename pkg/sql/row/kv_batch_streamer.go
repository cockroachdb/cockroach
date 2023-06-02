// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// txnKVStreamer handles retrieval of key/values.
type txnKVStreamer struct {
	kvBatchFetcherHelper
	streamer   *kvstreamer.Streamer
	keyLocking lock.Strength

	spans       roachpb.Spans
	spanIDs     []int
	reqsScratch []kvpb.RequestUnion

	acc *mon.BoundAccount

	// getResponseScratch is reused to return the result of Get requests.
	getResponseScratch [1]roachpb.KeyValue

	results         []kvstreamer.Result
	lastResultState struct {
		kvstreamer.Result
		// Used only for ScanResponses.
		remainingBatches [][]byte
	}
}

var _ KVBatchFetcher = &txnKVStreamer{}

// newTxnKVStreamer creates a new txnKVStreamer.
func newTxnKVStreamer(
	streamer *kvstreamer.Streamer,
	lockStrength descpb.ScanLockingStrength,
	acc *mon.BoundAccount,
	kvPairsRead *int64,
	batchRequestsIssued *int64,
) KVBatchFetcher {
	f := &txnKVStreamer{
		streamer:   streamer,
		keyLocking: GetKeyLockingStrength(lockStrength),
		acc:        acc,
	}
	f.kvBatchFetcherHelper.init(f.nextBatch, kvPairsRead, batchRequestsIssued)
	return f
}

// SetupNextFetch implements the KVBatchFetcher interface.
func (f *txnKVStreamer) SetupNextFetch(
	ctx context.Context,
	spans roachpb.Spans,
	spanIDs []int,
	bytesLimit rowinfra.BytesLimit,
	_ rowinfra.KeyLimit,
	_ bool,
) error {
	if bytesLimit != rowinfra.NoBytesLimit {
		return errors.AssertionFailedf("unexpected non-zero bytes limit for txnKVStreamer")
	}
	f.reset(ctx)
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "Scan %s", spans)
	}
	// Make sure to nil out the requests past the length that will be used in
	// spansToRequests so that we lose references to the underlying Get and Scan
	// requests (which could keep large byte slices alive) from the previous
	// iteration.
	//
	// Note that we could not do this nil-ing out after Enqueue() returned on
	// the previous iteration because in some cases the streamer will hold on to
	// the slice (which is the case when the requests are contained within a
	// single range). At the same time we don't want to push the responsibility
	// of nil-ing the slice out because we (i.e. the txnKVStreamer) are the ones
	// that keep the slice for reuse, and the streamer doesn't know anything
	// about the slice reuse.
	reqsScratch := f.reqsScratch[:cap(f.reqsScratch)]
	for i := len(spans); i < len(reqsScratch); i++ {
		reqsScratch[i] = kvpb.RequestUnion{}
	}
	// TODO(yuzefovich): consider supporting COL_BATCH_RESPONSE scan format.
	reqs := spansToRequests(spans, kvpb.BATCH_RESPONSE, false /* reverse */, f.keyLocking, reqsScratch)
	if err := f.streamer.Enqueue(ctx, reqs); err != nil {
		return err
	}
	f.spans = spans
	f.spanIDs = spanIDs
	// Keep the reference to the requests slice in order to reuse in the future.
	f.reqsScratch = reqs
	reqsScratchMemUsage := requestUnionOverhead * int64(cap(f.reqsScratch))
	return f.acc.ResizeTo(ctx, reqsScratchMemUsage)
}

func (f *txnKVStreamer) getSpanID(resultPosition int) int {
	if f.spanIDs == nil {
		return resultPosition
	}
	return f.spanIDs[resultPosition]
}

// proceedWithLastResult processes the result which must be already set on the
// lastResultState and emits the first part of the response (the only part for
// GetResponses).
func (f *txnKVStreamer) proceedWithLastResult(
	ctx context.Context,
) (skip bool, _ KVBatchFetcherResponse, _ error) {
	result := f.lastResultState.Result
	ret := KVBatchFetcherResponse{
		MoreKVs: true,
		spanID:  f.getSpanID(result.Position),
	}
	if get := result.GetResp; get != nil {
		// No need to check get.IntentValue since the Streamer guarantees that
		// it is nil.
		if get.Value == nil {
			// Nothing found in this particular response, so we skip it.
			f.releaseLastResult(ctx)
			return true, KVBatchFetcherResponse{}, nil
		}
		origSpan := f.spans[result.Position]
		f.getResponseScratch[0] = roachpb.KeyValue{Key: origSpan.Key, Value: *get.Value}
		ret.KVs = f.getResponseScratch[:]
		return false, ret, nil
	}
	scan := result.ScanResp
	if len(scan.BatchResponses) > 0 {
		ret.BatchResponse, f.lastResultState.remainingBatches = scan.BatchResponses[0], scan.BatchResponses[1:]
	}
	// We're consciously ignoring scan.Rows argument since the Streamer
	// guarantees to always produce Scan responses using BATCH_RESPONSE format.
	//
	// Note that ret.BatchResponse might be nil when the ScanResponse is empty,
	// and the caller (the KVFetcher) will skip over it.
	return false, ret, nil
}

func (f *txnKVStreamer) releaseLastResult(ctx context.Context) {
	f.lastResultState.Release(ctx)
	f.lastResultState.Result = kvstreamer.Result{}
}

func (f *txnKVStreamer) nextBatch(ctx context.Context) (resp KVBatchFetcherResponse, _ error) {
	// Check whether there are more batches in the current ScanResponse.
	if len(f.lastResultState.remainingBatches) > 0 {
		ret := KVBatchFetcherResponse{
			MoreKVs: true,
			spanID:  f.getSpanID(f.lastResultState.Result.Position),
		}
		ret.BatchResponse, f.lastResultState.remainingBatches = f.lastResultState.remainingBatches[0], f.lastResultState.remainingBatches[1:]
		return ret, nil
	}

	// Release the current result.
	f.releaseLastResult(ctx)

	// Process the next result we have already received from the streamer.
	for len(f.results) > 0 {
		// Peel off the next result and set it into lastResultState.
		f.lastResultState.Result = f.results[0]
		f.lastResultState.remainingBatches = nil
		// Lose the reference to that result and advance the results slice for
		// the next iteration.
		f.results[0] = kvstreamer.Result{}
		f.results = f.results[1:]
		skip, ret, err := f.proceedWithLastResult(ctx)
		if !skip || err != nil {
			return ret, err
		}
	}

	// Get more results from the streamer. This call will block until some
	// results are available or we're done.
	//
	// The memory accounting for the returned results has already been performed
	// by the streamer against its own budget, so we don't have to concern
	// ourselves with the memory accounting here.
	var err error
	f.results, err = f.streamer.GetResults(ctx)
	if len(f.results) == 0 || err != nil {
		return KVBatchFetcherResponse{MoreKVs: false}, err
	}
	return f.nextBatch(ctx)
}

// reset releases all of the results from the last fetch.
func (f *txnKVStreamer) reset(ctx context.Context) {
	f.lastResultState.Release(ctx)
	for _, r := range f.results {
		r.Release(ctx)
	}
}

// Close releases the resources of this txnKVStreamer.
func (f *txnKVStreamer) Close(ctx context.Context) {
	f.reset(ctx)
	f.streamer.Close(ctx)
	// Preserve observability-related fields.
	*f = txnKVStreamer{kvBatchFetcherHelper: f.kvBatchFetcherHelper}
}
