// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// txnKVStreamer handles retrieval of key/values.
type txnKVStreamer struct {
	kvBatchMetrics
	streamer       *kvstreamer.Streamer
	lockStrength   lock.Strength
	lockDurability lock.Durability
	rawMVCCValues  bool
	reverse        bool

	// spans contains the last set of spans provided in SetupNextFetch. The
	// original span is only needed when handling Get responses, so each span is
	// nil-ed out when it resulted in a Scan or ReverseScan request (i.e. it had
	// both Key and EndKey set).
	spans       identifiableSpans
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
	lockDurability descpb.ScanLockingDurability,
	acc *mon.BoundAccount,
	kvPairsRead *int64,
	batchRequestsIssued *int64,
	rawMVCCValues bool,
	reverse bool,
) KVBatchFetcher {
	f := &txnKVStreamer{
		streamer:       streamer,
		lockStrength:   GetKeyLockingStrength(lockStrength),
		lockDurability: GetKeyLockingDurability(lockDurability),
		reverse:        reverse,
		acc:            acc,
		rawMVCCValues:  rawMVCCValues,
	}
	f.kvBatchMetrics.init(kvPairsRead, batchRequestsIssued)
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

	// Since the streamer takes ownership of the spans slice, we don't need to
	// perform the deep copy. Notably, the spans might be modified (when the
	// streamer receives the resume spans), but the streamer will always keep the
	// memory accounting up to date.
	f.spans = identifiableSpans{
		Spans:   spans,
		spanIDs: spanIDs,
	}
	if f.reverse {
		// Reverse scans receive the spans in decreasing order. Note that we
		// need to be this tricky since we're updating the spans in place.
		i, j := 0, f.spans.Len()-1
		for i < j {
			f.spans.swap(i, j)
			i++
			j--
		}
	}
	scanName := "Scan"
	if f.reverse {
		scanName = "ReverseScan"
	}
	if log.ExpensiveLogEnabled(ctx, 2) {
		lockStr := ""
		if f.lockStrength != lock.None {
			lockStr = fmt.Sprintf(" lock %s (%s)", f.lockStrength.String(), f.lockDurability.String())
		}
		log.VEventf(ctx, 2, "%s %s%s", scanName, spans.BoundedString(1024 /* bytesHint */), lockStr)
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
	reqs := spansToRequests(spans, kvpb.BATCH_RESPONSE, f.reverse, f.rawMVCCValues, f.lockStrength, f.lockDurability, reqsScratch)
	if err := f.streamer.Enqueue(ctx, reqs); err != nil {
		// Mark this error as having come from the storage layer. This will
		// allow us to avoid creating a sentry report since this error isn't
		// actionable (e.g. we can get stop.ErrUnavailable here, which would be
		// treated as "internal error" by the ColIndexJoin, which later would
		// result in treating it as assertion failure because the error doesn't
		// have the PG code - marking it as a storage error will skip that).
		return colexecerror.NewStorageError(err)
	}
	// For the spans slice we only need to account for the overhead of
	// roachpb.Span objects. This is because spans that correspond to
	// - Scan requests just got nil-ed out in `spansToRequests`,
	// - Get requests have each key being shared directly (i.e. memory aliased)
	//   with the Get requests, and the streamer will account for the latter.
	//   Thus, in order to not double-count memory usage, we do no accounting
	//   here.
	spansMemUsage := roachpb.SpanOverhead * int64(cap(spans))

	// Keep the reference to the requests slice in order to reuse in the future.
	f.reqsScratch = reqs
	reqsScratchMemUsage := requestUnionOverhead * int64(cap(f.reqsScratch))
	return f.acc.ResizeTo(ctx, spansMemUsage+reqsScratchMemUsage)
}

func (f *txnKVStreamer) getSpanID(resultPosition int) int {
	if f.spans.spanIDs == nil {
		return resultPosition
	}
	return f.spans.spanIDs[resultPosition]
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
		origSpan := f.spans.Spans[result.Position]
		f.getResponseScratch[0] = roachpb.KeyValue{Key: origSpan.Key, Value: *get.Value}
		ret.KVs = f.getResponseScratch[:]
		return false, ret, nil
	}
	batchResponses := kvstreamer.GetScanBatchResponses(result.ScanResp)
	if len(batchResponses) > 0 {
		ret.BatchResponse, f.lastResultState.remainingBatches = batchResponses[0], batchResponses[1:]
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

// NextBatch implements the KVBatchFetcher interface.
func (f *txnKVStreamer) NextBatch(ctx context.Context) (KVBatchFetcherResponse, error) {
	resp, err := f.nextBatch(ctx)
	if !resp.MoreKVs || err != nil {
		return resp, err
	}
	f.kvBatchMetrics.Record(resp)
	return resp, nil
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
	f.acc.Clear(ctx)
	// Preserve observability-related fields.
	*f = txnKVStreamer{kvBatchMetrics: f.kvBatchMetrics}
}
