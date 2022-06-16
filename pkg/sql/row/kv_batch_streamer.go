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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CanUseStreamer returns whether the kvstreamer.Streamer API should be used.
func CanUseStreamer(ctx context.Context, settings *cluster.Settings) bool {
	return useStreamerEnabled.Get(&settings.SV)
}

// useStreamerEnabled determines whether the Streamer API should be used.
// TODO(yuzefovich): remove this in 23.1.
var useStreamerEnabled = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	"sql.distsql.use_streamer.enabled",
	"determines whether the usage of the Streamer API is allowed. "+
		"Enabling this will increase the speed of lookup/index joins "+
		"while adhering to memory limits.",
	true,
)

// txnKVStreamer handles retrieval of key/values.
type txnKVStreamer struct {
	streamer   *kvstreamer.Streamer
	keyLocking lock.Strength

	spans   roachpb.Spans
	spanIDs []int

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
	streamer *kvstreamer.Streamer, lockStrength descpb.ScanLockingStrength,
) KVBatchFetcher {
	return &txnKVStreamer{
		streamer:   streamer,
		keyLocking: getKeyLockingStrength(lockStrength),
	}
}

// SetupNextFetch implements the KVBatchFetcher interface.
func (f *txnKVStreamer) SetupNextFetch(
	ctx context.Context,
	spans roachpb.Spans,
	spanIDs []int,
	bytesLimit rowinfra.BytesLimit,
	_ rowinfra.KeyLimit,
) error {
	if bytesLimit != rowinfra.NoBytesLimit {
		return errors.AssertionFailedf("unexpectedly non-zero bytes limit for txnKVStreamer")
	}
	f.reset(ctx)
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "Scan %s", spans)
	}
	reqs := spansToRequests(spans, false /* reverse */, f.keyLocking)
	if err := f.streamer.Enqueue(ctx, reqs); err != nil {
		return err
	}
	f.spans = spans
	f.spanIDs = spanIDs
	return nil
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
) (skip bool, _ kvBatchFetcherResponse, _ error) {
	result := f.lastResultState.Result
	ret := kvBatchFetcherResponse{
		moreKVs: true,
		spanID:  f.getSpanID(result.Position),
	}
	if get := result.GetResp; get != nil {
		// No need to check get.IntentValue since the Streamer guarantees that
		// it is nil.
		if get.Value == nil {
			// Nothing found in this particular response, so we skip it.
			f.releaseLastResult(ctx)
			return true, kvBatchFetcherResponse{}, nil
		}
		origSpan := f.spans[result.Position]
		f.getResponseScratch[0] = roachpb.KeyValue{Key: origSpan.Key, Value: *get.Value}
		ret.kvs = f.getResponseScratch[:]
		return false, ret, nil
	}
	scan := result.ScanResp
	if len(scan.BatchResponses) > 0 {
		ret.batchResponse, f.lastResultState.remainingBatches = scan.BatchResponses[0], scan.BatchResponses[1:]
	}
	// We're consciously ignoring scan.Rows argument since the Streamer
	// guarantees to always produce Scan responses using BATCH_RESPONSE format.
	//
	// Note that ret.batchResponse might be nil when the ScanResponse is empty,
	// and the caller (the KVFetcher) will skip over it.
	return false, ret, nil
}

func (f *txnKVStreamer) releaseLastResult(ctx context.Context) {
	f.lastResultState.Release(ctx)
	f.lastResultState.Result = kvstreamer.Result{}
}

// nextBatch implements the KVBatchFetcher interface.
func (f *txnKVStreamer) nextBatch(ctx context.Context) (kvBatchFetcherResponse, error) {
	// Check whether there are more batches in the current ScanResponse.
	if len(f.lastResultState.remainingBatches) > 0 {
		ret := kvBatchFetcherResponse{
			moreKVs: true,
			spanID:  f.getSpanID(f.lastResultState.Result.Position),
		}
		ret.batchResponse, f.lastResultState.remainingBatches = f.lastResultState.remainingBatches[0], f.lastResultState.remainingBatches[1:]
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
		return kvBatchFetcherResponse{moreKVs: false}, err
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

// close releases the resources of this txnKVStreamer.
func (f *txnKVStreamer) close(ctx context.Context) {
	f.reset(ctx)
	f.streamer.Close(ctx)
	*f = txnKVStreamer{}
}
