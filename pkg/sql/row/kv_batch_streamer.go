// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// UseStreamerEnabled determines whether the Streamer API can be used.
// TODO(yuzefovich): remove this in 22.2.
var UseStreamerEnabled = settings.RegisterBoolSetting(
	"sql.distsql.use_streamer.enabled",
	"determines whether the usage of the Streamer API is allowed. "+
		"Enabling this will increase the speed of lookup/index joins "+
		"while adhering to memory limits.",
	true,
)

// TxnKVStreamer handles retrieval of key/values.
type TxnKVStreamer struct {
	streamer kvcoord.Streamer
	spans    roachpb.Spans

	// numOutstandingRequests tracks the number of requests that haven't been
	// fully responded to yet.
	numOutstandingRequests int

	results         []kvcoord.Result
	lastResultState struct {
		kvcoord.Result
		// numEmitted tracks the number of times this result has been fully
		// emitted.
		numEmitted int
		// Used only for ScanResponses.
		remainingBatches [][]byte
	}
}

var _ KVBatchFetcher = &TxnKVStreamer{}

// NewTxnKVStreamer creates a new TxnKVStreamer.
func NewTxnKVStreamer(
	ctx context.Context,
	streamer kvcoord.Streamer,
	spans roachpb.Spans,
	lockStrength descpb.ScanLockingStrength,
) (*TxnKVStreamer, error) {
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "Scan %s", spans)
	}
	keyLocking := getKeyLockingStrength(lockStrength)
	reqs := spansToRequests(spans, false /* reverse */, keyLocking)
	scratchKeys := make([]int, len(spans))
	for i := range scratchKeys {
		scratchKeys[i] = i
	}
	if err := streamer.Enqueue(ctx, reqs, scratchKeys); err != nil {
		return nil, err
	}
	return &TxnKVStreamer{
		streamer:               streamer,
		spans:                  spans,
		numOutstandingRequests: len(spans),
	}, nil
}

// proceedWithLastResult processes the result which must be already set on the
// lastResultState and emits the first part of the response (the only part for
// GetResponses).
func (f *TxnKVStreamer) proceedWithLastResult() (
	skip bool,
	kvs []roachpb.KeyValue,
	batchResp []byte,
	err error,
) {
	result := f.lastResultState.Result
	if get := result.GetResp; get != nil {
		if get.IntentValue != nil {
			return false, nil, nil, errors.AssertionFailedf(
				"unexpectedly got an IntentValue back from a SQL GetRequest %v", *get.IntentValue,
			)
		}
		if get.Value == nil {
			// Nothing found in this particular response, so we skip it.
			f.releaseLastResult()
			return true, nil, nil, nil
		}
		pos := result.Keys[f.lastResultState.numEmitted]
		origSpan := f.spans[pos]
		f.lastResultState.numEmitted++
		f.numOutstandingRequests--
		return false, []roachpb.KeyValue{{Key: origSpan.Key, Value: *get.Value}}, nil, nil
	}
	scan := result.ScanResp
	if len(scan.BatchResponses) > 0 {
		batchResp, f.lastResultState.remainingBatches = scan.BatchResponses[0], scan.BatchResponses[1:]
	}
	if len(f.lastResultState.remainingBatches) == 0 {
		f.processedScanResponse()
	}
	return false, scan.Rows, batchResp, nil
}

// processedScanResponse updates the lastResultState before emitting the last
// part of the ScanResponse. This method should be called for each request that
// the ScanResponse satisfies.
func (f *TxnKVStreamer) processedScanResponse() {
	f.lastResultState.numEmitted++
	if f.lastResultState.ScanComplete {
		f.numOutstandingRequests--
	}
}

func (f *TxnKVStreamer) releaseLastResult() {
	f.lastResultState.MemoryTok.Release()
	f.lastResultState.Result = kvcoord.Result{}
}

// nextBatch returns the next batch of key/value pairs. If there are none
// available, a fetch is initiated. When there are no more keys, ok is false.
func (f *TxnKVStreamer) nextBatch(
	ctx context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResp []byte, err error) {
	if f.numOutstandingRequests == 0 {
		// All requests have already been responded to.
		f.releaseLastResult()
		return false, nil, nil, nil
	}

	// Check whether there are more batches in the current ScanResponse.
	if len(f.lastResultState.remainingBatches) > 0 {
		batchResp, f.lastResultState.remainingBatches = f.lastResultState.remainingBatches[0], f.lastResultState.remainingBatches[1:]
		if len(f.lastResultState.remainingBatches) == 0 {
			f.processedScanResponse()
		}
		return true, nil, batchResp, nil
	}

	// Check whether the current result satisfies multiple requests.
	if f.lastResultState.numEmitted < len(f.lastResultState.Keys) {
		// Note that we should never get an error here since we're processing
		// the same result again.
		_, kvs, batchResp, err = f.proceedWithLastResult()
		return true, kvs, batchResp, err
	}

	// Release the current result.
	if f.lastResultState.numEmitted == len(f.lastResultState.Keys) && f.lastResultState.numEmitted > 0 {
		f.releaseLastResult()
	}

	// Process the next result we have already received from the streamer.
	for len(f.results) > 0 {
		result := f.results[0]
		f.lastResultState.Result = result
		f.lastResultState.numEmitted = 0
		f.lastResultState.remainingBatches = nil
		f.results[0] = kvcoord.Result{}
		f.results = f.results[1:]
		var skip bool
		skip, kvs, batchResp, err = f.proceedWithLastResult()
		if err != nil {
			return false, nil, nil, err
		}
		if skip {
			continue
		}
		return true, kvs, batchResp, nil
	}

	// Get more results from the streamer. The returned results are accounted
	// against the streamer's budget, so we don't have to worry about that.
	f.results, err = f.streamer.GetResults(ctx)
	if f.results == nil || err != nil {
		return false, nil, nil, err
	}
	return f.nextBatch(ctx)
}

// close releases the resources of this TxnKVStreamer.
func (f *TxnKVStreamer) close(context.Context) {
	if f.lastResultState.MemoryTok != nil {
		f.lastResultState.MemoryTok.Release()
	}
	for _, r := range f.results {
		r.MemoryTok.Release()
	}
	*f = TxnKVStreamer{}
}
