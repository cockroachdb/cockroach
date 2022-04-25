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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// CanUseStreamer returns whether the kvstreamer.Streamer API should be used.
func CanUseStreamer(ctx context.Context, settings *cluster.Settings) bool {
	// TODO(yuzefovich): remove the version gate in 22.2 cycle.
	return settings.Version.IsActive(ctx, clusterversion.ScanWholeRows) &&
		useStreamerEnabled.Get(&settings.SV)
}

// useStreamerEnabled determines whether the Streamer API should be used.
// TODO(yuzefovich): remove this in 22.2.
var useStreamerEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.distsql.use_streamer.enabled",
	"determines whether the usage of the Streamer API is allowed. "+
		"Enabling this will increase the speed of lookup/index joins "+
		"while adhering to memory limits.",
	true,
)

// TxnKVStreamer handles retrieval of key/values.
type TxnKVStreamer struct {
	streamer *kvstreamer.Streamer
	spans    roachpb.Spans

	// getResponseScratch is reused to return the result of Get requests.
	getResponseScratch [1]roachpb.KeyValue

	results         []kvstreamer.Result
	lastResultState struct {
		kvstreamer.Result
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
	streamer *kvstreamer.Streamer,
	spans roachpb.Spans,
	lockStrength descpb.ScanLockingStrength,
) (*TxnKVStreamer, error) {
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "Scan %s", spans)
	}
	keyLocking := getKeyLockingStrength(lockStrength)
	reqs := spansToRequests(spans, false /* reverse */, keyLocking)
	if err := streamer.Enqueue(ctx, reqs, nil /* enqueueKeys */); err != nil {
		return nil, err
	}
	return &TxnKVStreamer{
		streamer: streamer,
		spans:    spans,
	}, nil
}

// proceedWithLastResult processes the result which must be already set on the
// lastResultState and emits the first part of the response (the only part for
// GetResponses).
func (f *TxnKVStreamer) proceedWithLastResult(
	ctx context.Context,
) (skip bool, kvs []roachpb.KeyValue, batchResp []byte, err error) {
	result := f.lastResultState.Result
	if get := result.GetResp; get != nil {
		// No need to check get.IntentValue since the Streamer guarantees that
		// it is nil.
		if get.Value == nil {
			// Nothing found in this particular response, so we skip it.
			f.releaseLastResult(ctx)
			return true, nil, nil, nil
		}
		pos := result.EnqueueKeysSatisfied[f.lastResultState.numEmitted]
		origSpan := f.spans[pos]
		f.lastResultState.numEmitted++
		f.getResponseScratch[0] = roachpb.KeyValue{Key: origSpan.Key, Value: *get.Value}
		return false, f.getResponseScratch[:], nil, nil
	}
	scan := result.ScanResp
	if len(scan.BatchResponses) > 0 {
		batchResp, f.lastResultState.remainingBatches = scan.BatchResponses[0], scan.BatchResponses[1:]
	}
	if len(f.lastResultState.remainingBatches) == 0 {
		f.lastResultState.numEmitted++
	}
	// We're consciously ignoring scan.Rows argument since the Streamer
	// guarantees to always produce Scan responses using BATCH_RESPONSE format.
	//
	// Note that batchResp might be nil when the ScanResponse is empty, and the
	// caller (the KVFetcher) will skip over it.
	return false, nil, batchResp, nil
}

func (f *TxnKVStreamer) releaseLastResult(ctx context.Context) {
	f.lastResultState.Release(ctx)
	f.lastResultState.Result = kvstreamer.Result{}
}

// nextBatch returns the next batch of key/value pairs. If there are none
// available, a fetch is initiated. When there are no more keys, ok is false.
func (f *TxnKVStreamer) nextBatch(
	ctx context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResp []byte, err error) {
	// Check whether there are more batches in the current ScanResponse.
	if len(f.lastResultState.remainingBatches) > 0 {
		batchResp, f.lastResultState.remainingBatches = f.lastResultState.remainingBatches[0], f.lastResultState.remainingBatches[1:]
		if len(f.lastResultState.remainingBatches) == 0 {
			f.lastResultState.numEmitted++
		}
		return true, nil, batchResp, nil
	}

	// Check whether the current result satisfies multiple requests.
	if f.lastResultState.numEmitted < len(f.lastResultState.EnqueueKeysSatisfied) {
		// Note that we should never get an error here since we're processing
		// the same result again.
		_, kvs, batchResp, err = f.proceedWithLastResult(ctx)
		return true, kvs, batchResp, err
	}

	// Release the current result.
	if f.lastResultState.numEmitted == len(f.lastResultState.EnqueueKeysSatisfied) && f.lastResultState.numEmitted > 0 {
		f.releaseLastResult(ctx)
	}

	// Process the next result we have already received from the streamer.
	for len(f.results) > 0 {
		// Peel off the next result and set it into lastResultState.
		f.lastResultState.Result = f.results[0]
		f.lastResultState.numEmitted = 0
		f.lastResultState.remainingBatches = nil
		// Lose the reference to that result and advance the results slice for
		// the next iteration.
		f.results[0] = kvstreamer.Result{}
		f.results = f.results[1:]
		var skip bool
		skip, kvs, batchResp, err = f.proceedWithLastResult(ctx)
		if err != nil {
			return false, nil, nil, err
		}
		if skip {
			continue
		}
		return true, kvs, batchResp, err
	}

	// Get more results from the streamer. This call will block until some
	// results are available or we're done.
	//
	// The memory accounting for the returned results has already been performed
	// by the streamer against its own budget, so we don't have to concern
	// ourselves with the memory accounting here.
	f.results, err = f.streamer.GetResults(ctx)
	if len(f.results) == 0 || err != nil {
		return false, nil, nil, err
	}
	return f.nextBatch(ctx)
}

// close releases the resources of this TxnKVStreamer.
func (f *TxnKVStreamer) close(ctx context.Context) {
	f.lastResultState.Release(ctx)
	for _, r := range f.results {
		r.Release(ctx)
	}
	*f = TxnKVStreamer{}
}
