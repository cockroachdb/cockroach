// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SplitByLoadEnabled wraps "kv.range_split.by_load_enabled".
var SplitByLoadEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.range_split.by_load_enabled",
	"allow automatic splits of ranges based on where load is concentrated",
	true,
).WithPublic()

// SplitByLoadQPSThreshold wraps "kv.range_split.load_qps_threshold".
var SplitByLoadQPSThreshold = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.range_split.load_qps_threshold",
	"the QPS over which, the range becomes a candidate for load based splitting",
	2500, // 2500 req/s
).WithPublic()

// SplitByLoadQPSThreshold returns the QPS request rate for a given replica.
func (r *Replica) SplitByLoadQPSThreshold() float64 {
	return float64(SplitByLoadQPSThreshold.Get(&r.store.cfg.Settings.SV))
}

// SplitByLoadEnabled returns whether load based splitting is enabled.
// Although this is a method of *Replica, the configuration is really global,
// shared across all stores.
func (r *Replica) SplitByLoadEnabled() bool {
	return SplitByLoadEnabled.Get(&r.store.cfg.Settings.SV) &&
		!r.store.TestingKnobs().DisableLoadBasedSplitting
}

// getResponseBoundarySpan computes the union span of the true spans that were
// iterated over using the request span and the response's resumeSpan.
//
// Assumptions:
// 1. br != nil
// 2. len(ba.Requests) == len(br.Responses)
// Assumptions are checked in executeBatchWithConcurrencyRetries.
func getResponseBoundarySpan(
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse,
) (responseBoundarySpan roachpb.Span) {
	addSpanToBoundary := func(span roachpb.Span) {
		if !responseBoundarySpan.Valid() {
			responseBoundarySpan = span
		} else {
			responseBoundarySpan = responseBoundarySpan.Combine(span)
		}
	}
	for i, respUnion := range br.Responses {
		reqHeader := ba.Requests[i].GetInner().Header()
		resp := respUnion.GetInner()
		resumeSpan := resp.Header().ResumeSpan
		if resumeSpan == nil {
			// Fully evaluated.
			addSpanToBoundary(reqHeader.Span())
			continue
		}

		switch resp.(type) {
		case *roachpb.GetResponse:
			// The request did not evaluate. Ignore it.
			continue
		case *roachpb.ScanResponse:
			// Not reverse (->)
			// Request:    [key...............endKey)
			// ResumeSpan:          [key......endKey)
			// True span:  [key......key)
			//
			// Assumptions (not checked to minimize overhead):
			// reqHeader.EndKey == resumeSpan.EndKey
			// reqHeader.Key <= resumeSpan.Key.
			if reqHeader.Key.Equal(resumeSpan.Key) {
				// The request did not evaluate. Ignore it.
				continue
			}
			addSpanToBoundary(roachpb.Span{
				Key:    reqHeader.Key,
				EndKey: resumeSpan.Key,
			})
		case *roachpb.ReverseScanResponse:
			// Reverse (<-)
			// Request:    [key...............endKey)
			// ResumeSpan: [key......endKey)
			// True span:           [endKey...endKey)
			//
			// Assumptions (not checked to minimize overhead):
			// reqHeader.Key == resumeSpan.Key
			// resumeSpan.EndKey <= reqHeader.EndKey.
			if reqHeader.EndKey.Equal(resumeSpan.EndKey) {
				// The request did not evaluate. Ignore it.
				continue
			}
			addSpanToBoundary(roachpb.Span{
				Key:    resumeSpan.EndKey,
				EndKey: reqHeader.EndKey,
			})
		default:
			// Consider it fully evaluated, which is safe.
			addSpanToBoundary(reqHeader.Span())
		}
	}
	return
}

// recordBatchForLoadBasedSplitting records the batch's spans to be considered
// for load based splitting.
func (r *Replica) recordBatchForLoadBasedSplitting(
	ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse,
) {
	if !r.SplitByLoadEnabled() {
		return
	}
	shouldInitSplit := r.loadBasedSplitter.Record(ctx, timeutil.Now(), len(ba.Requests), func() roachpb.Span {
		return getResponseBoundarySpan(ba, br)
	})
	if shouldInitSplit {
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	}
}
