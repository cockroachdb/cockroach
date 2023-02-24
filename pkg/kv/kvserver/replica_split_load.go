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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

// SplitByLoadCPUThreshold wraps "kv.range_split.load_cpu_threshold". The
// default threshold of 500ms translates to a replica utilizing 50% of a CPU
// core processing requests. In practice, the "real" CPU usage of a replica all
// things considered (sql,compactions, gc) tends to be around 3x the attributed
// usage which this threshold is checked against. This means that in a static
// state we would expect no more than (number of cores) / 1.5 load based
// splits. In practice however, workload patterns change. The default threshold
// was selected after running kv(0|95)/(splt=0|seq) and allocbench, then
// inspecting which threshold had the best performance. Performance was
// measured as max ops/s for kv and resource balance for allocbench. See #96869
// for more details.
var SplitByLoadCPUThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.range_split.load_cpu_threshold",
	"the CPU use per second over which, the range becomes a candidate for load based splitting",
	500*time.Millisecond,
).WithPublic()

func (obj LBRebalancingObjective) ToSplitObjective() split.SplitObjective {
	switch obj {
	case LBRebalancingQueries:
		return split.SplitQPS
	case LBRebalancingCPU:
		return split.SplitCPU
	default:
		panic(fmt.Sprintf("unknown objective %d", obj))
	}
}

// replicaSplitConfig implements the split.SplitConfig interface.
type replicaSplitConfig struct {
	randSource split.RandSource
	st         *cluster.Settings
}

func newReplicaSplitConfig(st *cluster.Settings) *replicaSplitConfig {
	return &replicaSplitConfig{
		randSource: split.GlobalRandSource(),
		st:         st,
	}
}

// NewLoadBasedSplitter returns a new LoadBasedSplitter that may be used to
// find the midpoint based on recorded load.
func (c *replicaSplitConfig) NewLoadBasedSplitter(
	startTime time.Time, obj split.SplitObjective,
) split.LoadBasedSplitter {
	switch obj {
	case split.SplitQPS:
		return split.NewUnweightedFinder(startTime, c.randSource)
	case split.SplitCPU:
		return split.NewWeightedFinder(startTime, c.randSource)
	default:
		panic(errors.AssertionFailedf("Unkown rebalance objective %d", obj))
	}
}

// StatRetention returns the duration that recorded load is to be retained.
func (c *replicaSplitConfig) StatRetention() time.Duration {
	return kvserverbase.SplitByLoadMergeDelay.Get(&c.st.SV)
}

// StatThreshold returns the threshold for load above which the range should be
// considered split.
func (c *replicaSplitConfig) StatThreshold(obj split.SplitObjective) float64 {
	switch obj {
	case split.SplitQPS:
		return float64(SplitByLoadQPSThreshold.Get(&c.st.SV))
	case split.SplitCPU:
		return float64(SplitByLoadCPUThreshold.Get(&c.st.SV))
	default:
		panic(errors.AssertionFailedf("Unkown rebalance objective %d", obj))
	}
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
	ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
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
		case *kvpb.GetResponse:
			// The request did not evaluate. Ignore it.
			continue
		case *kvpb.ScanResponse:
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
		case *kvpb.ReverseScanResponse:
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
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse, cpu int,
) {
	if !r.SplitByLoadEnabled() {
		return
	}

	// There is nothing to do when either the batch request or batch response
	// are nil as we cannot record the load to a keyspan.
	if ba == nil || br == nil {
		return
	}

	if len(ba.Requests) != len(br.Responses) {
		log.KvDistribution.Errorf(ctx,
			"Requests and responses should be equal lengths: # of requests = %d, # of responses = %d",
			len(ba.Requests), len(br.Responses))
	}

	loadFn := func(obj split.SplitObjective) int {
		switch obj {
		case split.SplitCPU:
			return cpu
		default:
			return len(ba.Requests)
		}
	}

	spanFn := func() roachpb.Span {
		return getResponseBoundarySpan(ba, br)
	}

	shouldInitSplit := r.loadBasedSplitter.Record(ctx, r.Clock().PhysicalTime(), loadFn, spanFn)
	if shouldInitSplit {
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	}
}
