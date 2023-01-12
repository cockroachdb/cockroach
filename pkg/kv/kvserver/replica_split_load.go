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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// transitions (cluster setting change)
//  add rpc field MaxCPU
//  maxCPU / maxQPS alwasy return not ok when being used
//  decider reset
//  set threshold function
//  set finder creation fn

// SplitByLoadEnabled wraps "kv.range_split.by_load_enabled".
var SplitByLoadEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.range_split.by_load_enabled",
	"allow automatic splits of ranges based on where load is concentrated",
	true,
).WithPublic()

var EnableUnweightedLBSplitFinder = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.unweighted_lb_split_finder.enabled",
	"if enabled, use the un-weighted finder for load-based splitting; "+
		"the unweighted finder will attempt to find a key during when splitting "+
		"a range based on load that evenly divides the QPS among the resulting "+
		"left and right hand side ranges",
	false,
)

// SplitByLoadQPSThreshold wraps "kv.range_split.load_qps_threshold".
var SplitByLoadQPSThreshold = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.range_split.load_qps_threshold",
	"the QPS over which, the range becomes a candidate for load based splitting",
	2500, // 2500 req/s
).WithPublic()

// SplitByLoadCPUThreshold wraps "kv.range_split.load_qps_threshold".
var SplitByLoadCPUThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.range_split.load_cpu_threshold",
	"the CPU use per second over which, the range becomes a candidate for load based splitting",
	250*time.Millisecond,
).WithPublic()

func (r *Replica) SplitByLoadThreshold(ctx context.Context) float64 {
	return SplitByLoadThresholdFn(ctx, r.store.cfg.Settings)()
}

func SplitByLoadThresholdFn(ctx context.Context, st *cluster.Settings) func() float64 {
	return func() float64 {
		if QPSSplittingEnabled(ctx, st) {
			return float64(SplitByLoadQPSThreshold.Get(&st.SV))
		}
		return float64(SplitByLoadCPUThreshold.Get(&st.SV))
	}
}

func NewFinderFn(
	ctx context.Context, st *cluster.Settings,
) func(time.Time, split.RandSource) split.LoadBasedSplitter {
	return func(startTime time.Time, randSource split.RandSource) split.LoadBasedSplitter {
		if QPSSplittingEnabled(ctx, st) {
			return split.NewUnweightedFinder(startTime, randSource)
		} else {
			return split.NewWeightedFinder(startTime, randSource)
		}
	}
}

// split queue
// - store rebalancer dimension
// - threshold for that dimension
// merge queue
// - store rebalancer dimension
// - threshold for that dimension
// - which value to look at when trying to merge
// finder
// - split threshold
// - new finder fn
// - when to reset for new dimension
//
// SplitByLoadEnabled returns whether load based splitting is enabled.
// Although this is a method of *Replica, the configuration is really global,
// shared across all stores.
func (r *Replica) SplitByLoadEnabled() bool {
	return SplitByLoadEnabled.Get(&r.store.cfg.Settings.SV) &&
		!r.store.TestingKnobs().DisableLoadBasedSplitting
}

func QPSSplittingEnabled(ctx context.Context, sv *cluster.Settings) bool {
	rebalanceObjective := LoadBasedRebalancingObjective(ctx, sv)
	return rebalanceObjective == LBRebalancingQueries || EnableUnweightedLBSplitFinder.Get(&sv.SV)
}

func (r *Replica) QPSSplittingEnabled(ctx context.Context) bool {
	return QPSSplittingEnabled(ctx, r.store.cfg.Settings)
}

func (r *Replica) setOnDimensionChange(ctx context.Context) {
	r.loadBasedSplitter.Reset(r.Clock().PhysicalTime())
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

type loadSplitStat struct {
	max float64
	ok  bool
}

func (ls loadSplitStat) merge(other loadSplitStat) loadSplitStat {
	return loadSplitStat{
		ok:  ls.ok && other.ok,
		max: ls.max + other.max,
	}
}

type loadSplitStats struct {
	CPU loadSplitStat
	QPS loadSplitStat
}

func (lss loadSplitStats) merge(other loadSplitStats) loadSplitStats {
	return loadSplitStats{
		CPU: lss.CPU.merge(other.CPU),
		QPS: lss.QPS.merge(other.QPS),
	}
}

func (lss loadSplitStats) String() string {
	var buf strings.Builder

	if lss.CPU.ok {
		fmt.Fprintf(&buf, "cpu-per-second=%s", string(humanizeutil.Duration(time.Duration(int64(lss.CPU.max)))))
	}
	if lss.QPS.ok {
		fmt.Fprintf(&buf, "queries-per-second=%.2f", lss.QPS.max)
	}
	return buf.String()
}

func (r *Replica) GetLoadSplitStats(ctx context.Context) loadSplitStats {
	lss := loadSplitStats{}
	max, ok := r.loadBasedSplitter.MaxStat(ctx, r.Clock().PhysicalTime())
	if r.QPSSplittingEnabled(ctx) {
		lss.QPS = loadSplitStat{max: max, ok: ok}
	} else {
		lss.CPU = loadSplitStat{max: max, ok: ok}
	}
	return lss
}

// recordBatchForLoadBasedSplitting records the batch's spans to be considered
// for load based splitting.
func (r *Replica) recordBatchForLoadBasedSplitting(
	ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, stat int,
) {
	if !r.SplitByLoadEnabled() {
		return
	}

	// When there is nothing to do when either the batch request or batch
	// response are nil.
	if ba == nil || br == nil {
		return
	}

	if len(ba.Requests) != len(br.Responses) {
		log.KvDistribution.Errorf(ctx,
			"Requests and responses should be equal lengths: # of requests = %d, # of responses = %d",
			len(ba.Requests), len(br.Responses))
	}

	// When QPS splitting is enabled, use the number of requests rather than
	// the given stat for recording load.
	if r.QPSSplittingEnabled(ctx) {
		stat = len(ba.Requests)
	}

	shouldInitSplit := r.loadBasedSplitter.Record(ctx, timeutil.Now(), stat, func() roachpb.Span {
		return getResponseBoundarySpan(ba, br)
	})
	if shouldInitSplit {
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	}
}
