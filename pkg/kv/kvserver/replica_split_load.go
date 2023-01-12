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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// SplitByLoadCPUThreshold wraps "kv.range_split.load_qps_threshold".
var SplitByLoadCPUThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.range_split.load_cpu_threshold",
	"the CPU use per second over which, the range becomes a candidate for load based splitting",
	250*time.Millisecond,
).WithPublic()

// SplitObjective is a type that specifies a load based splitting objective.
type SplitObjective int

const (
	// SplitQPS will track and split QPS (queries-per-second) over a range.
	SplitQPS SplitObjective = iota
	// SplitCPU will track and split CPU (cpu-per-second) over a range.
	SplitCPU
)

// String returns a human readable string representation of the dimension.
func (d SplitObjective) String() string {
	switch d {
	case SplitQPS:
		return "qps"
	case SplitCPU:
		return "cpu"
	default:
		panic(fmt.Sprintf("cannot name: unknown objective with ordinal %d", d))
	}
}

// Format returns a formatted string for a value.
func (d SplitObjective) Format(value float64) string {
	switch d {
	case SplitQPS:
		return fmt.Sprintf("%.1f", value)
	case SplitCPU:
		return string(humanizeutil.Duration(time.Duration(int64(value))))
	default:
		panic(fmt.Sprintf("cannot format value: unknown objective with ordinal %d", d))
	}
}

// replicaSplitConfig implements the split.SplitConfig interface.
type replicaSplitConfig struct {
	randSource                 split.RandSource
	rebalanceObjectiveProvider RebalanceObjectiveProvider
	st                         *cluster.Settings
}

func newReplicaSplitConfig(
	st *cluster.Settings, rebalanceObjectiveProvider RebalanceObjectiveProvider,
) *replicaSplitConfig {
	return &replicaSplitConfig{
		randSource:                 split.GlobalRandSource(),
		rebalanceObjectiveProvider: rebalanceObjectiveProvider,
		st:                         st,
	}
}

// SplitObjective returns the current split objective. Currently this tracks
// 1:1 to the rebalance objective e.g. balancing QPS means also load based
// splitting on QPS.
func (c *replicaSplitConfig) SplitObjective() SplitObjective {
	obj := c.rebalanceObjectiveProvider.Objective()
	switch obj {
	case LBRebalancingQueries:
		return SplitQPS
	case LBRebalancingCPU:
		return SplitCPU
	default:
		panic(errors.AssertionFailedf("Unkown split objective %d", obj))
	}
}

// NewLoadBasedSplitter returns a new LoadBasedSplitter that may be used to
// find the midpoint based on recorded load.
func (c *replicaSplitConfig) NewLoadBasedSplitter(startTime time.Time) split.LoadBasedSplitter {
	obj := c.SplitObjective()
	switch obj {
	case SplitQPS:
		return split.NewUnweightedFinder(startTime, c.randSource)
	case SplitCPU:
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
func (c *replicaSplitConfig) StatThreshold() float64 {
	obj := c.SplitObjective()
	switch obj {
	case SplitQPS:
		return float64(SplitByLoadQPSThreshold.Get(&c.st.SV))
	case SplitCPU:
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

type loadSplitStat struct {
	max float64
	ok  bool
	typ SplitObjective
}

func (r *Replica) loadSplitStat(ctx context.Context) loadSplitStat {
	max, ok := r.loadBasedSplitter.MaxStat(ctx, r.Clock().PhysicalTime())
	lss := loadSplitStat{
		max: max,
		ok:  ok,
		typ: r.store.splitConfig.SplitObjective(),
	}
	return lss
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
	if r.store.splitConfig.SplitObjective() == SplitQPS {
		stat = len(ba.Requests)
	}

	shouldInitSplit := r.loadBasedSplitter.Record(ctx, timeutil.Now(), stat, func() roachpb.Span {
		return getResponseBoundarySpan(ba, br)
	})
	if shouldInitSplit {
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
	}
}
