// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// SplitByLoadEnabled wraps "kv.range_split.by_load.enabled".
var SplitByLoadEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.range_split.by_load_enabled",
	"allow automatic splits of ranges based on where load is concentrated",
	true,
	settings.WithName("kv.range_split.by_load.enabled"),
	settings.WithPublic)

// SplitByLoadQPSThreshold wraps "kv.range_split.load_qps_threshold".
var SplitByLoadQPSThreshold = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.range_split.load_qps_threshold",
	"the QPS over which, the range becomes a candidate for load based splitting",
	2500, // 2500 req/s
	settings.WithPublic)

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
	settings.SystemOnly,
	"kv.range_split.load_cpu_threshold",
	"the CPU use per second over which, the range becomes a candidate for load based splitting",
	500*time.Millisecond,
	// We enforce a minimum because of recursive splitting that may occur if
	// the threshold is set too low. There is a fixed CPU overhead for a
	// replica. At the moment no split key will be produced unless there are
	// more than 100 samples (batch requests) to that replica, however the
	// memory overhead of tracking split keys in split/weighted_finder.go is
	// noticeable and a finder is created after exceeding this threshold.
	settings.DurationWithMinimum(10*time.Millisecond),
	settings.WithPublic,
)

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

// loadSplitKey returns a suggested load split key for the range if it exists,
// otherwise it returns nil. If there were any errors encountered when
// validating the split key, the error is returned as well. It is guaranteed
// that the key returned, if non-nil, will be greater than the start key of the
// range and also within the range bounds.
//
// NOTE: The returned split key CAN BE BETWEEN A SQL ROW, The split key
// returned should only be used to engage a split via adminSplitWithDescriptor
// where findFirstSafeKey is set to true.
func (r *Replica) loadSplitKey(ctx context.Context, now time.Time) roachpb.Key {
	var splitKey roachpb.Key
	if overrideFn := r.store.cfg.TestingKnobs.LoadBasedSplittingOverrideKey; overrideFn != nil {
		var useSplitKey bool
		if splitKey, useSplitKey = overrideFn(r.GetRangeID()); useSplitKey {
			return splitKey
		}
	} else {
		splitKey = r.loadBasedSplitter.MaybeSplitKey(ctx, now)
	}

	if splitKey == nil {
		return nil
	}

	// If the splitKey belongs to a Table range, try and shorten the key to just
	// the row prefix. This allows us to check that splitKey doesn't map to the
	// first key of the range here. If the split key contains column families, it
	// is possible that the full key is strictly after every existing key for
	// that row. e.g. for a table row where the table ID is 100, index ID is 1,
	// primary key is a, and the column family ID is 3 (length=1):
	//
	//   splitKey = /Table/100/1/"a"/3/1
	//   existing = [..., /Table/100/1/"a"/2/1]
	//
	// We would not split at /Table/100/1/"a" as there's no key >= the splitKey
	// in the range.
	//
	// NB: We handle unsafe split keys in replica.adminSplitWithDescriptor, so it
	// isn't an issue if we return an unsafe key here. See the case where
	// findFirstSafeKey is true.
	if keyRowPrefix, err := keys.EnsureSafeSplitKey(splitKey); err == nil {
		splitKey = keyRowPrefix
	}

	// We swallow the error here and instead log an event. It is currently
	// expected that the load based splitter may return the start key of the
	// range.
	if err := splitKeyPreCheck(r.Desc().RSpan(), splitKey); err != nil {
		log.KvDistribution.VEventf(ctx, 1, "suggested load split key not usable: %s", err)
		return nil
	}

	return splitKey
}

// splitKeyPreCheck checks that a split key is addressable and not the same as
// the start key. An error is returned if these are not true. Additional checks
// are made in adminSplitWithDescriptor when a split request is processed by
// the replica.
func splitKeyPreCheck(rspan roachpb.RSpan, splitKey roachpb.Key) error {
	splitRKey, err := keys.Addr(splitKey)
	if err != nil {
		return err
	}

	// If the split key is equal to the start key of the range, it is treated as
	// a no-op in adminSplitWithDescriptor, however it is treated as an error
	// here because we shouldn't be suggesting split keys that are identical to
	// the start key of the range.
	if splitRKey.Equal(rspan.Key) {
		return errors.Errorf(
			"split key is equal to range start key (split_key=%s)",
			splitRKey)
	}

	return nil
}
