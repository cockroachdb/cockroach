// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	ioGrantCoordinatorToReturnQuota = metric.Metadata{
		Name:        "admission.io_grant_coordinator.to_return",
		Help:        "Outstanding messages to return to remote senders",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	ioGrantCoordinatorAvailableQuota = metric.Metadata{
		Name:        "admission.io_grant_coordinator.available_quota",
		Help:        "Available quota across all store pairs",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	ioGrantCoordinatorQuotaAddition = metric.Metadata{
		Name:        "admission.io_grant_coordinator.quota_addition",
		Help:        "Quota added all store pairs",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	ioGrantCoordinatorQuotaDeduction = metric.Metadata{
		Name:        "admission.io_grant_coordinator.quota_deducted",
		Help:        "Quota deducted all store pairs",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	ioGrantCoordinatorTotalCount = metric.Metadata{
		Name:        "admission.io_grant_coordinator.total_count",
		Help:        "Total number of quota pools, across all store pairs",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	ioGrantCoordinatorDepletedCount = metric.Metadata{
		Name:        "admission.io_grant_coordinator.depleted_count",
		Help:        "Quota pools (across all store pairs) with no remaining quota",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	ioGrantCoordinatorWaiting = metric.Metadata{
		Name:        "admission.io_grant_coordinator.waiting",
		Help:        "Total number of requests waiting for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	ioGrantCoordinatorAdmitted = metric.Metadata{
		Name:        "admission.io_grant_coordinator.admitted",
		Help:        "Total number of requests admitted for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	ioGrantCoordinatorErrored = metric.Metadata{
		Name:        "admission.io_grant_coordinator.errored",
		Help:        "Total number of requests admitted for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	ioGrantCoordinatorWaitingDuration = metric.Metadata{
		Name:        "admission.io_grant_coordinator.waiting_duration",
		Help:        "Total number of requests waiting for flow tokens",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

type ioGrantCoordinatorMetrics struct {
	WaitingRequests  [1 + numWorkClasses]*metric.Gauge
	WaitingDuration  [1 + numWorkClasses]*metric.Histogram
	AdmittedRequests [numWorkClasses]*metric.Counter
	ErroredRequests  [numWorkClasses]*metric.Counter
	QuotaAdded       [numWorkClasses]*metric.Counter
	QuotaDeducted    [numWorkClasses]*metric.Counter
	AvailableQuota   [numWorkClasses]*metric.Gauge
	ToReturnQuota    [numWorkClasses]*metric.Gauge
	DepletedCount    [numWorkClasses]*metric.Gauge
	TotalCount       *metric.Gauge
}

var _ metric.Struct = &ioGrantCoordinatorMetrics{}

func newIOGrantCoordinatorMetrics(i *IOGrantCoordinator) *ioGrantCoordinatorMetrics {
	metrics := &ioGrantCoordinatorMetrics{}
	for _, wc := range []workClass{regularWorkClass, elasticWorkClass} {
		wc := wc // copy loop variable
		metrics.WaitingRequests[wc] = metric.NewGauge(
			addName("_"+wc.String(), ioGrantCoordinatorWaiting),
		)
		metrics.AdmittedRequests[wc] = metric.NewCounter(
			addName("_"+wc.String(), ioGrantCoordinatorAdmitted),
		)
		metrics.ErroredRequests[wc] = metric.NewCounter(
			addName("_"+wc.String(), ioGrantCoordinatorErrored),
		)
		metrics.WaitingDuration[wc] = metric.NewHistogram(
			addName("_"+wc.String(), ioGrantCoordinatorWaitingDuration),
			base.DefaultHistogramWindowInterval(),
			metric.IOLatencyBuckets,
		)
		metrics.QuotaAdded[wc] = metric.NewCounter(
			addName("_"+wc.String(), ioGrantCoordinatorQuotaAddition),
		)
		metrics.QuotaDeducted[wc] = metric.NewCounter(
			addName("_"+wc.String(), ioGrantCoordinatorQuotaDeduction),
		)
		metrics.AvailableQuota[wc] = metric.NewFunctionalGauge(
			addName("_"+wc.String(), ioGrantCoordinatorAvailableQuota),
			func() int64 {
				sum := int64(0)
				i.mu.Lock()
				defer i.mu.Unlock()
				for _, wbc := range i.mu.writeBurstCapacities {
					sum += wbc.m[wc]
				}
				return sum
			},
		)
		metrics.ToReturnQuota[wc] = metric.NewFunctionalGauge(
			addName("_"+wc.String(), ioGrantCoordinatorToReturnQuota),
			func() int64 {
				sum := int64(0)
				i.outboundMu.Lock()
				defer i.outboundMu.Unlock()
				for _, nodeReturns := range i.outboundMu.toReturnV2 {
					for _, highestEntryByWorkPri := range nodeReturns {
						for pri := range highestEntryByWorkPri {
							if wc != workClassFromPri(pri) {
								continue
							}
							sum += 1
						}
					}
				}
				return sum
			},
		)
		metrics.DepletedCount[wc] = metric.NewFunctionalGauge(
			addName("_"+wc.String(), ioGrantCoordinatorDepletedCount),
			func() int64 {
				count := int64(0)
				i.mu.Lock()
				defer i.mu.Unlock()
				for _, wbc := range i.mu.writeBurstCapacities {
					if wbc.m[wc] <= 0 {
						count += 1
					}
				}
				return count
			},
		)
	}
	metrics.WaitingRequests[numWorkClasses] = metric.NewGauge(ioGrantCoordinatorWaiting)
	metrics.WaitingDuration[numWorkClasses] = metric.NewHistogram(
		ioGrantCoordinatorWaitingDuration,
		base.DefaultHistogramWindowInterval(),
		metric.IOLatencyBuckets,
	)
	metrics.TotalCount = metric.NewFunctionalGauge(
		ioGrantCoordinatorTotalCount,
		func() int64 {
			i.mu.Lock()
			defer i.mu.Unlock()
			return int64(len(i.mu.writeBurstCapacities))
		},
	)
	return metrics
}

func (i *ioGrantCoordinatorMetrics) incWaitingRequests(class workClass) {
	i.WaitingRequests[class].Inc(1)
	i.WaitingRequests[numWorkClasses].Inc(1)
}

func (i *ioGrantCoordinatorMetrics) decWaitingRequests(class workClass) {
	i.AdmittedRequests[class].Inc(1)
	i.WaitingRequests[class].Dec(1)
	i.WaitingRequests[numWorkClasses].Dec(1)
}

func (i *ioGrantCoordinatorMetrics) recordWaitingDuration(class workClass, dur time.Duration) {
	i.WaitingDuration[class].RecordValue(dur.Nanoseconds())
	i.WaitingDuration[numWorkClasses].RecordValue(dur.Nanoseconds())
}

// MetricStruct implements the metric.Struct interface.
func (k *ioGrantCoordinatorMetrics) MetricStruct() {}

type IOGrantCoordinator struct {
	mu struct {
		syncutil.Mutex
		writeBurstCapacities map[TenantStoreTuple]writesBurstCapacity // lazily instantiated
	}
	maxBurstCapacity map[workClass]int64

	localNodeID roachpb.NodeID // to short circuit
	outboundMu  struct {
		syncutil.Mutex
		localStores map[roachpb.StoreID]struct{} // to short circuit

		// Store we're returning to => <Tenant,Store> bucket held there that
		// we're returning to => Segmented by priority => flow tokens being
		// returned.
		toReturn map[roachpb.StoreID]map[TenantStoreTuple]map[admissionpb.WorkPriority]int64

		// Node we're returning logical admission to => <Range,Store> we're
		// returning it for => Segmented by priority => term+index up until
		// which work was logically admitted.
		toReturnV2 map[roachpb.NodeID]map[RangeStoreTuple]map[admissionpb.WorkPriority]TermIndexTuple
	}

	FlowTokenTrackerFinder FlowTokenTrackerFinder
	metrics                *ioGrantCoordinatorMetrics
}

type RangeStoreTuple struct {
	roachpb.RangeID
	roachpb.StoreID
}

type TermIndexTuple struct {
	Term  uint64
	Index uint64
}

func (t TermIndexTuple) String() string {
	return fmt.Sprintf("entry=%d/%d", t.Term, t.Index)
}

func (t TermIndexTuple) Equal(o TermIndexTuple) bool {
	return t.Term == o.Term && t.Index == o.Index
}

func (t TermIndexTuple) Less(o TermIndexTuple) bool {
	if t.Term != o.Term {
		return t.Term < o.Term
	}
	return t.Index < o.Index
}

func (t TermIndexTuple) LessEq(o TermIndexTuple) bool {
	return t.Less(o) || t.Equal(o)
}

func newIOGrantCoordinator(registry *metric.Registry) *IOGrantCoordinator {
	i := &IOGrantCoordinator{
		maxBurstCapacity: map[workClass]int64{
			regularWorkClass: 16 << 20, // 16 MiB
			elasticWorkClass: 8 << 20,  // 8 MiB
		},
	}
	i.metrics = newIOGrantCoordinatorMetrics(i)
	registry.AddMetricStruct(i.metrics)

	i.mu.writeBurstCapacities = make(map[TenantStoreTuple]writesBurstCapacity)
	i.outboundMu.toReturnV2 = make(map[roachpb.NodeID]map[RangeStoreTuple]map[admissionpb.WorkPriority]TermIndexTuple)
	i.outboundMu.localStores = make(map[roachpb.StoreID]struct{})
	return i
}

func (i *IOGrantCoordinator) WaitForBurstCapacity(
	ctx context.Context, ts TenantStoreTuple, pri admissionpb.WorkPriority, createTime int64,
) error {
	class := workClassFromPri(pri)

	i.metrics.incWaitingRequests(class)
	defer i.metrics.decWaitingRequests(class)

	tBegin := timeutil.Now()
	defer func() {
		i.metrics.recordWaitingDuration(class, timeutil.Since(tBegin))
	}()

	for {
		i.mu.Lock()
		c := i.getWriteBurstCapacityLocked(ts)
		capacity := c.m[class]
		i.mu.Unlock()

		if capacity > 0 {
			log.VInfof(ctx, 1, "flow tokens available (%s capacity=%d, duration=%s, pri=%s create-time=%d)", ts, capacity, timeutil.Since(tBegin), pri, createTime)
			select {
			case c.ch <- struct{}{}: // signal the next waiter, if any
			default:
			}
			return nil
		}

		log.VInfof(ctx, 1, "waiting for flow tokens (%s capacity=%d, duration=%s, pri=%s create-time=%d)", ts, capacity, timeutil.Since(tBegin), pri, createTime)
		select {
		case <-c.ch: // wait for a signal
		case <-ctx.Done():
			if ctx.Err() != nil {
				i.metrics.ErroredRequests[class].Inc(1)
			}
			return ctx.Err()
		}
	}
}

func (i *IOGrantCoordinator) ResetBurstCapacity() {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, c := range i.mu.writeBurstCapacities {
		for class, maxC := range i.maxBurstCapacity {
			c.m[class] = maxC
		}

		select {
		case c.ch <- struct{}{}: // signal the next waiter, if any
		default:
		}
	}
}

func (i *IOGrantCoordinator) AdjustBurstCapacity(
	ctx context.Context,
	ts TenantStoreTuple,
	pri admissionpb.WorkPriority,
	delta int64,
	entry TermIndexTuple,
) {
	class := workClassFromPri(pri)

	i.mu.Lock()
	defer i.mu.Unlock()

	c := i.getWriteBurstCapacityLocked(ts)
	beforeRegular, beforeElastic := c.m[regularWorkClass], c.m[elasticWorkClass]

	// XXX: XXX: XXX: Stop enforcing the ceiling under test builds. Under
	// production builds, also enforce some floor (-ve 16 MiB for regular, -24
	// MiB for elastic).
	switch class {
	case regularWorkClass:
		c.m[class] += delta

		if delta > 0 {
			c.m[class] = min(c.m[class], i.maxBurstCapacity[class]) // enforce ceiling

			// Second half of regular additions, also add to elastic.
			if c.m[regularWorkClass] > i.maxBurstCapacity[elasticWorkClass] {
				c.m[elasticWorkClass] += min(delta, c.m[regularWorkClass]-i.maxBurstCapacity[elasticWorkClass])
				c.m[elasticWorkClass] = min(c.m[elasticWorkClass], i.maxBurstCapacity[elasticWorkClass]) // enforce ceiling
			}
		} else {
			if beforeRegular > i.maxBurstCapacity[elasticWorkClass] {
				// First half of regular deduction, also deduct from elastic.
				// Allows elastic quota to go -ve.
				c.m[elasticWorkClass] -= min(beforeRegular-i.maxBurstCapacity[elasticWorkClass], -delta)
			}
		}
	case elasticWorkClass:
		// Elastic {deductions,additions} only affect elastic quota.
		c.m[class] += delta
		if delta > 0 {
			c.m[class] = min(c.m[class], i.maxBurstCapacity[class]) // enforce ceiling
		}
	}

	diffRegular := c.m[regularWorkClass] - beforeRegular
	diffElastic := c.m[elasticWorkClass] - beforeElastic
	if diffRegular < 0 {
		i.metrics.QuotaDeducted[regularWorkClass].Inc(-diffRegular)
	} else if diffRegular > 0 {
		i.metrics.QuotaAdded[regularWorkClass].Inc(diffRegular)
	}
	if diffElastic < 0 {
		i.metrics.QuotaDeducted[elasticWorkClass].Inc(-diffElastic)
	} else if diffElastic > 0 {
		i.metrics.QuotaAdded[elasticWorkClass].Inc(diffElastic)
	}

	if diffElastic > 0 || diffRegular > 0 {
		select {
		case c.ch <- struct{}{}: // signal a waiter, if any
		default:
		}
	}
	sign := ""
	if delta >= 0 {
		sign = "+"
	}
	log.VInfof(ctx, 1, "adjust flow tokens (%s pri=%s delta=%s%d %s): regular=%d (before=%d, diff=%d) elastic=%d (before=%d, diff=%d)",
		ts, pri, sign, delta, entry, c.m[regularWorkClass], beforeRegular, diffRegular, c.m[elasticWorkClass], beforeElastic, diffElastic)
}

type ReturnQuotaTracker interface {
	TrackQuotaToReturn(
		ctx context.Context,
		ts RangeStoreTuple,
		pri admissionpb.WorkPriority,
		returnTo roachpb.NodeID,
		entry TermIndexTuple,
	)
}

func (i *IOGrantCoordinator) SetNodeID(id roachpb.NodeID) {
	i.localNodeID = id
}

func (i *IOGrantCoordinator) TrackQuotaToReturn(
	ctx context.Context,
	rs RangeStoreTuple,
	pri admissionpb.WorkPriority,
	returnTo roachpb.NodeID,
	entry TermIndexTuple,
) {
	if i.localNodeID == returnTo {
		if log.V(1) {
			log.Infof(ctx, "(fast path) informing n%s of logical admission (r%d s%s pri=%s entry=%d/%d)",
				i.localNodeID, rs.RangeID, rs.StoreID, pri, entry.Term, entry.Index)
		}
		flowTokenTracker, found := i.FlowTokenTrackerFinder.GetFlowTokenTracker(ctx, rs.RangeID)
		if !found {
			log.VInfof(ctx, 1, "did not find token bucket to return to (was the proposer replica removed?)")
			return
		}
		flowTokenTracker.Release(ctx, rs.StoreID, pri, entry) // fast path
		return
	}

	i.outboundMu.Lock()
	defer i.outboundMu.Unlock()
	rangeStoreToWorkPriToTermIndexM, ok := i.outboundMu.toReturnV2[returnTo]
	if !ok {
		rangeStoreToWorkPriToTermIndexM = map[RangeStoreTuple]map[admissionpb.WorkPriority]TermIndexTuple{}
		i.outboundMu.toReturnV2[returnTo] = rangeStoreToWorkPriToTermIndexM
	}
	if _, ok := rangeStoreToWorkPriToTermIndexM[rs]; !ok {
		i.outboundMu.toReturnV2[returnTo][rs] = map[admissionpb.WorkPriority]TermIndexTuple{}
	}
	if oldTI := rangeStoreToWorkPriToTermIndexM[rs][pri]; !oldTI.LessEq(entry) {
		// XXX: XXX: XXX: There's a race between the fast path when IO tokens
		// are just grabbed, and the granter granting something that was queued.
		// Do something better than just ignoring it?
		log.Warningf(ctx, "unexpected regression in logically admitted entry for r%s s%s pri=%s source=n%d, expected %s < %s",
			rs.RangeID, rs.StoreID, pri, returnTo, oldTI, entry)
		return
	}
	rangeStoreToWorkPriToTermIndexM[rs][pri] = entry
}

func (i *IOGrantCoordinator) GetQuotaToReturnRemotely(
	ctx context.Context, returnTo roachpb.NodeID,
) (_ map[RangeStoreTuple]map[admissionpb.WorkPriority]TermIndexTuple, ok bool) {
	i.outboundMu.Lock()
	defer i.outboundMu.Unlock()

	rs, ok := i.outboundMu.toReturnV2[returnTo]
	delete(i.outboundMu.toReturnV2, returnTo)
	return rs, ok
}

func (i *IOGrantCoordinator) getWriteBurstCapacityLocked(ts TenantStoreTuple) writesBurstCapacity {
	c, ok := i.mu.writeBurstCapacities[ts]
	if !ok {
		c = newWritesBurstCapacity(
			i.maxBurstCapacity[regularWorkClass],
			i.maxBurstCapacity[elasticWorkClass],
		)
		i.mu.writeBurstCapacities[ts] = c
	}
	return c
}

func (i *IOGrantCoordinator) TestingGetWriteBurstCapacity(
	ts TenantStoreTuple,
) [numWorkClasses]int64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	var ret [numWorkClasses]int64
	for wc, c := range i.getWriteBurstCapacityLocked(ts).m {
		ret[wc] = c
	}
	return ret
}

func (i *IOGrantCoordinator) TestingGetWriteBurstCapacities() map[TenantStoreTuple][numWorkClasses]int64 {
	i.mu.Lock()
	defer i.mu.Unlock()

	ret := make(map[TenantStoreTuple][numWorkClasses]int64)
	for ts, wbc := range i.mu.writeBurstCapacities {
		arr, ok := ret[ts]
		if !ok {
			arr = [numWorkClasses]int64{}
		}
		for wc, c := range wbc.m {
			arr[wc] = c
		}
		ret[ts] = arr
	}
	return ret
}

type TenantStoreTuple struct {
	roachpb.TenantID
	roachpb.StoreID
}

func (t TenantStoreTuple) String() string {
	tenantSt := t.TenantID.String()
	if t.TenantID.IsSystem() {
		tenantSt = "1"
	}
	return fmt.Sprintf("tenant-store=t%s/s%s", tenantSt, t.StoreID.String())
}

type writesBurstCapacity struct {
	m  map[workClass]int64
	ch chan struct{}
}

func newWritesBurstCapacity(regular, elastic int64) writesBurstCapacity {
	return writesBurstCapacity{
		m: map[workClass]int64{
			regularWorkClass: regular,
			elasticWorkClass: elastic,
		},
		ch: make(chan struct{}, 1),
	}
}

type replicationAdmissionWorkHandleKey struct{}

type ReplicationAdmissionWorkHandle struct {
	Priority           admissionpb.WorkPriority
	IOGrantCoordinator *IOGrantCoordinator
	OriginalRangeID    roachpb.RangeID // XXX: Remove.
	FlowTokenTracker   FlowTokenTracker
}

type FlowTokenTrackerFinder interface {
	GetFlowTokenTracker(context.Context, roachpb.RangeID) (tracker FlowTokenTracker, found bool)
}

type FlowTokenTracker interface {
	TrackReplicaMuLocked(ctx context.Context, storeID roachpb.StoreID, pri admissionpb.WorkPriority, count int64, entry TermIndexTuple, originalRangeID roachpb.RangeID) bool
	Release(ctx context.Context, storeID roachpb.StoreID, pri admissionpb.WorkPriority, entry TermIndexTuple)
}

func (h *ReplicationAdmissionWorkHandle) Proposed(
	ctx context.Context,
	tenID roachpb.TenantID,
	storeIDs []roachpb.StoreID,
	flowTokenTracker FlowTokenTracker,
	size int64,
	entry TermIndexTuple,
) {
	if h == nil {
		return
	}
	for _, storeID := range storeIDs {
		tracked := flowTokenTracker.TrackReplicaMuLocked(ctx, storeID, h.Priority, size, entry, h.OriginalRangeID)
		if tracked {
			h.IOGrantCoordinator.AdjustBurstCapacity(ctx, TenantStoreTuple{TenantID: tenID, StoreID: storeID}, h.Priority, -size, entry)
		}
		// XXX: XXX: XXX: This is weird. If we're not able to track because
		// we haven't initialized the tracker yet, we're still actually
		// replicating a below-raft-admission proposal encoding. So we'll
		// get attempts to release this quota afterwards. When initializing
		// the tracker, do we also want to initialize it with the raft log
		// index? So all these ghost-returns get discarded? Or can we figure
		// out a better story for initialization of the flow tracker.
	}
}

// ContextWithReplicationAdmissionHandle returns a Context wrapping the supplied
// store work handle, if any.
func ContextWithReplicationAdmissionHandle(
	ctx context.Context, h *ReplicationAdmissionWorkHandle,
) context.Context {
	if h == nil {
		return ctx
	}
	return context.WithValue(ctx, replicationAdmissionWorkHandleKey{}, h)
}

// ReplicationAdmissionHandleFromContext returns the store work handle contained
// in the Context, if any.
func ReplicationAdmissionHandleFromContext(ctx context.Context) *ReplicationAdmissionWorkHandle {
	val := ctx.Value(replicationAdmissionWorkHandleKey{})
	h, ok := val.(*ReplicationAdmissionWorkHandle)
	if !ok {
		return nil
	}
	return h
}

// XXX: Overall notes.
// - Measure the overhead of below-raft admission metadata being sent along
//   every raft command. Use kv100 with small batches and block size.
// - [x] Plug quota pool leak. Looks like timeseries data stops being able to go
//   through. Log context from the requests that do acquire continuously in the
//   steady state, see where they're coming from. Looks like it was bypassed
//   admission work. Investigate why that didn't return quota.
// - What are some leaktest like tests we can add end-to-end? Run some workload,
//   stop running it, disable replication admission control, and see quota pools
//   return back to where they were? Induce chaos (dead nodes, lease transfers,
//   rebalancing, etc.).
// - Rename the Deprecated... stuff for raft encodings. We'll use that encoding
//   when there's no AC data to be encoded. Obviates a migration.
// - Double check how we're doing physical token estimations/deductions. We
//   don't have the full split as written originally.
// - How do we calculate the write throughput I can drive over a WAN link and a
//   given "gate quota". Does the request size matter? The bandwidth of the pipe.
//   1GB/s link over 200ms = 200MB in flight to the LSM.
// - Write tests for all this, with concurrency. Look at the concurrency
//   package for how Nathan tested that. Want to see "grant chains" in action.
// - Implement "tentatively deducted" to prevent over-admission/improve interop
//   with slot queuing that comes after. Since deduction from this pool happens
//   after proposal evaluation, we need to be careful to not over-admit at this
//   point, lest our burst budget be completely bypassed. We can use an estimate
//   here to make sure we're not admitting significantly more than we should.
//   Once the request has passed the proposal evaluation stage and actually
//   deducted, we can drop it from our "between gate and prop-eval inflight
//   estimate". If we had over-estimated, we can try and grant another request
//   right then. We don't need to do anything if we had under estimated.
// - Use aggmetrics in IOGrantCoordinator metrics, segmented by tenants.
// - Need to prune records from the writeBurstCapacities map as tenants/stores
//   disappear, or stores 'fail' (whatever that means), or tenants get
//   added/removed. Perhaps even other conditions. Some 'sweeper' thing. Maybe
//   default to sweeping non-aggressively with a few direct hints.
// - When returning flow control tokens either to the local or remote sender, do
//   it through an interface. Both should look identical in code, backed by
//   interface impls.
// - Document what would happen if flow control tokens were augmented with a
//   work queue. Needs interface addition to support `granted()` to a specific
//   tenant. Or `hasWaitingRequests` for a specific tenant. So its able to peek
//   into the heap.
// - Note that the worst case burst will be reduced once we do copysets.
// - In the single node, or rf=1x case, everything should look the way it
//   does today. i.e. we have a WorkQueue for the LSM that's consuming logical
//   IO tokens by which it's enabling intra-tenant isolation and inter-tenant
//   prioritization. These tokens, when consumed, are re-added to the
//   ioGrantCoordinator which permits more write work.
// - Ideally we don't introduce yet-another-grant-coordinator type and duplicate
//   code dealing with the corresponding work queue. This is the same problem with
//   the ElasticCPUGranter. Consolidate. Pick a better name, since we have an
//   kvStoreTokenGranter. Something something "burst"? Or "replicated write"? Or
//   "flow tokens"? Not correct to call this "inflight tracker". It's the
//   available window, or "burst", or "burst budget", or "receive window" (from
//   TCP), or "{send,usable} window". But it's not a sliding window since we're
//   not tracking indexes that are being rolled over. There's no refill rate,
//   we're working with just the burst. Talk about what we would do if we had
//   perfect information with zero delay. This ability to burst lets us avoid
//   the remote prediction problem and avoid the risk of under/over utilizing.
//   Maybe call this flow tokens instead. Flow tokens have no
//   auto-replenishment, they're returned at the point where the flow
//   terminates. They're originally held at the sender.
// - For the writeBurstQuota map, see whether we should use sync.Map, or a
//   generified version of syncutil.IntMap.
// - There's no way to control exactly how much C is returned from proposals. We
//   set it to zero by setting the encoding bit. Maybe that's an inflexible API,
//   and instead should allow passing the C back and forth?
// - What happens if we periodically reset the pool on the sender side? Due to
//   dropped msgappresps for ex. Worst case we get re-adds for extant
//   quota, and it raises it back up to some limit (16MB). The re-add will
//   nillify itself, either if it gets re-added when the pool is full, or by
//   nullifying a subsequent re-add. It can contribute to a higher burst than
//   16MB (imagine quick successions of resets).
// - Check how the entry.Size() compares to the actual message itself. We
//   don't want the linear model multipliers to go too high.
// - Pull out protobuf changes into a single PR.
// - [Sumeer] Maybe have an AsyncAdmit on the work queue itself. And the
//   notions of physical/logical admission is only relevant at the store work
//   queue levels. Trying to keep work queue as {cpu,store,sql} agnostic as
//   possible, keeping custom logic at the StoreWorkQueue type instead.
// - We no longer need per-work estimation for below-raft admission. For
//   only-below-raft admission, the L0 {write,ingest} linear models don't need a
//   constant term since follower writes would be adequately integrated.
// - Document that "admin" commands like splits, merges, lease transfers bypass
//   all AC queues, and also don't use replication admission control.
// - Reset/reconstruct quota pools during failures, etc. Make sure we see no
//   leaks during lease transfers and configuration changes.
//   - Use some sort of sweeper maybe as a last resort? Defense-in-depth thing?
// - Document why we'd want to use replication admission control partially, say
//   only for elastic traffic. How latency profile is felt due to taking flow
//   control tokens at the outset. If we never get good enough at timely failure
//   detection, or detection of network jitters, etc (remember that as soon as
//   16MB of writes get used up towards a follower store, we can cause write
//   stalls), maybe we can explore the whole idea of not taking flow tokens away
//   unless we know logical admission is being deferred, i.e. traffic shaping is
//   needed. So we're not subject to latency perturbations unless overloaded. We
//   could only use this protocol ("activate sender quota pool only once
//   informed of the need by a receiver") for the regular quota pools where we
//   are indeed latency sensitive.
// - Introduce a cluster setting, or something, and use an onChange trigger to
//   reset all TDS completely.
// - Document what we could do if we had a workqueue-like thing on top of the
//   TDS-level, to improve granularity of intra-tenant prioritization, improve
//   create-time ordering, and epoch-LIFO.
// - Rename this to just StoreWorkDoneInfor to "StoreWork". Get rid of the
//   kvadmission alias. Not sure it's giving us much.

// Using benchstat for before/after replication admission control proto changes:
//
//   ۩ dev bench pkg/kv/kvserver \
//  	--filter BenchmarkReplicaProposal/bytes=256_B,withFollower=true \
// 		-v --stream-output --ignore-cache --bench-time=5s --count 5
//   goos: darwin
//   goarch: arm64
//   ...
//
//   name                                              old time/op    new time/op    delta
//   ReplicaProposal/bytes=256_B,withFollower=true-10    45.4µs ± 2%    45.1µs ± 1%   ~     (p=1.000 n=5+5)
//
//   name                                              old speed      new speed      delta
//   ReplicaProposal/bytes=256_B,withFollower=true-10  5.65MB/s ± 3%  5.67MB/s ± 1%   ~     (p=0.905 n=5+5)
//
//   name                                              old alloc/op   new alloc/op   delta
//   ReplicaProposal/bytes=256_B,withFollower=true-10    26.3kB ± 1%    26.6kB ± 1%   ~     (p=0.056 n=5+5)
//
//   name                                              old allocs/op  new allocs/op  delta
//   ReplicaProposal/bytes=256_B,withFollower=true-10       129 ± 1%       130 ± 1%   ~     (p=0.206 n=5+5)
