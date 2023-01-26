// Copyright 2021 The Cockroach Authors.
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
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
)

// Use of the admission control package spans the SQL and KV layers. When
// running in a multi-tenant setting, we have per-tenant SQL-only servers and
// multi-tenant storage servers. These multi-tenant storage servers contain
// the multi-tenant KV layer, and the SQL layer for the system tenant. Most of
// the following settings are relevant to both kinds of servers (except for
// KVAdmissionControlEnabled). Only the system tenant can modify these
// settings in the storage servers, while a regular tenant can modify these
// settings for their SQL-only servers. Which is why these are typically
// TenantWritable.

// KVAdmissionControlEnabled controls whether KV server-side admission control
// is enabled.
var KVAdmissionControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv.enabled",
	"when true, work performed by the KV layer is subject to admission control",
	true).WithPublic()

// KVBulkOnlyAdmissionControlEnabled controls whether user (normal and above
// priority) work is subject to admission control. If it is set to true, then
// user work will not be throttled by admission control but bulk work still will
// be. This setting is a preferable alternative to completely disabling
// admission control. It can be used reactively in cases where index backfill,
// schema modifications or other bulk operations are causing high latency due to
// io_overload on nodes.
// TODO(baptist): Find a better solution to this in v23.1.
var KVBulkOnlyAdmissionControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv.bulk_only.enabled",
	"when both admission.kv.enabled and this is true, only throttle bulk work",
	false)

// SQLKVResponseAdmissionControlEnabled controls whether response processing
// in SQL, for KV requests, is enabled.
var SQLKVResponseAdmissionControlEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"admission.sql_kv_response.enabled",
	"when true, work performed by the SQL layer when receiving a KV response is subject to "+
		"admission control",
	true).WithPublic()

// SQLSQLResponseAdmissionControlEnabled controls whether response processing
// in SQL, for DistSQL requests, is enabled.
var SQLSQLResponseAdmissionControlEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"admission.sql_sql_response.enabled",
	"when true, work performed by the SQL layer when receiving a DistSQL response is subject "+
		"to admission control",
	true).WithPublic()

var admissionControlEnabledSettings = [numWorkKinds]*settings.BoolSetting{
	KVWork:             KVAdmissionControlEnabled,
	SQLKVResponseWork:  SQLKVResponseAdmissionControlEnabled,
	SQLSQLResponseWork: SQLSQLResponseAdmissionControlEnabled,
}

// KVTenantWeightsEnabled controls whether tenant weights are enabled for KV
// admission control. This setting has no effect if admission.kv.enabled is
// false.
var KVTenantWeightsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv.tenant_weights.enabled",
	"when true, tenant weights are enabled for KV admission control",
	false).WithPublic()

// KVStoresTenantWeightsEnabled controls whether tenant weights are enabled
// for KV-stores admission control. This setting has no effect if
// admission.kv.enabled is false.
var KVStoresTenantWeightsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.kv.stores.tenant_weights.enabled",
	"when true, tenant weights are enabled for KV-stores admission control",
	false).WithPublic()

// EpochLIFOEnabled controls whether the adaptive epoch-LIFO scheme is enabled
// for admission control. Is only relevant when the above admission control
// settings are also set to true. Unlike those settings, which are granular
// for each kind of admission queue, this setting applies to all the queues.
// This is because we recommend that all those settings be enabled or none be
// enabled, and we don't want to carry forward unnecessarily granular
// settings.
var EpochLIFOEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"admission.epoch_lifo.enabled",
	"when true, epoch-LIFO behavior is enabled when there is significant delay in admission",
	false).WithPublic()

var epochLIFOEpochDuration = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"admission.epoch_lifo.epoch_duration",
	"the duration of an epoch, for epoch-LIFO admission control ordering",
	epochLength,
	func(v time.Duration) error {
		if v < time.Millisecond {
			return errors.Errorf("epoch-LIFO: epoch duration is too small")
		}
		return nil
	}).WithPublic()

var epochLIFOEpochClosingDeltaDuration = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"admission.epoch_lifo.epoch_closing_delta_duration",
	"the delta duration before closing an epoch, for epoch-LIFO admission control ordering",
	epochClosingDelta,
	func(v time.Duration) error {
		if v < time.Millisecond {
			return errors.Errorf("epoch-LIFO: epoch closing delta is too small")
		}
		return nil
	}).WithPublic()

var epochLIFOQueueDelayThresholdToSwitchToLIFO = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"admission.epoch_lifo.queue_delay_threshold_to_switch_to_lifo",
	"the queue delay encountered by a (tenant,priority) for switching to epoch-LIFO ordering",
	maxQueueDelayToSwitchToLifo,
	func(v time.Duration) error {
		if v < time.Millisecond {
			return errors.Errorf("epoch-LIFO: queue delay threshold is too small")
		}
		return nil
	}).WithPublic()

// WorkInfo provides information that is used to order work within an
// WorkQueue. The WorkKind is not included as a field since an WorkQueue deals
// with a single WorkKind.
type WorkInfo struct {
	// TenantID is the id of the tenant. For single-tenant clusters, this will
	// always be the SystemTenantID.
	TenantID roachpb.TenantID
	// Priority is utilized within a tenant.
	Priority admissionpb.WorkPriority
	// CreateTime is equivalent to Time.UnixNano() at the creation time of this
	// work or a parent work (e.g. could be the start time of the transaction,
	// if this work was created as part of a transaction). It is used to order
	// work within a (TenantID, Priority) pair -- earlier CreateTime is given
	// preference.
	CreateTime int64
	// BypassAdmission allows the work to bypass admission control, but allows
	// for it to be accounted for. Ignored unless TenantID is the
	// SystemTenantID. It should be used for high-priority intra-KV work, and
	// when KV work generates other KV work (to avoid deadlock). Ignored
	// otherwise.
	BypassAdmission bool

	// Optional information specified only for WorkQueues where the work is tied
	// to a range. This allows queued work to return early as soon as the range
	// is no longer in a relevant state at this node. Currently only KVWork is
	// tied to a range.
	// TODO(sumeer): use these in the WorkQueue implementation.

	// RangeID is the range at which this work must be performed. Optional (see
	// comment above).
	RangeID roachpb.RangeID
	// RequiresLeaseholder is true iff the work requires the leaseholder.
	// Optional (see comment above).
	RequiresLeaseholder bool

	// For internal use by wrapper classes. The requested tokens or slots.
	requestedCount int64
}

// WorkQueue maintains a queue of work waiting to be admitted. Ordering of
// work is achieved via 2 heaps: a tenant heap orders the tenants with waiting
// work in increasing order of used slots or tokens, optionally adjusted by
// tenant weights. Within each tenant, the waiting work is ordered based on
// priority and create time. Tenants with non-zero values of used slots or
// tokens are tracked even if they have no more waiting work. Token usage is
// reset to zero every second. The choice of 1 second of memory for token
// distribution fairness is somewhat arbitrary. The same 1 second interval is
// also used to garbage collect tenants who have no waiting requests and no
// used slots or tokens.
//
// Usage example:
//
//	var grantCoord *GrantCoordinator
//	<initialize grantCoord>
//	kvQueue := grantCoord.GetWorkQueue(KVWork)
//	<hand kvQueue to the code that does kv server work>
//
//	// Before starting some kv server work
//	if enabled, err := kvQueue.Admit(ctx, WorkInfo{TenantID: tid, ...}); err != nil {
//	  return err
//	}
//	<do the work>
//	if enabled {
//	  kvQueue.AdmittedWorkDone(tid)
//	}
type WorkQueue struct {
	ambientCtx  context.Context
	workKind    WorkKind
	granter     granter
	usesTokens  bool
	tiedToRange bool
	settings    *cluster.Settings

	// Prevents more than one caller to be in Admit and calling tryGet or adding
	// to the queue. It allows WorkQueue to release mu before calling tryGet and
	// be assured that it is not competing with another Admit.
	// Lock ordering is admitMu < mu.
	admitMu syncutil.Mutex
	mu      struct {
		syncutil.Mutex
		// Tenants with waiting work.
		tenantHeap tenantHeap
		// All tenants, including those without waiting work. Periodically cleaned.
		tenants       map[uint64]*tenantInfo
		tenantWeights struct {
			mu syncutil.Mutex
			// active refers to the currently active weights. mu is held for updates
			// to the inactive weights, to prevent concurrent updates. After
			// updating the inactive weights, it is made active by swapping with
			// active, while also holding WorkQueue.mu. Therefore, reading
			// tenantWeights.active does not require tenantWeights.mu. For lock
			// ordering, tenantWeights.mu precedes WorkQueue.mu.
			//
			// The maps are lazily allocated.
			active, inactive map[uint64]uint32
		}
		// The highest epoch that is closed.
		closedEpochThreshold int64
		// Following values are copied from the cluster settings.
		epochLengthNanos            int64
		epochClosingDeltaNanos      int64
		maxQueueDelayToSwitchToLifo time.Duration
	}
	logThreshold log.EveryN
	metrics      *WorkQueueMetrics
	stopCh       chan struct{}

	timeSource timeutil.TimeSource
}

var _ requester = &WorkQueue{}

type workQueueOptions struct {
	usesTokens  bool
	tiedToRange bool

	// timeSource can be set to non-nil for tests. If nil,
	// the timeutil.DefaultTimeSource will be used.
	timeSource timeutil.TimeSource
	// The epoch closing goroutine can be disabled for tests.
	disableEpochClosingGoroutine bool
}

func makeWorkQueueOptions(workKind WorkKind) workQueueOptions {
	switch workKind {
	case KVWork:
		// CPU bound KV work uses tokens. We also use KVWork for the per-store
		// queues, which use tokens -- the caller overrides the usesTokens value
		// in that case.
		return workQueueOptions{usesTokens: false, tiedToRange: true}
	case SQLKVResponseWork, SQLSQLResponseWork:
		return workQueueOptions{usesTokens: true, tiedToRange: false}
	case SQLStatementLeafStartWork, SQLStatementRootStartWork:
		return workQueueOptions{usesTokens: false, tiedToRange: false}
	default:
		panic(errors.AssertionFailedf("unexpected workKind %d", workKind))
	}
}

func makeWorkQueue(
	ambientCtx log.AmbientContext,
	workKind WorkKind,
	granter granter,
	settings *cluster.Settings,
	metrics *WorkQueueMetrics,
	opts workQueueOptions,
) requester {
	q := &WorkQueue{}
	initWorkQueue(q, ambientCtx, workKind, granter, settings, metrics, opts)
	return q
}

func initWorkQueue(
	q *WorkQueue,
	ambientCtx log.AmbientContext,
	workKind WorkKind,
	granter granter,
	settings *cluster.Settings,
	metrics *WorkQueueMetrics,
	opts workQueueOptions,
) {
	stopCh := make(chan struct{})

	timeSource := opts.timeSource
	if timeSource == nil {
		timeSource = timeutil.DefaultTimeSource{}
	}

	q.ambientCtx = ambientCtx.AnnotateCtx(context.Background())
	q.workKind = workKind
	q.granter = granter
	q.usesTokens = opts.usesTokens
	q.tiedToRange = opts.tiedToRange
	q.settings = settings
	q.logThreshold = log.Every(5 * time.Minute)
	q.metrics = metrics
	q.stopCh = stopCh
	q.timeSource = timeSource

	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.mu.tenants = make(map[uint64]*tenantInfo)
		q.sampleEpochLIFOSettingsLocked()
	}()
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				q.gcTenantsAndResetTokens()
			case <-stopCh:
				// Channel closed.
				return
			}
		}
	}()
	q.tryCloseEpoch(q.timeNow())
	if !opts.disableEpochClosingGoroutine {
		q.startClosingEpochs()
	}
}

func isInTenantHeap(tenant *tenantInfo) bool {
	// If there is some waiting work, this tenant is in tenantHeap.
	return len(tenant.waitingWorkHeap) > 0 || len(tenant.openEpochsHeap) > 0
}

func (q *WorkQueue) timeNow() time.Time {
	return q.timeSource.Now()
}

func (q *WorkQueue) epochLIFOEnabled() bool {
	return EpochLIFOEnabled.Get(&q.settings.SV)
}

// Samples the latest cluster settings for epoch-LIFO.
func (q *WorkQueue) sampleEpochLIFOSettingsLocked() {
	epochLengthNanos := int64(epochLIFOEpochDuration.Get(&q.settings.SV))
	if epochLengthNanos != q.mu.epochLengthNanos {
		// Reset what is closed. A proper closed value will be calculated when the
		// next epoch closes. This ensures that if we are increasing the epoch
		// length, we will regress what epoch number is closed. Meanwhile, all
		// work subject to LIFO queueing will get queued in the openEpochsHeap,
		// which is fine (we admit from there too).
		q.mu.closedEpochThreshold = 0
	}
	q.mu.epochLengthNanos = epochLengthNanos
	q.mu.epochClosingDeltaNanos = int64(epochLIFOEpochClosingDeltaDuration.Get(&q.settings.SV))
	q.mu.maxQueueDelayToSwitchToLifo = epochLIFOQueueDelayThresholdToSwitchToLIFO.Get(&q.settings.SV)
}

func (q *WorkQueue) startClosingEpochs() {
	go func() {
		// If someone sets the epoch length to a huge value by mistake, we will
		// still sample every second, so that we can adjust when they fix their
		// mistake.
		const maxTimerDur = time.Second
		// This is the min duration we set the timer for, to avoid setting smaller
		// and smaller timers, in case the timer fires slightly early.
		const minTimerDur = time.Millisecond
		var timer *time.Timer
		for {
			q.mu.Lock()
			q.sampleEpochLIFOSettingsLocked()
			nextCloseTime := q.nextEpochCloseTimeLocked()
			q.mu.Unlock()
			timeNow := q.timeNow()
			timerDur := nextCloseTime.Sub(timeNow)
			if timerDur > 0 {
				if timerDur > maxTimerDur {
					timerDur = maxTimerDur
				} else if timerDur < minTimerDur {
					timerDur = minTimerDur
				}
				if timer == nil {
					timer = time.NewTimer(timerDur)
				} else {
					timer.Reset(timerDur)
				}
				select {
				case <-timer.C:
				case <-q.stopCh:
					// Channel closed.
					return
				}
			} else {
				q.tryCloseEpoch(timeNow)
			}
		}
	}()
}

func (q *WorkQueue) nextEpochCloseTimeLocked() time.Time {
	// +2 since we need to advance the threshold by 1, and another 1 since the
	// epoch closes at its end time.
	timeUnixNanos :=
		(q.mu.closedEpochThreshold+2)*q.mu.epochLengthNanos + q.mu.epochClosingDeltaNanos
	return timeutil.Unix(0, timeUnixNanos)
}

func (q *WorkQueue) tryCloseEpoch(timeNow time.Time) {
	epochLIFOEnabled := q.epochLIFOEnabled()
	q.mu.Lock()
	defer q.mu.Unlock()
	epochClosingTimeNanos := timeNow.UnixNano() - q.mu.epochLengthNanos - q.mu.epochClosingDeltaNanos
	epoch := epochForTimeNanos(epochClosingTimeNanos, q.mu.epochLengthNanos)
	if epoch <= q.mu.closedEpochThreshold {
		return
	}
	q.mu.closedEpochThreshold = epoch
	doLog := q.logThreshold.ShouldLog()
	for _, tenant := range q.mu.tenants {
		prevThreshold := tenant.fifoPriorityThreshold
		tenant.fifoPriorityThreshold =
			tenant.priorityStates.getFIFOPriorityThresholdAndReset(
				tenant.fifoPriorityThreshold, q.mu.epochLengthNanos, q.mu.maxQueueDelayToSwitchToLifo)
		if !epochLIFOEnabled {
			tenant.fifoPriorityThreshold = int(admissionpb.LowPri)
		}
		if tenant.fifoPriorityThreshold != prevThreshold || doLog {
			logVerb := "is"
			if tenant.fifoPriorityThreshold != prevThreshold {
				logVerb = "changed to"
			}
			// TODO(sumeer): export this as a per-tenant metric somehow. We could
			// start with this being a per-WorkQueue metric for only the system
			// tenant. However, currently we share metrics across WorkQueues --
			// specifically all the store WorkQueues share the same metric. We
			// should eliminate that sharing and make those per store metrics.
			log.Infof(q.ambientCtx, "%s: FIFO threshold for tenant %d %s %d",
				workKindString(q.workKind), tenant.id, logVerb, tenant.fifoPriorityThreshold)
		}
		// Note that we are ignoring the new priority threshold and only
		// dequeueing the ones that are in the closed epoch. It is possible to
		// have work items that are not in the closed epoch and whose priority
		// makes them no longer subject to LIFO, but they will need to wait here
		// until their epochs close. This is considered acceptable since the
		// priority threshold should not fluctuate rapidly.
		for len(tenant.openEpochsHeap) > 0 {
			work := tenant.openEpochsHeap[0]
			if work.epoch > epoch {
				break
			}
			heap.Pop(&tenant.openEpochsHeap)
			heap.Push(&tenant.waitingWorkHeap, work)
		}
	}
}

// Admit is called when requesting admission for some work. If err!=nil, the
// request was not admitted, potentially due to the deadline being exceeded.
// The enabled return value is relevant when err=nil, and represents whether
// admission control is enabled. AdmittedWorkDone must be called iff
// enabled=true && err!=nil, and the WorkKind for this queue uses slots.
func (q *WorkQueue) Admit(ctx context.Context, info WorkInfo) (enabled bool, err error) {
	enabledSetting := admissionControlEnabledSettings[q.workKind]
	if enabledSetting != nil && !enabledSetting.Get(&q.settings.SV) {
		return false, nil
	}
	if info.requestedCount == 0 {
		// Callers from outside the admission package don't set requestedCount --
		// these are implicitly requesting a count of 1.
		info.requestedCount = 1
	}
	if !q.usesTokens && info.requestedCount != 1 {
		panic(errors.AssertionFailedf("unexpected requestedCount: %d", info.requestedCount))
	}
	q.metrics.incRequested(info.Priority)
	tenantID := info.TenantID.ToUint64()

	// The code in this method does not use defer to unlock the mutexes because
	// it needs the flexibility of selectively unlocking one of these on a
	// certain code path. When changing the code, be careful in making sure the
	// mutexes are properly unlocked on all code paths.
	q.admitMu.Lock()
	q.mu.Lock()
	tenant, ok := q.mu.tenants[tenantID]
	if !ok {
		tenant = newTenantInfo(tenantID, q.getTenantWeightLocked(tenantID))
		q.mu.tenants[tenantID] = tenant
	}
	if info.BypassAdmission && roachpb.IsSystemTenantID(tenantID) && q.workKind == KVWork {
		tenant.used += uint64(info.requestedCount)
		if isInTenantHeap(tenant) {
			q.mu.tenantHeap.fix(tenant)
		}
		q.mu.Unlock()
		q.admitMu.Unlock()
		q.granter.tookWithoutPermission(info.requestedCount)
		q.metrics.incAdmitted(info.Priority)
		return true, nil
	}
	// Work is subject to admission control.

	// Tell priorityStates about this received work. We don't tell it about work
	// that has bypassed admission control, since priorityStates is deciding the
	// threshold for LIFO queueing based on observed admission latency.
	tenant.priorityStates.requestAtPriority(info.Priority)

	if len(q.mu.tenantHeap) == 0 {
		// Fast-path. Try to grab token/slot.
		// Optimistically update used to avoid locking again.
		tenant.used += uint64(info.requestedCount)
		q.mu.Unlock()
		if q.granter.tryGet(info.requestedCount) {
			q.admitMu.Unlock()
			q.metrics.incAdmitted(info.Priority)
			return true, nil
		}
		// Did not get token/slot.
		//
		// There is a race here: before q.mu is acquired, the granter could
		// experience a reduction in load and call
		// WorkQueue.hasWaitingRequests to see if it should grant, but since
		// there is nothing in the queue that method will return false. Then the
		// work here queues up even though granter has spare capacity. We could
		// add additional synchronization (and complexity to the granter
		// interface) to deal with this, by keeping the granter's lock
		// (GrantCoordinator.mu) locked when returning from tryGrant and call
		// granter again to release that lock after this work has been queued. But
		// it has the downside of extending the scope of GrantCoordinator.mu.
		// Instead we tolerate this race in the knowledge that GrantCoordinator
		// will periodically, at a high frequency, look at the state of the
		// requesters to see if there is any queued work that can be granted
		// admission.
		q.mu.Lock()
		prevTenant := tenant
		// The tenant could have been removed when using tokens. See the comment
		// where the tenantInfo struct is declared.
		tenant, ok = q.mu.tenants[tenantID]
		if !q.usesTokens {
			if !ok || prevTenant != tenant {
				panic("prev tenantInfo no longer in map")
			}
			if tenant.used < uint64(info.requestedCount) {
				panic(errors.AssertionFailedf("tenant.used %d < info.requestedCount %d",
					tenant.used, info.requestedCount))
			}
			tenant.used -= uint64(info.requestedCount)
		} else {
			if !ok {
				tenant = newTenantInfo(tenantID, q.getTenantWeightLocked(tenantID))
				q.mu.tenants[tenantID] = tenant
			}
			// Don't want to overflow tenant.used if it is already 0 because of
			// being reset to 0 by the GC goroutine.
			if tenant.used >= uint64(info.requestedCount) {
				tenant.used -= uint64(info.requestedCount)
			}
		}
	}
	// Check for cancellation.
	startTime := q.timeNow()
	if ctx.Err() != nil {
		// Already canceled. More likely to happen if cpu starvation is
		// causing entering into the work queue to be delayed.
		q.mu.Unlock()
		q.admitMu.Unlock()
		q.metrics.incErrored(info.Priority)
		deadline, _ := ctx.Deadline()
		return true,
			errors.Newf("work %s deadline already expired: deadline: %v, now: %v",
				workKindString(q.workKind), deadline, startTime)
	}
	// Push onto heap(s).
	ordering := fifoWorkOrdering
	if int(info.Priority) < tenant.fifoPriorityThreshold {
		ordering = lifoWorkOrdering
	}
	work := newWaitingWork(info.Priority, ordering, info.CreateTime, info.requestedCount, startTime, q.mu.epochLengthNanos)
	inTenantHeap := isInTenantHeap(tenant)
	if work.epoch <= q.mu.closedEpochThreshold || ordering == fifoWorkOrdering {
		heap.Push(&tenant.waitingWorkHeap, work)
	} else {
		heap.Push(&tenant.openEpochsHeap, work)
	}
	if !inTenantHeap {
		heap.Push(&q.mu.tenantHeap, tenant)
	}
	// Else already in tenantHeap.

	// Release all locks and start waiting.
	q.mu.Unlock()
	q.admitMu.Unlock()

	q.metrics.recordStartWait(info.Priority)
	defer releaseWaitingWork(work)
	select {
	case <-ctx.Done():
		waitDur := q.timeNow().Sub(startTime)
		q.mu.Lock()
		// The work was cancelled, so waitDur is less than the wait time this work
		// would have encountered if it actually waited until admission. However,
		// this lower bound is still useful for calculating the FIFO=>LIFO switch
		// since it is possible that all work at this priority is exceeding the
		// deadline and being cancelled. The risk here is that if the deadlines
		// are too short, we could underestimate the actual wait time.
		tenant.priorityStates.updateDelayLocked(work.priority, waitDur, true /* canceled */)
		if work.heapIndex == -1 {
			// No longer in heap. Raced with token/slot grant.
			if !q.usesTokens {
				if tenant.used < uint64(info.requestedCount) {
					panic(errors.AssertionFailedf("tenant.used %d < info.requestedCount %d",
						tenant.used, info.requestedCount))
				}
				tenant.used -= uint64(info.requestedCount)
			}
			// Else, we don't decrement tenant.used since we don't want to race with
			// the gc goroutine that will set used=0.
			q.mu.Unlock()
			q.granter.returnGrant(info.requestedCount)
			// The channel is sent to after releasing mu, so we don't need to hold
			// mu when receiving from it. Additionally, we've already called
			// returnGrant so we're not holding back future grant chains if this one
			// chain gets terminated.
			chainID := <-work.ch
			q.granter.continueGrantChain(chainID)
		} else {
			if work.inWaitingWorkHeap {
				tenant.waitingWorkHeap.remove(work)
			} else {
				tenant.openEpochsHeap.remove(work)
			}
			if !isInTenantHeap(tenant) {
				q.mu.tenantHeap.remove(tenant)
			}
			q.mu.Unlock()
		}
		q.metrics.incErrored(info.Priority)
		q.metrics.recordFinishWait(info.Priority, waitDur)
		deadline, _ := ctx.Deadline()
		log.Eventf(ctx, "deadline expired, waited in %s queue for %v",
			workKindString(q.workKind), waitDur)
		return true,
			errors.Newf("work %s deadline expired while waiting: deadline: %v, start: %v, dur: %v",
				workKindString(q.workKind), deadline, startTime, waitDur)
	case chainID, ok := <-work.ch:
		if !ok {
			panic(errors.AssertionFailedf("channel should not be closed"))
		}
		q.metrics.incAdmitted(info.Priority)
		waitDur := q.timeNow().Sub(startTime)
		q.metrics.recordFinishWait(info.Priority, waitDur)
		if work.heapIndex != -1 {
			panic(errors.AssertionFailedf("grantee should be removed from heap"))
		}
		log.Eventf(ctx, "admitted, waited in %s queue for %v", workKindString(q.workKind), waitDur)
		q.granter.continueGrantChain(chainID)
		return true, nil
	}
}

// AdmittedWorkDone is used to inform the WorkQueue that some admitted work is
// finished. It must be called iff the WorkKind of this WorkQueue uses slots
// (not tokens), i.e., KVWork, SQLStatementLeafStartWork,
// SQLStatementRootStartWork.
func (q *WorkQueue) AdmittedWorkDone(tenantID roachpb.TenantID) {
	if q.usesTokens {
		panic(errors.AssertionFailedf("tokens should not be returned"))
	}
	// Single slot is allocated for the work.
	q.mu.Lock()
	tenant, ok := q.mu.tenants[tenantID.ToUint64()]
	if !ok {
		panic(errors.AssertionFailedf("tenant not found"))
	}
	tenant.used--
	if isInTenantHeap(tenant) {
		q.mu.tenantHeap.fix(tenant)
	}
	q.mu.Unlock()
	q.granter.returnGrant(1)
}

func (q *WorkQueue) hasWaitingRequests() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.mu.tenantHeap) > 0
}

func (q *WorkQueue) granted(grantChainID grantChainID) int64 {
	// Reduce critical section by getting time before mutex acquisition.
	now := q.timeNow()
	q.mu.Lock()
	if len(q.mu.tenantHeap) == 0 {
		q.mu.Unlock()
		return 0
	}
	tenant := q.mu.tenantHeap[0]
	var item *waitingWork
	if len(tenant.waitingWorkHeap) > 0 {
		item = heap.Pop(&tenant.waitingWorkHeap).(*waitingWork)
	} else {
		item = heap.Pop(&tenant.openEpochsHeap).(*waitingWork)
	}
	waitDur := now.Sub(item.enqueueingTime)
	tenant.priorityStates.updateDelayLocked(item.priority, waitDur, false /* canceled */)
	tenant.used += uint64(item.requestedCount)
	if isInTenantHeap(tenant) {
		q.mu.tenantHeap.fix(tenant)
	} else {
		q.mu.tenantHeap.remove(tenant)
	}
	// Get the value of requestedCount before releasing the mutex, since after
	// releasing Admit can notice that item is no longer in the heap and call
	// releaseWaitingWork to return item to the waitingWorkPool.
	requestedCount := item.requestedCount
	q.mu.Unlock()
	// Reduce critical section by sending on channel after releasing mutex.
	item.ch <- grantChainID
	return requestedCount
}

func (q *WorkQueue) gcTenantsAndResetTokens() {
	q.mu.Lock()
	defer q.mu.Unlock()
	// With large numbers of active tenants, this iteration could hold the lock
	// longer than desired. We could break this iteration into smaller parts if
	// needed.
	for id, info := range q.mu.tenants {
		if info.used == 0 && !isInTenantHeap(info) {
			delete(q.mu.tenants, id)
			releaseTenantInfo(info)
		} else if q.usesTokens {
			info.used = 0
			// All the heap members will reset used=0, so no need to change heap
			// ordering.
		}
	}
}

// adjustTenantTokens is used internally by StoreWorkQueue. The
// additionalTokens count can be negative, in which case it is returning
// tokens. This is only for WorkQueue's own accounting -- it should not call
// into granter.
func (q *WorkQueue) adjustTenantTokens(tenantID roachpb.TenantID, additionalTokens int64) {
	tid := tenantID.ToUint64()
	q.mu.Lock()
	defer q.mu.Unlock()
	tenant, ok := q.mu.tenants[tid]
	if ok {
		if additionalTokens < 0 {
			toReturn := uint64(-additionalTokens)
			if tenant.used < toReturn {
				tenant.used = 0
			} else {
				tenant.used -= toReturn
			}
		} else {
			tenant.used += uint64(additionalTokens)
		}
	}
}

func (q *WorkQueue) String() string {
	return redact.StringWithoutMarkers(q)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (q *WorkQueue) SafeFormat(s redact.SafePrinter, _ rune) {
	q.mu.Lock()
	defer q.mu.Unlock()
	s.Printf("closed epoch: %d ", q.mu.closedEpochThreshold)
	s.Printf("tenantHeap len: %d", len(q.mu.tenantHeap))
	if len(q.mu.tenantHeap) > 0 {
		s.Printf(" top tenant: %d", q.mu.tenantHeap[0].id)
	}
	var ids []uint64
	for id := range q.mu.tenants {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		tenant := q.mu.tenants[id]
		s.Printf("\n tenant-id: %d used: %d, w: %d, fifo: %d", tenant.id, tenant.used,
			tenant.weight, tenant.fifoPriorityThreshold)
		if len(tenant.waitingWorkHeap) > 0 {
			s.Printf(" waiting work heap:")
			for i := range tenant.waitingWorkHeap {
				var workOrdering string
				if tenant.waitingWorkHeap[i].arrivalTimeWorkOrdering == lifoWorkOrdering {
					workOrdering = ", lifo-ordering"
				}
				s.Printf(" [%d: pri: %d, ct: %d, epoch: %d, qt: %d%s]", i,
					tenant.waitingWorkHeap[i].priority,
					tenant.waitingWorkHeap[i].createTime/int64(time.Millisecond),
					tenant.waitingWorkHeap[i].epoch,
					tenant.waitingWorkHeap[i].enqueueingTime.UnixNano()/int64(time.Millisecond), workOrdering)
			}
		}
		if len(tenant.openEpochsHeap) > 0 {
			s.Printf(" open epochs heap:")
			for i := range tenant.openEpochsHeap {
				s.Printf(" [%d: pri: %d, ct: %d, epoch: %d, qt: %d]", i,
					tenant.openEpochsHeap[i].priority,
					tenant.openEpochsHeap[i].createTime/int64(time.Millisecond),
					tenant.openEpochsHeap[i].epoch,
					tenant.openEpochsHeap[i].enqueueingTime.UnixNano()/int64(time.Millisecond))
			}
		}
	}
}

// Weight for tenants that are not assigned a weight. This typically applies
// to tenants which weren't on this node in the prior call to
// SetTenantWeights. Additionally, it is also the minimum tenant weight.
const defaultTenantWeight = 1

// The current cap on the weight of a tenant. We don't allow a single tenant
// to use more than cap times the number of resources of the smallest tenant.
// For KV slots, we have seen a range of slot counts from 50-200 for 16 cpu
// nodes, for a KV50 workload, depending on how we set
// admission.kv_slot_adjuster.overload_threshold. We don't want to starve
// small tenants, so the cap is currently set to 20. A more sophisticated fair
// sharing scheme would not need such a cap.
const tenantWeightCap = 20

func (q *WorkQueue) getTenantWeightLocked(tenantID uint64) uint32 {
	weight, ok := q.mu.tenantWeights.active[tenantID]
	if !ok {
		weight = defaultTenantWeight
	}
	return weight
}

// SetTenantWeights sets the weight of tenants, using the provided tenant ID
// => weight map. A nil map will result in all tenants having the same weight.
func (q *WorkQueue) SetTenantWeights(tenantWeights map[uint64]uint32) {
	q.mu.tenantWeights.mu.Lock()
	defer q.mu.tenantWeights.mu.Unlock()
	if q.mu.tenantWeights.inactive == nil {
		q.mu.tenantWeights.inactive = make(map[uint64]uint32)
	}
	// Remove all elements from the inactive map.
	for k := range q.mu.tenantWeights.inactive {
		delete(q.mu.tenantWeights.inactive, k)
	}
	// Compute the max weight in the new map, for enforcing the tenantWeightCap.
	maxWeight := uint32(1)
	for _, v := range tenantWeights {
		if v > maxWeight {
			maxWeight = v
		}
	}
	scaling := float64(1)
	if maxWeight > tenantWeightCap {
		scaling = tenantWeightCap / float64(maxWeight)
	}
	// Populate the weights in the inactive map.
	for k, v := range tenantWeights {
		w := uint32(math.Ceil(float64(v) * scaling))
		if w < defaultTenantWeight {
			w = defaultTenantWeight
		}
		q.mu.tenantWeights.inactive[k] = w
	}
	q.mu.Lock()
	// Establish the new active map.
	q.mu.tenantWeights.active, q.mu.tenantWeights.inactive =
		q.mu.tenantWeights.inactive, q.mu.tenantWeights.active
	// Create a slice for storing all the tenantIDs. We use this to split the
	// update to the data-structures that require holding q.mu, in case there
	// are 1000s of tenants (we don't want to hold q.mu for long durations).
	tenantIDs := make([]uint64, len(q.mu.tenants))
	i := 0
	for k := range q.mu.tenants {
		tenantIDs[i] = k
		i++
	}
	q.mu.Unlock()
	// Any tenants not in tenantIDs will see the latest weight when their
	// tenantInfo is created. The existing ones need their weights to be
	// updated.

	// tenantIDs[index] represents the next tenantID that needs to be updated.
	var index int
	n := len(tenantIDs)
	// updateNextBatch acquires q.mu and updates a batch of tenants.
	updateNextBatch := func() (repeat bool) {
		q.mu.Lock()
		defer q.mu.Unlock()
		// Arbitrary batch size of 5.
		const batchSize = 5
		for i := 0; i < batchSize; i++ {
			if index >= n {
				return false
			}
			tenantID := tenantIDs[index]
			tenantInfo := q.mu.tenants[tenantID]
			weight := q.getTenantWeightLocked(tenantID)
			if tenantInfo != nil && tenantInfo.weight != weight {
				tenantInfo.weight = weight
				if isInTenantHeap(tenantInfo) {
					q.mu.tenantHeap.fix(tenantInfo)
				}
			}
			index++
		}
		return true
	}
	for updateNextBatch() {
	}
}

// close tells the gc goroutine to stop.
func (q *WorkQueue) close() {
	close(q.stopCh)
}

type workOrderingKind int8

const (
	fifoWorkOrdering workOrderingKind = iota
	lifoWorkOrdering
)

type priorityState struct {
	priority admissionpb.WorkPriority
	// maxQueueDelay includes the delay of both successfully admitted and
	// canceled requests.
	//
	// NB: The maxQueueDelay value is an incomplete picture of delay since it
	// does not have visibility into work that is still waiting in the queue.
	// However, since we use the maxQueueDelay across a collection of priorities
	// to set a priority threshold, we expect that usually there will be some
	// work just below the priority threshold that does dequeue (with high
	// latency) -- if not, it is likely that the next high priority is actually
	// the one experiencing some instances of high latency. That is, it is very
	// unlikely to be the case that a certain priority sees admission with no
	// high latency while the next lower priority never gets work dequeued
	// because of resource saturation.
	maxQueueDelay time.Duration
	// Count of requests that were successfully admitted (not canceled). This is
	// used in concert with lowestPriorityWithRequests to detect priorities
	// where work was queued but nothing was successfully admitted.
	admittedCount int
}

// priorityStates tracks information about admission requests and admission
// grants at various priorities. It is used to set a priority threshold for
// LIFO queuing. There is one priorityStates per tenant, since it is embedded
// in a tenantInfo.
type priorityStates struct {
	// In increasing order of priority. Expected to not have more than 10
	// elements, so a linear search is fast. The slice is emptied after each
	// epoch is closed.
	ps                         []priorityState
	lowestPriorityWithRequests int
}

// makePriorityStates returns an empty priorityStates, that reuses the
// ps slice.
func makePriorityStates(ps []priorityState) priorityStates {
	return priorityStates{ps: ps[:0], lowestPriorityWithRequests: admissionpb.OneAboveHighPri}
}

// requestAtPriority is called when a request is received at the given
// priority.
func (ps *priorityStates) requestAtPriority(priority admissionpb.WorkPriority) {
	if int(priority) < ps.lowestPriorityWithRequests {
		ps.lowestPriorityWithRequests = int(priority)
	}
}

// updateDelayLocked is called with the delay experienced by work at the given
// priority. This is used to compute priorityState.maxQueueDelay. Canceled
// indicates whether the request was canceled while waiting in the queue, or
// successfully admitted.
func (ps *priorityStates) updateDelayLocked(
	priority admissionpb.WorkPriority, delay time.Duration, canceled bool,
) {
	i := 0
	n := len(ps.ps)
	for ; i < n; i++ {
		pri := ps.ps[i].priority
		if pri == priority {
			if !canceled {
				ps.ps[i].admittedCount++
			}
			if ps.ps[i].maxQueueDelay < delay {
				ps.ps[i].maxQueueDelay = delay
			}
			return
		}
		if pri > priority {
			break
		}
	}
	admittedCount := 1
	if canceled {
		admittedCount = 0
	}
	state := priorityState{priority: priority, maxQueueDelay: delay, admittedCount: admittedCount}
	if i == n {
		ps.ps = append(ps.ps, state)
	} else {
		ps.ps = append(ps.ps[:i+1], ps.ps[i:]...)
		ps.ps[i] = state
	}
}

func (ps *priorityStates) getFIFOPriorityThresholdAndReset(
	curPriorityThreshold int, epochLengthNanos int64, maxQueueDelayToSwitchToLifo time.Duration,
) int {
	// priority is monotonically increasing in the calculation below.
	priority := int(admissionpb.LowPri)
	foundLowestPriority := false
	handlePriorityState := func(p priorityState) {
		if p.maxQueueDelay > maxQueueDelayToSwitchToLifo {
			// LIFO.
			priority = int(p.priority) + 1
		} else if int(p.priority) < curPriorityThreshold {
			// Currently LIFO. If the delay is above some fraction of the threshold,
			// we continue as LIFO. If the delay is below that fraction, we could
			// have a situation where requests were made at this priority but
			// nothing was admitted -- we continue with LIFO in that case too.
			if p.maxQueueDelay > time.Duration(epochLengthNanos)/10 ||
				(p.admittedCount == 0 && int(p.priority) >= ps.lowestPriorityWithRequests) {
				priority = int(p.priority) + 1
			}
			// Else, can switch to FIFO, at least based on queue delay at this
			// priority. But we examine the other higher priorities too, since it is
			// possible that few things were received for this priority and it got
			// lucky in getting them admitted.
		}
	}
	for i := range ps.ps {
		p := ps.ps[i]
		if int(p.priority) == ps.lowestPriorityWithRequests {
			foundLowestPriority = true
		}
		handlePriorityState(p)
	}
	if !foundLowestPriority && ps.lowestPriorityWithRequests != admissionpb.OneAboveHighPri &&
		priority <= ps.lowestPriorityWithRequests {
		// The new threshold will cause lowestPriorityWithRequests to be FIFO, and
		// we know nothing exited admission control for this lowest priority.
		// Since !foundLowestPriority, we know we haven't explicitly considered
		// this priority in the above loop. So we consider it now.
		handlePriorityState(priorityState{
			priority:      admissionpb.WorkPriority(ps.lowestPriorityWithRequests),
			maxQueueDelay: 0,
			admittedCount: 0,
		})
	}
	ps.ps = ps.ps[:0]
	ps.lowestPriorityWithRequests = admissionpb.OneAboveHighPri
	return priority
}

// tenantInfo is the per-tenant information in the tenantHeap.
type tenantInfo struct {
	id uint64
	// The weight assigned to the tenant. Must be > 0.
	weight uint32
	// used can be the currently used slots, or the tokens granted within the last
	// interval.
	//
	// tenantInfo will not be GC'd until both used==0 and
	// len(waitingWorkHeap)==0.
	//
	// Note that used can be reset to 0 periodically, iff the WorkQueue is using
	// tokens (not slots). This creates a risk since callers of Admit hold
	// references to tenantInfo. We do not want a race condition where the
	// tenantInfo held in Admit is returned to the sync.Pool. Note that this
	// race is almost impossible to reproduce in practice since GC loop runs at
	// 1s intervals and needs two iterations to GC a tenantInfo -- first to
	// reset used=0 and then the next time to GC it. We fix this by being
	// careful in the code of Admit by not reusing a reference to tenantInfo,
	// and instead grab a new reference from the map.
	//
	// The above fix for the GC race condition is insufficient to prevent
	// overflow of the used field if the reset to used=0 happens between used++
	// and used-- within Admit. Properly fixing that would need to track the
	// count of used==0 resets and gate the used-- on the count not having
	// changed. This was considered unnecessarily complicated and instead we
	// simply (a) do not do used-- for the tokens case, if used is already zero,
	// or (b) do not do used-- for the tokens case if the request was canceled.
	// This does imply some inaccuracy in token counting -- it can be fixed if
	// needed.
	used            uint64
	waitingWorkHeap waitingWorkHeap
	openEpochsHeap  openEpochsHeap

	priorityStates priorityStates
	// priority >= fifoPriorityThreshold is FIFO. This uses a larger sized type
	// than WorkPriority since the threshold can be > MaxPri.
	fifoPriorityThreshold int

	// The heapIndex is maintained by the heap.Interface methods, and represents
	// the heapIndex of the item in the heap.
	heapIndex int
}

// tenantHeap is a heap of tenants with waiting work, ordered in increasing
// order of tenantInfo.used/tenantInfo.weight (weights are an optional
// feature, and default to 1). That is, we prefer tenants that are using less.
type tenantHeap []*tenantInfo

var _ heap.Interface = (*tenantHeap)(nil)

var tenantInfoPool = sync.Pool{
	New: func() interface{} {
		return &tenantInfo{}
	},
}

func newTenantInfo(id uint64, weight uint32) *tenantInfo {
	ti := tenantInfoPool.Get().(*tenantInfo)
	*ti = tenantInfo{
		id:                    id,
		weight:                weight,
		waitingWorkHeap:       ti.waitingWorkHeap,
		openEpochsHeap:        ti.openEpochsHeap,
		priorityStates:        makePriorityStates(ti.priorityStates.ps),
		fifoPriorityThreshold: int(admissionpb.LowPri),
		heapIndex:             -1,
	}
	return ti
}

func releaseTenantInfo(ti *tenantInfo) {
	if isInTenantHeap(ti) {
		panic("tenantInfo has non-empty heap")
	}
	// NB: {waitingWorkHeap,openEpochsHeap}.Pop nil the slice elements when
	// removing, so we are not inadvertently holding any references.
	if cap(ti.waitingWorkHeap) > 100 {
		ti.waitingWorkHeap = nil
	}
	if cap(ti.openEpochsHeap) > 100 {
		ti.openEpochsHeap = nil
	}

	*ti = tenantInfo{
		waitingWorkHeap: ti.waitingWorkHeap,
		openEpochsHeap:  ti.openEpochsHeap,
		priorityStates:  makePriorityStates(ti.priorityStates.ps),
	}
	tenantInfoPool.Put(ti)
}

func (th *tenantHeap) fix(item *tenantInfo) {
	heap.Fix(th, item.heapIndex)
}

func (th *tenantHeap) remove(item *tenantInfo) {
	heap.Remove(th, item.heapIndex)
}

func (th *tenantHeap) Len() int {
	return len(*th)
}

func (th *tenantHeap) Less(i, j int) bool {
	// used_i/weight_i < used_j/weight_j
	return (*th)[i].used*uint64((*th)[j].weight) < (*th)[j].used*uint64((*th)[i].weight)
}

func (th *tenantHeap) Swap(i, j int) {
	(*th)[i], (*th)[j] = (*th)[j], (*th)[i]
	(*th)[i].heapIndex = i
	(*th)[j].heapIndex = j
}

func (th *tenantHeap) Push(x interface{}) {
	n := len(*th)
	item := x.(*tenantInfo)
	item.heapIndex = n
	*th = append(*th, item)
}

func (th *tenantHeap) Pop() interface{} {
	old := *th
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.heapIndex = -1
	*th = old[0 : n-1]
	return item
}

// waitingWork is the per-work information in the waitingWorkHeap.
type waitingWork struct {
	priority admissionpb.WorkPriority
	// The workOrderingKind for this priority when this work was queued.
	arrivalTimeWorkOrdering workOrderingKind
	createTime              int64
	requestedCount          int64
	// epoch is a function of the createTime.
	epoch int64

	// ch is used to communicate a grant to the waiting goroutine. The
	// grantChainID is used by the waiting goroutine to call continueGrantChain.
	ch chan grantChainID
	// The heapIndex is maintained by the heap.Interface methods, and represents
	// the heapIndex of the item in the heap. -1 when not in the heap. The same
	// heapIndex is used by the waitingWorkHeap and the openEpochsHeap since a
	// waitingWork is only in one of them.
	heapIndex int
	// Set to true when added to waitingWorkHeap. Only used to disambiguate
	// which heap the waitingWork is in, when we know it is in one of the heaps.
	// The only state transition is from false => true, and never restored back
	// to false.
	inWaitingWorkHeap bool
	enqueueingTime    time.Time
}

var waitingWorkPool = sync.Pool{
	New: func() interface{} {
		return &waitingWork{}
	},
}

// The default epoch length for doing epoch-LIFO. The epoch-LIFO scheme relies
// on clock synchronization and the expectation that transaction/query
// deadlines will be significantly higher than execution time under low load.
// A standard LIFO scheme suffers from a severe problem when a single user
// transaction can result in many lower-level work that get distributed to
// many nodes, and previous work execution can result in new work being
// submitted for admission: the later work for a transaction may no longer be
// the latest seen by the system, so will not be preferred. This means LIFO
// would do some work items from each transaction and starve the remaining
// work, so nothing would complete. This is even worse than FIFO which at
// least prefers the same transactions until they are complete (FIFO and LIFO
// are using the transaction CreateTime, and not the work arrival time).
//
// Consider a case where transaction deadlines are 1s (note this may not
// necessarily be an actual deadline, and could be a time duration after which
// the user impact is extremely negative), and typical transaction execution
// times (under low load) of 10ms. A 100ms epoch will increase transaction
// latency to at most 100ms + 5ms + 10ms, since execution will not start until
// the epoch of the transaction's CreateTime is closed. At that time, due to
// clock synchronization, all nodes will start executing that epoch and will
// implicitly have the same set of competing transactions. By the time the
// next epoch closes and the current epoch's transactions are deprioritized,
// 100ms will have elapsed, which is enough time for most of these
// transactions that get admitted to have finished all their work.
//
// Note that LIFO queueing will only happen at bottleneck nodes, and decided
// on a (tenant, priority) basis. So if there is even a single bottleneck node
// for a (tenant, priority), the above delay will occur. When the epoch closes
// at the bottleneck node, the creation time for this transaction will be
// sufficiently in the past, so the non-bottleneck nodes (using FIFO) will
// prioritize it over recent transactions. Note that there is an inversion in
// that the non-bottleneck nodes are ordering in the opposite way for such
// closed epochs, but since they are not bottlenecked, the queueing delay
// should be minimal.
//
// These are defaults and can be overridden using cluster settings. Increasing
// the epoch length will cause the epoch number to decrease. This will cause
// some confusion in the ordering between work that was previously queued with
// a higher epoch number. We accept that temporary confusion (it will clear
// once old queued work is admitted or canceled). We do not try to maintain a
// monotonic epoch, based on the epoch number already in place before the
// change, since different nodes will see the cluster setting change at
// different times.

const epochLength = time.Millisecond * 100
const epochClosingDelta = time.Millisecond * 5

// Latency threshold for switching to LIFO queuing. Once we switch to LIFO,
// the minimum latency will be epochLenghNanos+epochClosingDeltaNanos, so it
// makes sense not to switch until the observed latency is around the same.
const maxQueueDelayToSwitchToLifo = epochLength + epochClosingDelta

func epochForTimeNanos(t int64, epochLengthNanos int64) int64 {
	return t / epochLengthNanos
}

func newWaitingWork(
	priority admissionpb.WorkPriority,
	arrivalTimeWorkOrdering workOrderingKind,
	createTime int64,
	requestedCount int64,
	enqueueingTime time.Time,
	epochLengthNanos int64,
) *waitingWork {
	ww := waitingWorkPool.Get().(*waitingWork)
	ch := ww.ch
	if ch == nil {
		ch = make(chan grantChainID, 1)
	}
	*ww = waitingWork{
		priority:                priority,
		arrivalTimeWorkOrdering: arrivalTimeWorkOrdering,
		createTime:              createTime,
		requestedCount:          requestedCount,
		epoch:                   epochForTimeNanos(createTime, epochLengthNanos),
		ch:                      ch,
		heapIndex:               -1,
		enqueueingTime:          enqueueingTime,
	}
	return ww
}

// releaseWaitingWork must be called with an empty waitingWork.ch.
func releaseWaitingWork(ww *waitingWork) {
	ch := ww.ch
	select {
	case <-ch:
		panic("channel must be empty and not closed")
	default:
	}
	*ww = waitingWork{
		ch: ch,
	}
	waitingWorkPool.Put(ww)
}

// waitingWorkHeap is a heap of waiting work within a tenant. It is ordered in
// decreasing order of priority, and within the same priority in increasing
// order of createTime (to prefer older work) for FIFO, and in decreasing
// order of createTime for LIFO. In the LIFO case the heap only contains
// epochs that are closed.
type waitingWorkHeap []*waitingWork

var _ heap.Interface = (*waitingWorkHeap)(nil)

func (wwh *waitingWorkHeap) remove(item *waitingWork) {
	heap.Remove(wwh, item.heapIndex)
}

func (wwh *waitingWorkHeap) Len() int { return len(*wwh) }

// Less does LIFO or FIFO ordering among work with the same priority. The
// ordering to use is specified by the arrivalTimeWorkOrdering. When
// transitioning from LIFO => FIFO or FIFO => LIFO, we can have work with
// different arrivalTimeWorkOrderings in the heap (for the same priority). In
// this case we err towards LIFO since this indicates a new or recent overload
// situation. If it was a recent overload that no longer exists, we will be
// able to soon drain these LIFO work items from the queue since they will get
// admitted. Erring towards FIFO has the danger that if we are transitioning
// to LIFO we will need to wait for those old queued items to be serviced
// first, which will delay the transition.
//
// Less is not strict weak ordering since the transitivity property is not
// satisfied in the presence of elements that have different values of
// arrivalTimeWorkOrdering. This is acceptable for heap maintenance.
// Example: Three work items with the same epoch where t1 < t2 < t3
//
//	w3: (fifo, create: t3, epoch: e)
//	w2: (lifo, create: t2, epoch: e)
//	w1: (fifo, create: t1, epoch: e)
//	w1 < w3, w3 < w2, w2 < w1, which is a cycle.
func (wwh *waitingWorkHeap) Less(i, j int) bool {
	if (*wwh)[i].priority == (*wwh)[j].priority {
		if (*wwh)[i].arrivalTimeWorkOrdering == lifoWorkOrdering ||
			(*wwh)[i].arrivalTimeWorkOrdering != (*wwh)[j].arrivalTimeWorkOrdering {
			// LIFO, and the epoch is closed, so can simply use createTime.
			return (*wwh)[i].createTime > (*wwh)[j].createTime
		}
		// FIFO.
		return (*wwh)[i].createTime < (*wwh)[j].createTime
	}
	return (*wwh)[i].priority > (*wwh)[j].priority
}

func (wwh *waitingWorkHeap) Swap(i, j int) {
	(*wwh)[i], (*wwh)[j] = (*wwh)[j], (*wwh)[i]
	(*wwh)[i].heapIndex = i
	(*wwh)[j].heapIndex = j
}

func (wwh *waitingWorkHeap) Push(x interface{}) {
	n := len(*wwh)
	item := x.(*waitingWork)
	item.heapIndex = n
	item.inWaitingWorkHeap = true
	*wwh = append(*wwh, item)
}

func (wwh *waitingWorkHeap) Pop() interface{} {
	old := *wwh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.heapIndex = -1
	*wwh = old[0 : n-1]
	return item
}

// openEpochsHeap is a heap of waiting work within a tenant that will be
// subject to LIFO ordering (when transferred to the waitingWorkHeap) and
// whose epoch is not yet closed. See the Less method for the ordering applied
// here.
type openEpochsHeap []*waitingWork

var _ heap.Interface = (*openEpochsHeap)(nil)

func (oeh *openEpochsHeap) remove(item *waitingWork) {
	heap.Remove(oeh, item.heapIndex)
}

func (oeh *openEpochsHeap) Len() int { return len(*oeh) }

// Less orders in increasing order of epoch, and within the same epoch, with
// decreasing priority and with the same priority with increasing CreateTime.
// It is not typically dequeued from to admit work, but if it is, it will
// behave close to the FIFO ordering in the waitingWorkHeap (not exactly FIFO
// because work items with higher priority can be later than those with lower
// priority if they have a higher epoch -- but epochs are coarse enough that
// this should not be a factor). This close-to-FIFO is preferable since
// dequeuing from this queue may be an indicator that the overload is going
// away. There is also a risk with this close-to-FIFO behavior if we rapidly
// fluctuate between overload and normal: doing FIFO here could cause
// transaction work to start but not finish because the rest of the work may
// be done using LIFO ordering. When an epoch closes, a prefix of this heap
// will be dequeued and added to the waitingWorkHeap.
func (oeh *openEpochsHeap) Less(i, j int) bool {
	if (*oeh)[i].epoch == (*oeh)[j].epoch {
		if (*oeh)[i].priority == (*oeh)[j].priority {
			return (*oeh)[i].createTime < (*oeh)[j].createTime
		}
		return (*oeh)[i].priority > (*oeh)[j].priority
	}
	return (*oeh)[i].epoch < (*oeh)[j].epoch
}

func (oeh *openEpochsHeap) Swap(i, j int) {
	(*oeh)[i], (*oeh)[j] = (*oeh)[j], (*oeh)[i]
	(*oeh)[i].heapIndex = i
	(*oeh)[j].heapIndex = j
}

func (oeh *openEpochsHeap) Push(x interface{}) {
	n := len(*oeh)
	item := x.(*waitingWork)
	item.heapIndex = n
	*oeh = append(*oeh, item)
}

func (oeh *openEpochsHeap) Pop() interface{} {
	old := *oeh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.heapIndex = -1
	*oeh = old[0 : n-1]
	return item
}

var (
	requestedMeta = metric.Metadata{
		Name:        "admission.requested.",
		Help:        "Number of requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	admittedMeta = metric.Metadata{
		Name:        "admission.admitted.",
		Help:        "Number of requests admitted",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	erroredMeta = metric.Metadata{
		Name:        "admission.errored.",
		Help:        "Number of requests not admitted due to error",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	waitDurationSumMeta = metric.Metadata{
		Name:        "admission.wait_sum.",
		Help:        "Total wait time in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	waitDurationsMeta = metric.Metadata{
		Name:        "admission.wait_durations.",
		Help:        "Wait time durations for requests that waited",
		Measurement: "Wait time Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}
	waitQueueLengthMeta = metric.Metadata{
		Name:        "admission.wait_queue_length.",
		Help:        "Length of wait queue",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
)

func addName(name string, meta metric.Metadata) metric.Metadata {
	rv := meta
	rv.Name = rv.Name + name
	return rv
}

// WorkQueueMetrics are metrics associated with a WorkQueue. These can be
// shared across WorkQueues, so Gauges should only be updated using deltas
// instead of by setting values.
type WorkQueueMetrics struct {
	name       string
	total      workQueueMetricsSingle
	byPriority sync.Map
	registry   *metric.Registry
}

// getOrCreate will return the metric if it exists or create it and then return
// it if it didn't previously exist.
// TODO(abaptist): Until https://github.com/cockroachdb/cockroach/issues/88846
// is fixed, this code is not useful since late registered metrics are not
// visible.
func (m *WorkQueueMetrics) getOrCreate(priority admissionpb.WorkPriority) workQueueMetricsSingle {
	// Try loading from the map first.
	val, ok := m.byPriority.Load(priority)
	if !ok {
		// This will only happen the first time it is requested. Doing this lazily
		// prevents unnecessary creation of unused priorities. Note that it is
		// necessary to call LoadOrStore here as this could be called concurrently.
		// It is not called the first Load so that we don't have to unnecessarily
		// create the metrics.
		statPrefix := fmt.Sprintf("%v.%v", m.name, admissionpb.WorkPriorityDict[priority])
		val, ok = m.byPriority.LoadOrStore(priority, makeWorkQueueMetricsSingle(statPrefix))
		if !ok {
			m.registry.AddMetricStruct(val)
		}
	}
	return val.(workQueueMetricsSingle)
}

type workQueueMetricsSingle struct {
	Requested       *metric.Counter
	Admitted        *metric.Counter
	Errored         *metric.Counter
	WaitDurations   metric.IHistogram
	WaitQueueLength *metric.Gauge
}

func (m *WorkQueueMetrics) incRequested(priority admissionpb.WorkPriority) {
	m.total.Requested.Inc(1)
	m.getOrCreate(priority).Requested.Inc(1)
}

func (m *WorkQueueMetrics) incAdmitted(priority admissionpb.WorkPriority) {
	m.total.Admitted.Inc(1)
	m.getOrCreate(priority).Admitted.Inc(1)
}

func (m *WorkQueueMetrics) incErrored(priority admissionpb.WorkPriority) {
	m.total.Errored.Inc(1)
	m.getOrCreate(priority).Errored.Inc(1)
}

func (m *WorkQueueMetrics) recordStartWait(priority admissionpb.WorkPriority) {
	m.total.WaitQueueLength.Inc(1)
	m.getOrCreate(priority).WaitQueueLength.Inc(1)
}

func (m *WorkQueueMetrics) recordFinishWait(priority admissionpb.WorkPriority, dur time.Duration) {
	m.total.WaitQueueLength.Dec(1)
	m.total.WaitDurations.RecordValue(dur.Nanoseconds())

	priorityStats := m.getOrCreate(priority)
	priorityStats.WaitQueueLength.Dec(1)
	priorityStats.WaitDurations.RecordValue(dur.Nanoseconds())
}

// MetricStruct implements the metric.Struct interface.
func (*WorkQueueMetrics) MetricStruct() {}

func makeWorkQueueMetrics(
	name string, registry *metric.Registry, applicablePriorities ...admissionpb.WorkPriority,
) *WorkQueueMetrics {
	totalMetric := makeWorkQueueMetricsSingle(name)
	registry.AddMetricStruct(totalMetric)
	wqm := &WorkQueueMetrics{
		name:     name,
		total:    totalMetric,
		registry: registry,
	}
	// TODO(abaptist): This is done to pre-register stats. Need to check that we
	// getOrCreate "enough" of the priorities to be useful. See
	// https://github.com/cockroachdb/cockroach/issues/88846.
	for _, pri := range applicablePriorities {
		wqm.getOrCreate(pri)
	}

	return wqm
}

func makeWorkQueueMetricsSingle(name string) workQueueMetricsSingle {
	return workQueueMetricsSingle{
		Requested: metric.NewCounter(addName(name, requestedMeta)),
		Admitted:  metric.NewCounter(addName(name, admittedMeta)),
		Errored:   metric.NewCounter(addName(name, erroredMeta)),
		WaitDurations: metric.NewHistogram(metric.HistogramOptions{
			UseHdrLatency: true,
			Metadata:      addName(name, waitDurationsMeta),
			Duration:      base.DefaultHistogramWindowInterval(),
			Buckets:       metric.IOLatencyBuckets,
		}),
		WaitQueueLength: metric.NewGauge(addName(name, waitQueueLengthMeta)),
	}
}

// StoreWriteWorkInfo is the information that needs to be provided for work
// seeking admission from a StoreWorkQueue.
type StoreWriteWorkInfo struct {
	WorkInfo
	// NB: no information about the size of the work is provided at admission
	// time. The token subtraction at admission time is completely based on past
	// estimates. This estimation is improved at work completion time via size
	// information provided in StoreWorkDoneInfo.
	//
	// TODO(sumeer): in some cases, like AddSSTable requests, we do have size
	// information at proposal time, and may be able to use it fruitfully.
}

// StoreWorkQueue is responsible for admission to a store.
type StoreWorkQueue struct {
	q [admissionpb.NumWorkClasses]WorkQueue
	// Only calls storeWriteDone. The rest of the interface is used by
	// WorkQueue.
	granters [admissionpb.NumWorkClasses]granterWithStoreWriteDone
	mu       struct {
		syncutil.RWMutex
		estimates storeRequestEstimates
		stats     storeAdmissionStats
	}
}

// StoreWorkHandle is returned by StoreWorkQueue.Admit, and contains state
// needed by the caller (see StoreWorkHandle.AdmissionEnabled) and by
// StoreWorkQueue.AdmittedWorkDone.
type StoreWorkHandle struct {
	tenantID roachpb.TenantID
	// The writeTokens acquired by this request. Must be > 0.
	writeTokens      int64
	workClass        admissionpb.WorkClass
	admissionEnabled bool
}

// AdmissionEnabled indicates whether admission control is enabled. If it
// returns false, there is no need to call StoreWorkQueue.AdmittedWorkDone.
func (h StoreWorkHandle) AdmissionEnabled() bool {
	return h.admissionEnabled
}

// Admit is called when requesting admission for store work. If err!=nil, the
// request was not admitted, potentially due to a deadline being exceeded. If
// err=nil and handle.AdmissionEnabled() is true, AdmittedWorkDone must be
// called when the admitted work is done.
func (q *StoreWorkQueue) Admit(
	ctx context.Context, info StoreWriteWorkInfo,
) (handle StoreWorkHandle, err error) {
	// For now, we compute a workClass based on priority.
	wc := admissionpb.WorkClassFromPri(info.Priority)
	h := StoreWorkHandle{
		tenantID:  info.TenantID,
		workClass: wc,
	}
	q.mu.RLock()
	estimates := q.mu.estimates
	q.mu.RUnlock()
	h.writeTokens = estimates.writeTokens
	info.WorkInfo.requestedCount = h.writeTokens
	enabled, err := q.q[wc].Admit(ctx, info.WorkInfo)
	if err != nil {
		return StoreWorkHandle{}, err
	}
	h.admissionEnabled = enabled
	return h, nil
}

// StoreWorkDoneInfo provides information about the work size after the work
// is done. This allows for correction of estimates made at admission time.
type StoreWorkDoneInfo struct {
	// The size of the Pebble write-batch, for normal writes. It is zero when
	// the write-batch is empty, which happens when all the bytes are being
	// added via sstable ingestion. NB: it is possible for both WriteBytes and
	// IngestedBytes to be 0 if nothing was actually written.
	WriteBytes int64
	// The size of the sstables, for ingests. Zero if there were no ingests.
	IngestedBytes int64
}

// AdmittedWorkDone indicates to the queue that the admitted work has
// completed.
func (q *StoreWorkQueue) AdmittedWorkDone(h StoreWorkHandle, doneInfo StoreWorkDoneInfo) error {
	if !h.admissionEnabled {
		return nil
	}
	q.updateStoreAdmissionStats(1, doneInfo, false)
	additionalTokens := q.granters[h.workClass].storeWriteDone(h.writeTokens, doneInfo)
	q.q[h.workClass].adjustTenantTokens(h.tenantID, additionalTokens)
	return nil
}

// BypassedWorkDone is called for follower writes, so that admission control
// can (a) adjust remaining tokens, (b) account for this in the per-work token
// estimation model.
func (q *StoreWorkQueue) BypassedWorkDone(workCount int64, doneInfo StoreWorkDoneInfo) {
	q.updateStoreAdmissionStats(uint64(workCount), doneInfo, true)
	// Since we have no control over such work, we choose to count it as
	// regularWorkClass.
	_ = q.granters[admissionpb.RegularWorkClass].storeWriteDone(0, doneInfo)
}

// StatsToIgnore is called for range snapshot ingestion -- see the comment in
// storeAdmissionStats.
func (q *StoreWorkQueue) StatsToIgnore(ingestStats pebble.IngestOperationStats) {
	q.mu.Lock()
	q.mu.stats.statsToIgnore.Bytes += ingestStats.Bytes
	q.mu.stats.statsToIgnore.ApproxIngestedIntoL0Bytes += ingestStats.ApproxIngestedIntoL0Bytes
	q.mu.Unlock()
}

func (q *StoreWorkQueue) updateStoreAdmissionStats(
	workCount uint64, doneInfo StoreWorkDoneInfo, bypassed bool,
) {
	q.mu.Lock()
	q.mu.stats.admittedCount += workCount
	q.mu.stats.writeAccountedBytes += uint64(doneInfo.WriteBytes)
	q.mu.stats.ingestedAccountedBytes += uint64(doneInfo.IngestedBytes)
	if bypassed {
		q.mu.stats.aux.bypassedCount += workCount
		q.mu.stats.aux.writeBypassedAccountedBytes += uint64(doneInfo.WriteBytes)
		q.mu.stats.aux.ingestedBypassedAccountedBytes += uint64(doneInfo.IngestedBytes)
	}
	q.mu.Unlock()
}

// SetTenantWeights passes through to WorkQueue.SetTenantWeights.
func (q *StoreWorkQueue) SetTenantWeights(tenantWeights map[uint64]uint32) {
	for i := range q.q {
		q.q[i].SetTenantWeights(tenantWeights)
	}
}

// getRequesters implements storeRequester.
func (q *StoreWorkQueue) getRequesters() [admissionpb.NumWorkClasses]requester {
	var result [admissionpb.NumWorkClasses]requester
	for i := range q.q {
		result[i] = &q.q[i]
	}
	return result
}

func (q *StoreWorkQueue) close() {
	for i := range q.q {
		q.q[i].close()
	}
}

func (q *StoreWorkQueue) getStoreAdmissionStats() storeAdmissionStats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.mu.stats
}

func (q *StoreWorkQueue) setStoreRequestEstimates(estimates storeRequestEstimates) {
	q.mu.Lock()
	q.mu.estimates = estimates
	q.mu.Unlock()
}

func makeStoreWorkQueue(
	ambientCtx log.AmbientContext,
	granters [admissionpb.NumWorkClasses]granterWithStoreWriteDone,
	settings *cluster.Settings,
	metrics *WorkQueueMetrics,
	opts workQueueOptions,
) storeRequester {
	q := &StoreWorkQueue{
		granters: granters,
	}
	for i := range q.q {
		initWorkQueue(&q.q[i], ambientCtx, KVWork, granters[i], settings, metrics, opts)
	}
	// Arbitrary initial value. This will be replaced before any meaningful
	// token constraints are enforced.
	q.mu.estimates = storeRequestEstimates{
		writeTokens: 1,
	}
	return q
}
