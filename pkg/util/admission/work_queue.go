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
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// KVAdmissionControlEnabled controls whether KV server-side admission control
// is enabled.
var KVAdmissionControlEnabled = settings.RegisterBoolSetting(
	"admission.kv.enabled",
	"when true, work performed by the KV layer is subject to admission control",
	true).WithPublic()

// SQLKVResponseAdmissionControlEnabled controls whether response processing
// in SQL, for KV requests, is enabled.
var SQLKVResponseAdmissionControlEnabled = settings.RegisterBoolSetting(
	"admission.sql_kv_response.enabled",
	"when true, work performed by the SQL layer when receiving a KV response is subject to "+
		"admission control",
	true).WithPublic()

// SQLSQLResponseAdmissionControlEnabled controls whether response processing
// in SQL, for DistSQL requests, is enabled.
var SQLSQLResponseAdmissionControlEnabled = settings.RegisterBoolSetting(
	"admission.sql_sql_response.enabled",
	"when true, work performed by the SQL layer when receiving a DistSQL response is subject "+
		"to admission control",
	true).WithPublic()

var admissionControlEnabledSettings = [numWorkKinds]*settings.BoolSetting{
	KVWork:             KVAdmissionControlEnabled,
	SQLKVResponseWork:  SQLKVResponseAdmissionControlEnabled,
	SQLSQLResponseWork: SQLSQLResponseAdmissionControlEnabled,
}

var EpochLIFOEnabled = settings.RegisterBoolSetting(
	"admission.epoch_lifo.enabled",
	"when true, epoch-LIFO behavior is enabled when there is significant delay in admission",
	false).WithPublic()

// WorkPriority represents the priority of work. In an WorkQueue, it is only
// used for ordering within a tenant. High priority work can starve lower
// priority work.
type WorkPriority int8

const (
	// LowPri is low priority work.
	LowPri WorkPriority = math.MinInt8
	// NormalPri is normal priority work.
	NormalPri WorkPriority = 0
	// HighPri is high priority work.
	HighPri WorkPriority = math.MaxInt8
)

// Prevent the linter from emitting unused warnings.
var _ = LowPri
var _ = NormalPri
var _ = HighPri

// WorkInfo provides information that is used to order work within an
// WorkQueue. The WorkKind is not included as a field since an WorkQueue deals
// with a single WorkKind.
type WorkInfo struct {
	// TenantID is the id of the tenant. For single-tenant clusters, this will
	// always be the SystemTenantID.
	TenantID roachpb.TenantID
	// Priority is utilized within a tenant.
	Priority WorkPriority
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
}

// WorkQueue maintains a queue of work waiting to be admitted. Ordering of
// work is achieved via 2 heaps: a tenant heap orders the tenants with waiting
// work in increasing order of used slots or tokens. Within each tenant, the
// waiting work is ordered based on priority and create time. Tenants with
// non-zero values of used slots or tokens are tracked even if they have no
// more waiting work. Token usage is reset to zero every second. The choice of
// 1 second of memory for token distribution fairness is somewhat arbitrary.
// The same 1 second interval is also used to garbage collect tenants who have
// no waiting requests and no used slots or tokens.
//
// Note that currently there are no weights associated with tenants -- one
// could imagine using weights and comparing used/weight values for
// inter-tenant fairness.
//
// Usage example:
//  var grantCoord *GrantCoordinator
//  <initialize grantCoord>
//  kvQueue := grantCoord.GetWorkQueue(KVWork)
//  <hand kvQueue to the code that does kv server work>
//
//  // Before starting some kv server work
//  if enabled, err := kvQueue.Admit(ctx, WorkInfo{TenantID: tid, ...}); err != nil {
//    return err
//  }
//  <do the work>
//  if enabled {
//    kvQueue.AdmittedWorkDone(tid)
//  }
type WorkQueue struct {
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
		tenants map[uint64]*tenantInfo
		// The highest epoch that is closed.
		closedEpochThreshold int64
	}
	metrics       WorkQueueMetrics
	admittedCount uint64
	stopCh        chan struct{}

	timeSource timeutil.TimeSource
}

var _ requester = &WorkQueue{}

type workQueueOptions struct {
	usesTokens  bool
	tiedToRange bool
	// If non-nil, the WorkQueue should use the supplied metrics instead of
	// creating its own.
	metrics *WorkQueueMetrics

	// timeSource can be set to non-nil for tests.
	timeSource timeutil.TimeSource
	// The epoch closing goroutine may be disabled for tests.
	disableEpochClosingGoroutine bool
}

func makeWorkQueueOptions(workKind WorkKind) workQueueOptions {
	switch workKind {
	case KVWork:
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
	workKind WorkKind, granter granter, settings *cluster.Settings, opts workQueueOptions,
) requester {
	stopCh := make(chan struct{})
	var metrics WorkQueueMetrics
	if opts.metrics == nil {
		metrics = makeWorkQueueMetrics(string(workKindString(workKind)))
	} else {
		metrics = *opts.metrics
	}
	q := &WorkQueue{
		workKind:    workKind,
		granter:     granter,
		usesTokens:  opts.usesTokens,
		tiedToRange: opts.tiedToRange,
		settings:    settings,
		metrics:     metrics,
		stopCh:      stopCh,
		timeSource:  opts.timeSource,
	}
	q.mu.tenants = make(map[uint64]*tenantInfo)
	go func() {
		ticker := time.NewTicker(time.Second)
		done := false
		for !done {
			select {
			case <-ticker.C:
				q.gcTenantsAndResetTokens()
			case <-stopCh:
				// Channel closed.
				done = true
			}
		}
	}()
	epochLIFOEnabled := q.settings != nil && EpochLIFOEnabled.Get(&q.settings.SV)
	q.tryCloseEpoch(epochLIFOEnabled)
	if !opts.disableEpochClosingGoroutine {
		go func() {
			// TODO(sumeer): reduce the ticker frequency after it aligns with epoch
			// close. We would also need to adjust for drift after we reduce
			// frequency.
			ticker := time.NewTicker(time.Millisecond)
			done := false
			for !done {
				select {
				case <-ticker.C:
					epochLIFOEnabled := q.settings != nil && EpochLIFOEnabled.Get(&q.settings.SV)
					q.tryCloseEpoch(epochLIFOEnabled)
				case <-stopCh:
					// Channel closed.
					done = true
				}
			}
		}()
	}
	return q
}

func isInTenantHeap(tenant *tenantInfo) bool {
	return len(tenant.waitingWorkHeap) > 0 || len(tenant.openEpochsHeap) > 0
}

func (q *WorkQueue) timeNow() time.Time {
	if q.timeSource != nil {
		return q.timeSource.Now()
	}
	return timeutil.Now()
}

func (q *WorkQueue) tryCloseEpoch(epochLIFOEnabled bool) {
	ec := epochForTimeNanos(q.timeNow().UnixNano() - epochLengthNanos - epochClosingDeltaNanos)
	q.mu.Lock()
	defer q.mu.Unlock()
	if ec > q.mu.closedEpochThreshold {
		q.mu.closedEpochThreshold = ec
		closedEpoch := q.mu.closedEpochThreshold
		for _, tenant := range q.mu.tenants {
			prevThreshold := tenant.fifoPriorityThreshold
			tenant.fifoPriorityThreshold =
				tenant.priorityStates.getFIFOPriorityThresholdAndReset(tenant.fifoPriorityThreshold)
			if !epochLIFOEnabled {
				tenant.fifoPriorityThreshold = int(LowPri)
			}
			if tenant.fifoPriorityThreshold != prevThreshold {
				// TODO(sumeer): remove after testing?
				log.Infof(context.Background(), "%s: FIFO threshold for tenant %d changed to %d",
					string(workKindString(q.workKind)), tenant.id, tenant.fifoPriorityThreshold)
			}
			// Note that we are ignoring the new priority threshold and only
			// dequeueing the ones that are in the closed epoch. It is possible to
			// have work items that are not in the closed epoch and whose priority
			// makes them no longer subject to LIFO, but they will need to wait here
			// until their epochs close. This is considered acceptable since the
			// priority threshold should not fluctuate rapidly.
			for len(tenant.openEpochsHeap) > 0 {
				work := tenant.openEpochsHeap[0]
				if work.epoch > closedEpoch {
					break
				}
				heap.Pop(&tenant.openEpochsHeap)
				heap.Push(&tenant.waitingWorkHeap, work)
			}
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
	if q.settings != nil && enabledSetting != nil && !enabledSetting.Get(&q.settings.SV) {
		return false, nil
	}
	q.metrics.Requested.Inc(1)
	tenantID := info.TenantID.ToUint64()

	// The code in this method does not use defer to unlock the mutexes because
	// it needs the flexibility of selectively unlocking one of these on a
	// certain code path. When changing the code, be careful in making sure the
	// mutexes are properly unlocked on all code paths.
	q.admitMu.Lock()
	q.mu.Lock()
	tenant, ok := q.mu.tenants[tenantID]
	if !ok {
		tenant = newTenantInfo(tenantID)
		q.mu.tenants[tenantID] = tenant
	}
	if info.BypassAdmission && roachpb.IsSystemTenantID(tenantID) && q.workKind == KVWork {
		tenant.used++
		if isInTenantHeap(tenant) {
			q.mu.tenantHeap.fix(tenant)
		}
		q.mu.Unlock()
		q.admitMu.Unlock()
		q.granter.tookWithoutPermission()
		q.metrics.Admitted.Inc(1)
		atomic.AddUint64(&q.admittedCount, 1)
		return true, nil
	}

	if len(q.mu.tenantHeap) == 0 {
		// Fast-path. Try to grab token/slot.
		// Optimistically update used to avoid locking again.
		tenant.used++
		q.mu.Unlock()
		if q.granter.tryGet() {
			q.admitMu.Unlock()
			q.metrics.Admitted.Inc(1)
			atomic.AddUint64(&q.admittedCount, 1)
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
		tenant.used--
	}
	// Check for cancellation.
	startTime := q.timeNow()
	doneCh := ctx.Done()
	if doneCh != nil {
		select {
		case _, ok := <-doneCh:
			if !ok {
				// Already canceled. More likely to happen if cpu starvation is
				// causing entering into the work queue to be delayed.
				q.mu.Unlock()
				q.admitMu.Unlock()
				q.metrics.Errored.Inc(1)
				deadline, _ := ctx.Deadline()
				return true,
					errors.Newf("work %s deadline already expired: deadline: %v, now: %v",
						workKindString(q.workKind), deadline, startTime)
			}
		default:
		}
	}
	// Push onto heap(s).
	ordering := fifoWorkOrdering
	if int(info.Priority) < tenant.fifoPriorityThreshold {
		ordering = lifoWorkOrdering
	}
	work := newWaitingWork(info.Priority, ordering, info.CreateTime, startTime)
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

	q.metrics.WaitQueueLength.Inc(1)
	defer releaseWaitingWork(work)
	select {
	case <-doneCh:
		waitDur := q.timeNow().Sub(startTime)
		q.mu.Lock()
		tenant.priorityStates.updateDelayLocked(work.priority, waitDur)
		if work.heapIndex == -1 {
			// No longer in heap. Raced with token/slot grant.
			tenant.used--
			q.mu.Unlock()
			q.granter.returnGrant()
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
		q.metrics.Errored.Inc(1)
		q.metrics.WaitDurationSum.Inc(waitDur.Microseconds())
		q.metrics.WaitDurations.RecordValue(waitDur.Nanoseconds())
		q.metrics.WaitQueueLength.Dec(1)
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
		q.metrics.Admitted.Inc(1)
		atomic.AddUint64(&q.admittedCount, 1)
		waitDur := q.timeNow().Sub(startTime)
		q.metrics.WaitDurationSum.Inc(waitDur.Microseconds())
		q.metrics.WaitDurations.RecordValue(waitDur.Nanoseconds())
		q.metrics.WaitQueueLength.Dec(1)
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
	q.granter.returnGrant()
}

func (q *WorkQueue) getAdmittedCount() uint64 {
	return atomic.LoadUint64(&q.admittedCount)
}

func (q *WorkQueue) hasWaitingRequests() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.mu.tenantHeap) > 0
}

func (q *WorkQueue) granted(grantChainID grantChainID) bool {
	// Reduce critical section by getting time before mutex acquisition.
	now := q.timeNow()
	q.mu.Lock()
	if len(q.mu.tenantHeap) == 0 {
		q.mu.Unlock()
		return false
	}
	tenant := q.mu.tenantHeap[0]
	var item *waitingWork
	if len(tenant.waitingWorkHeap) > 0 {
		item = heap.Pop(&tenant.waitingWorkHeap).(*waitingWork)
	} else {
		item = heap.Pop(&tenant.openEpochsHeap).(*waitingWork)
	}
	waitDur := now.Sub(item.enqueueingTime)
	tenant.priorityStates.updateDelayLocked(item.priority, waitDur)
	tenant.used++
	if isInTenantHeap(tenant) {
		q.mu.tenantHeap.fix(tenant)
	} else {
		q.mu.tenantHeap.remove(tenant)
	}
	q.mu.Unlock()
	// Reduce critical section by sending on channel after releasing mutex.
	item.ch <- grantChainID
	return true
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
		}
		if q.usesTokens {
			info.used = 0
			// All the heap members will reset used=0, so no need to change heap
			// ordering.
		}
	}
}

func (q *WorkQueue) String() string {
	return redact.StringWithoutMarkers(q)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (q *WorkQueue) SafeFormat(s redact.SafePrinter, verb rune) {
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
		s.Printf("\n tenant-id: %d used: %d, fifo: %d", tenant.id, tenant.used,
			tenant.fifoPriorityThreshold)
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
	priority      WorkPriority
	maxQueueDelay time.Duration
}

// In increasing order of priority. Expected to not have more than 5 elements,
// so a linear search is fast.
type priorityStates []priorityState

// TODO(sumeer): the maxQueueDelay value is incomplete since it does not have
// visibility into work that is still waiting in the queue.
func (ps *priorityStates) updateDelayLocked(priority WorkPriority, delay time.Duration) {
	i := 0
	n := len(*ps)
	for ; i < n; i++ {
		pri := (*ps)[i].priority
		if pri == priority {
			if (*ps)[i].maxQueueDelay < delay {
				(*ps)[i].maxQueueDelay = delay
			}
			return
		}
		if pri > priority {
			break
		}
	}
	state := priorityState{priority: priority, maxQueueDelay: delay}
	*ps = append(*ps, state)
	if i == n {
		return
	}
	for j := n; j > i; j-- {
		(*ps)[j] = (*ps)[j-1]
	}
	(*ps)[i] = state
}

func (ps *priorityStates) getFIFOPriorityThresholdAndReset(curPriorityThreshold int) int {
	priority := int(LowPri)
	for i := range *ps {
		p := (*ps)[i]
		if p.maxQueueDelay > time.Duration(epochLengthNanos) {
			// LIFO.
			priority = int(p.priority) + 1
		} else if int(p.priority) < curPriorityThreshold {
			// Currently LIFO. Should we continue as LIFO?
			if p.maxQueueDelay > time.Duration(epochLengthNanos)/10 {
				priority = int(p.priority) + 1
			}
			// Can switch to FIFO, at least based on queue delay at this priority.
			// But we must examine the other higher priorities too, since it is
			// possible that nothing was received or serviced for this priority.
		}
		(*ps)[i].maxQueueDelay = 0
	}
	return priority
}

// tenantInfo is the per-tenant information in the tenantHeap.
type tenantInfo struct {
	id uint64
	// used can be the currently used slots, or the tokens granted within the last
	// interval.
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
// order of tenantInfo.used. That is, we prefer tenants that are using less.
type tenantHeap []*tenantInfo

var _ heap.Interface = (*tenantHeap)(nil)

var tenantInfoPool = sync.Pool{
	New: func() interface{} {
		return &tenantInfo{}
	},
}

func newTenantInfo(id uint64) *tenantInfo {
	ti := tenantInfoPool.Get().(*tenantInfo)
	*ti = tenantInfo{
		id:                    id,
		waitingWorkHeap:       ti.waitingWorkHeap,
		openEpochsHeap:        ti.openEpochsHeap,
		priorityStates:        ti.priorityStates[:0],
		fifoPriorityThreshold: int(LowPri),
		heapIndex:             -1,
	}
	return ti
}

func releaseTenantInfo(ti *tenantInfo) {
	if len(ti.waitingWorkHeap) != 0 || len(ti.openEpochsHeap) != 0 {
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
		priorityStates:  ti.priorityStates[:0],
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
	return (*th)[i].used < (*th)[j].used
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
	priority WorkPriority
	// The workOrderingKind for this priority when this work was queued.
	arrivalTimeWorkOrdering workOrderingKind
	createTime              int64
	// epoch is a function of the createTime.
	epoch int64

	// ch is used to communicate a grant to the waiting goroutine. The
	// grantChainID is used by the waiting goroutine to call continueGrantChain.
	ch chan grantChainID
	// The heapIndex is maintained by the heap.Interface methods, and represents
	// the heapIndex of the item in the heap. -1 when not in the heap. The same
	// heapIndex is used by the waitingWorkHeap and the openEpochsHeap since a
	// waitingWork is only in one of these.
	heapIndex int
	// Set to true when added to waitingWorkHeap. Only used to disambiguate
	// which heap the waitingWork is in, when we know it is in one of the heaps.
	// So the only state transition is from false => true, and never restored
	// back to false.
	inWaitingWorkHeap bool
	enqueueingTime    time.Time
}

var waitingWorkPool = sync.Pool{
	New: func() interface{} {
		return &waitingWork{}
	},
}

// The epoch length for doing epoch-LIFO. The epoch-LIFO scheme relies on
// clock synchronization and the expectation that transaction/query deadlines
// will be significantly higher than execution time under low load. A standard
// LIFO scheme suffers from a severe problem when a single user transaction
// can result in many lower-level work that get distributed to many nodes, and
// previous work execution can result in new work being submitted for
// admission: the later work for a transaction may no longer be the latest
// seen by the system, so will not be preferred. This means LIFO would do some
// work items from each transaction and starve the remaining work, so nothing
// would complete. This is even worse than FIFO which at least prefers the
// same transactions until they are complete (FIFO and LIFO are using the
// transaction CreateTime, and not the work arrival time).
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
// TODO(sumeer): make this configurable via a cluster setting.
const epochLengthNanos = int64(time.Millisecond * 100)
const epochClosingDeltaNanos = int64(time.Millisecond * 5)

func epochForTimeNanos(t int64) int64 {
	return t / epochLengthNanos
}

func newWaitingWork(
	priority WorkPriority,
	arrivalTimeWorkOrdering workOrderingKind,
	createTime int64,
	enqueueingTime time.Time,
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
		epoch:                   epochForTimeNanos(createTime),
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
//  w3: (fifo, create: t3, epoch: e)
//  w2: (lifo, create: t2, epoch: e)
//  w1: (fifo, create: t1, epoch: e)
//  w1 < w3, w3 < w2, w2 < w1, which is a cycle.
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
// fluctuate between overload and normal and doing FIFO here causes
// transaction work to start but not finish because the rest of the work is
// done using LIFO ordering. We may need to experiment before making a final
// decision. When an epoch closes, a prefix of this heap will be dequeued and
// added to the waitingWorkHeap.
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
	Requested       *metric.Counter
	Admitted        *metric.Counter
	Errored         *metric.Counter
	WaitDurationSum *metric.Counter
	WaitDurations   *metric.Histogram
	WaitQueueLength *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (WorkQueueMetrics) MetricStruct() {}

func makeWorkQueueMetrics(name string) WorkQueueMetrics {
	return WorkQueueMetrics{
		Requested:       metric.NewCounter(addName(name, requestedMeta)),
		Admitted:        metric.NewCounter(addName(name, admittedMeta)),
		Errored:         metric.NewCounter(addName(name, erroredMeta)),
		WaitDurationSum: metric.NewCounter(addName(name, waitDurationSumMeta)),
		WaitDurations: metric.NewLatency(
			addName(name, waitDurationsMeta), base.DefaultHistogramWindowInterval()),
		WaitQueueLength: metric.NewGauge(addName(name, waitQueueLengthMeta)),
	}
}
