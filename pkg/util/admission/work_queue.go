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
	false).WithPublic()

// SQLKVResponseAdmissionControlEnabled controls whether response processing
// in SQL, for KV requests, is enabled.
var SQLKVResponseAdmissionControlEnabled = settings.RegisterBoolSetting(
	"admission.sql_kv_response.enabled",
	"when true, work performed by the SQL layer when receiving a KV response is subject to "+
		"admission control",
	false).WithPublic()

var admissionControlEnabledSettings = [numWorkKinds]*settings.BoolSetting{
	KVWork:            KVAdmissionControlEnabled,
	SQLKVResponseWork: SQLKVResponseAdmissionControlEnabled,
}

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
	}
	metrics       WorkQueueMetrics
	admittedCount uint64
	gcStopCh      chan struct{}
}

var _ requester = &WorkQueue{}

func makeWorkQueue(
	workKind WorkKind, granter granter, usesTokens bool, tiedToRange bool, settings *cluster.Settings,
) requester {
	gcStopCh := make(chan struct{})
	q := &WorkQueue{
		workKind:    workKind,
		granter:     granter,
		usesTokens:  usesTokens,
		tiedToRange: tiedToRange,
		settings:    settings,
		metrics:     makeWorkQueueMetrics(string(workKindString(workKind))),
		gcStopCh:    gcStopCh,
	}
	q.mu.tenants = make(map[uint64]*tenantInfo)
	go func() {
		ticker := time.NewTicker(time.Second)
		done := false
		for !done {
			select {
			case <-ticker.C:
				q.gcTenantsAndResetTokens()
			case <-gcStopCh:
				done = true
			}
		}
		close(gcStopCh)
	}()
	return q
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
		if len(tenant.waitingWorkHeap) > 0 {
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
	startTime := timeutil.Now()
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
	work := newWaitingWork(info.Priority, info.CreateTime)
	heap.Push(&tenant.waitingWorkHeap, work)
	if len(tenant.waitingWorkHeap) == 1 {
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
		q.mu.Lock()
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
			tenant.waitingWorkHeap.remove(work)
			if len(tenant.waitingWorkHeap) == 0 {
				q.mu.tenantHeap.remove(tenant)
			}
			q.mu.Unlock()
		}
		q.metrics.Errored.Inc(1)
		waitDur := timeutil.Since(startTime)
		q.metrics.WaitDurationSum.Inc(waitDur.Microseconds())
		q.metrics.WaitDurations.RecordValue(waitDur.Nanoseconds())
		q.metrics.WaitQueueLength.Dec(1)
		deadline, _ := ctx.Deadline()
		return true,
			errors.Newf("work %s deadline expired while waiting: deadline: %v, start: %v, dur: %v",
				workKindString(q.workKind), deadline, startTime, waitDur)
	case chainID, ok := <-work.ch:
		if !ok {
			panic(errors.AssertionFailedf("channel should not be closed"))
		}
		q.metrics.Admitted.Inc(1)
		atomic.AddUint64(&q.admittedCount, 1)
		waitDur := timeutil.Since(startTime)
		q.metrics.WaitDurationSum.Inc(waitDur.Microseconds())
		q.metrics.WaitDurations.RecordValue(waitDur.Nanoseconds())
		q.metrics.WaitQueueLength.Dec(1)
		if work.heapIndex != -1 {
			panic(errors.AssertionFailedf("grantee should be removed from heap"))
		}
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
	if len(tenant.waitingWorkHeap) > 0 {
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
	now := timeutil.Now()
	q.mu.Lock()
	if len(q.mu.tenantHeap) == 0 {
		q.mu.Unlock()
		return false
	}
	tenant := q.mu.tenantHeap[0]
	item := heap.Pop(&tenant.waitingWorkHeap).(*waitingWork)
	item.grantTime = now
	tenant.used++
	if len(tenant.waitingWorkHeap) > 0 {
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
		if info.used == 0 && len(info.waitingWorkHeap) == 0 {
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
		s.Printf("\n tenant-id: %d used: %d", tenant.id, tenant.used)
		if len(tenant.waitingWorkHeap) > 0 {
			s.Printf(" heap:")
			for i := range tenant.waitingWorkHeap {
				s.Printf(" %d: pri: %d, ct: %d", i, tenant.waitingWorkHeap[i].priority,
					tenant.waitingWorkHeap[i].createTime)
			}
		}
	}
}

// close tells the gc goroutine to stop.
func (q *WorkQueue) close() {
	q.gcStopCh <- struct{}{}
}

// tenantInfo is the per-tenant information in the tenantHeap.
type tenantInfo struct {
	id uint64
	// used can be the currently used slots, or the tokens granted within the last
	// interval.
	used            uint64
	waitingWorkHeap waitingWorkHeap

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
		id:              id,
		waitingWorkHeap: ti.waitingWorkHeap,
		heapIndex:       -1,
	}
	return ti
}

func releaseTenantInfo(ti *tenantInfo) {
	if len(ti.waitingWorkHeap) != 0 {
		panic("tenantInfo has non-empty heap")
	}
	// NB: waitingWorkHeap.Pop nils the slice elements when removing, so we are
	// not inadvertently holding any references.
	if cap(ti.waitingWorkHeap) > 100 {
		ti.waitingWorkHeap = nil
	}
	wwh := ti.waitingWorkHeap
	*ti = tenantInfo{
		waitingWorkHeap: wwh,
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
	priority   WorkPriority
	createTime int64
	// ch is used to communicate a grant to the waiting goroutine. The
	// grantChainID is used by the waiting goroutine to call continueGrantChain.
	ch chan grantChainID
	// The heapIndex is maintained by the heap.Interface methods, and represents
	// the heapIndex of the item in the heap. -1 when not in the heap.
	heapIndex int
	grantTime time.Time
}

// waitingWorkHeap is a heap of waiting work within a tenant. It is ordered in
// decreasing order of priority, and within the same priority in increasing
// order of createTime (to prefer older work).
type waitingWorkHeap []*waitingWork

var _ heap.Interface = (*waitingWorkHeap)(nil)

var waitingWorkPool = sync.Pool{
	New: func() interface{} {
		return &waitingWork{}
	},
}

func newWaitingWork(priority WorkPriority, createTime int64) *waitingWork {
	ww := waitingWorkPool.Get().(*waitingWork)
	ch := ww.ch
	if ch == nil {
		ch = make(chan grantChainID, 1)
	}
	*ww = waitingWork{
		priority:   priority,
		createTime: createTime,
		ch:         ch,
		heapIndex:  -1,
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

func (wwh *waitingWorkHeap) remove(item *waitingWork) {
	heap.Remove(wwh, item.heapIndex)
}

func (wwh *waitingWorkHeap) Len() int { return len(*wwh) }

func (wwh *waitingWorkHeap) Less(i, j int) bool {
	if (*wwh)[i].priority == (*wwh)[j].priority {
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

// WorkQueueMetrics are metrics associated with a WorkQueue.
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
