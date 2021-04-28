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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type WorkPriority int8

const (
	LowPri    WorkPriority = math.MinInt8
	NormalPri WorkPriority = 0
	HighPri   WorkPriority = math.MaxInt8
)

// WorkInfo provides information that is used to order work within an
// AdmissionQueue. The WorkKind is not included as a field since an
// AdmissionQueue deals with a single WorkKind.
type WorkInfo struct {
	// TenantID is the id of the tenant. For non-serverless, this will always be
	// the SystemTenantID.
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
	// when KV work generates other KV work (to avoid deadlock). Currently only
	// supported for AdmissionQueues that are using slots (not tokens).
	BypassAdmission bool

	// Information specified only for AdmissionQueues where the work is tied to
	// a range. This allows queued work to return early as soon as the range is
	// no longer in a relevant state at this node.
	// TODO(sumeer): use these in the AdmissionQueue implementation.

	// RangeID is the range at which this work must be performed.
	RangeID roachpb.RangeID
	// RequiresLeaseholder is true iff the work requires the leaseholder.
	RequiresLeaseholder bool
}

type AdmissionQueue struct {
	workKind    WorkKind
	granter     granter
	usesTokens  bool
	tiedToRange bool

	// Protects the following mutable data-structures.
	mu syncutil.Mutex
	// Prevents more than one caller to be in Admit and adding to queue, so can
	// release mu before calling tryGet and be assured that not competing with
	// another Admit.
	admitMu syncutil.Mutex

	// Tenants with waiting work.
	tenantHeap tenantHeap
	// All tenants, including those without waiting work. Periodically cleaned.
	tenants map[uint64]*tenantInfo

	metrics AdmissionQueueMetrics
}

var _ requester = &AdmissionQueue{}

func MakeAdmissionQueue(
	workKind WorkKind, granter granter, usesTokens bool, tiedToRange bool,
) *AdmissionQueue {
	q := &AdmissionQueue{
		workKind:    workKind,
		granter:     granter,
		usesTokens:  usesTokens,
		tiedToRange: tiedToRange,
		metrics:     MakeAdmissionQueueMetrics(string(workKindString(workKind))),
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			<-ticker.C
			q.gcTenantsAndResetTokens()
		}
	}()
	return q
}

// TODO(sumeer): reduce allocations by using a pool for tenantInfo, waitingWork,
// waitingWork.ch.

var neverClosedCh = make(chan struct{})

func (q *AdmissionQueue) Admit(ctx context.Context, info WorkInfo) error {
	q.metrics.Requested.Inc(1)
	tenantID := info.TenantID.ToUint64()

	q.admitMu.Lock()
	q.mu.Lock()
	tenant, ok := q.tenants[tenantID]
	if !ok {
		tenant = &tenantInfo{id: tenantID, index: -1}
		q.tenants[tenantID] = tenant
	}
	if info.BypassAdmission && roachpb.IsSystemTenantID(tenantID) {
		tenant.used++
		if len(tenant.waitingWorkHeap) > 0 {
			heap.Fix(&q.tenantHeap, tenant.index)
		}
		q.mu.Unlock()
		q.admitMu.Unlock()
		q.granter.tookWithoutPermission()
		q.metrics.Admitted.Inc(1)
		return nil
	}

	if len(q.tenantHeap) == 0 {
		// Fast-path. Try to grab token/slot.
		// Optimistically update used to avoid locking again.
		tenant.used++
		q.mu.Unlock()
		if q.granter.tryGet() {
			q.admitMu.Unlock()
			q.metrics.Admitted.Inc(1)
			return nil
		}
		// Did not get token/slot.
		//
		// There is a race here: before q.mu is acquired, the granter could
		// experiences a reduction in load and call
		// AdmissionQueue.hasWaitingRequests to see if it should grant, but since
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
	// Check deadline.
	startTime := time.Now()
	deadline, ok := ctx.Deadline()
	var doneCh <-chan struct{}
	if ok {
		doneCh = ctx.Done()
		if doneCh == nil {
			panic("have deadline but no done channel")
		}
		select {
		case _, ok := <-doneCh:
			if !ok {
				// Already cancelled. More likely to happen if cpu starvation is
				// causing entering into the admission queue to be delayed.
				q.mu.Unlock()
				q.admitMu.Unlock()
				q.metrics.Errored.Inc(1)
				return errors.Newf("work %s deadline already expired: deadline: %v, now: %v",
					workKindString(q.workKind), deadline, startTime)
			}
		default:
		}
	}
	// Push onto heap(s).
	ch := make(chan struct{}, 1)
	work := &waitingWork{
		priority:   info.Priority,
		createTime: info.CreateTime,
		ch:         ch,
		index:      -1,
	}
	heap.Push(&tenant.waitingWorkHeap, work)
	if len(tenant.waitingWorkHeap) == 1 {
		heap.Push(&q.tenantHeap, tenant)
	}
	// Else already in tenantHeap.
	q.metrics.WaitQueueLength.Inc(1)

	if doneCh == nil {
		doneCh = neverClosedCh
	}
	// Release all locks and start waiting.
	q.mu.Unlock()
	q.admitMu.Unlock()
	select {
	case <-doneCh:
		q.mu.Lock()
		if work.index == -1 {
			// No longer in heap. Raced with token/slot grant.
			tenant.used--
			q.mu.Unlock()
			q.granter.returnGrant()
			q.granter.continueGrantChain()
		} else {
			heap.Remove(&tenant.waitingWorkHeap, work.index)
			if len(tenant.waitingWorkHeap) == 0 {
				heap.Remove(&q.tenantHeap, tenant.index)
			}
			q.mu.Unlock()
		}
		q.metrics.Errored.Inc(1)
		waitDur := time.Now().Sub(startTime)
		q.metrics.WaitDurationSum.Inc(waitDur.Microseconds())
		q.metrics.WaitDurations.RecordValue(waitDur.Nanoseconds())
		q.metrics.WaitQueueLength.Dec(1)
		return errors.Newf("work %s deadline expired while waiting: deadline: %v, start: %v, dur: %v",
			workKindString(q.workKind), deadline, startTime, waitDur)
	case _, ok := <-ch:
		if !ok {
			panic("bug: channel should not be closed")
		}
		q.metrics.Admitted.Inc(1)
		waitDur := time.Now().Sub(startTime)
		q.metrics.WaitDurationSum.Inc(waitDur.Microseconds())
		q.metrics.WaitDurations.RecordValue(waitDur.Nanoseconds())
		q.metrics.WaitQueueLength.Dec(1)
		if work.index != -1 {
			panic("bug: slot grant should have removed from heap")
		}
		q.granter.continueGrantChain()
		return nil
	}
}

func (q *AdmissionQueue) AdmittedWorkDone(tenantID roachpb.TenantID) {
	if q.usesTokens {
		panic("tokens should not be returned")
	}
	q.mu.Lock()
	tenant, ok := q.tenants[tenantID.ToUint64()]
	if !ok {
		panic("tenant not found")
	}
	tenant.used--
	if len(tenant.waitingWorkHeap) > 0 {
		heap.Fix(&q.tenantHeap, tenant.index)
	}
	q.mu.Unlock()
	q.granter.returnGrant()
}

func (q *AdmissionQueue) hasWaitingRequests() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tenantHeap) > 0
}

func (q *AdmissionQueue) granted() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tenantHeap) == 0 {
		return false
	}
	tenant := q.tenantHeap[0]
	item := heap.Pop(&tenant.waitingWorkHeap).(*waitingWork)
	item.ch <- struct{}{}
	tenant.used++
	if len(tenant.waitingWorkHeap) > 0 {
		heap.Fix(&q.tenantHeap, tenant.index)
	} else {
		heap.Remove(&q.tenantHeap, tenant.index)
	}
	return true
}

func (q *AdmissionQueue) gcTenantsAndResetTokens() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for id, info := range q.tenants {
		if info.used == 0 && len(info.waitingWorkHeap) == 0 {
			delete(q.tenants, id)
		}
		if q.usesTokens {
			info.used = 0
			// All the heap members will reset used=0, so no need to change heap
			// ordering.
		}
	}
}

// tenantInfo is the per-tenant information in the tenantHeap.
type tenantInfo struct {
	id uint64
	// used can be the currently used slots, or the tokens granted within the last
	// interval.
	used            uint64
	waitingWorkHeap waitingWorkHeap

	// The index is maintained by the heap.Interface methods, and represents the
	// index of the item in the heap.
	index int
}

type tenantHeap []*tenantInfo

var _ heap.Interface = (*tenantHeap)(nil)

func (th *tenantHeap) Len() int {
	return len(*th)
}

func (th *tenantHeap) Less(i, j int) bool {
	return (*th)[i].used < (*th)[j].used
}

func (th *tenantHeap) Swap(i, j int) {
	(*th)[i], (*th)[j] = (*th)[j], (*th)[i]
	(*th)[i].index = i
	(*th)[j].index = j
}

func (th *tenantHeap) Push(x interface{}) {
	n := len(*th)
	item := x.(*tenantInfo)
	item.index = n
	*th = append(*th, item)
}

func (th *tenantHeap) Pop() interface{} {
	old := *th
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*th = old[0 : n-1]
	return item
}

// waitingWork is the per-work information in the waitingWorkHeap.
type waitingWork struct {
	priority   WorkPriority
	createTime int64
	ch         chan struct{}
	// The index is maintained by the heap.Interface methods, and represents the
	// index of the item in the heap.
	index int
}

type waitingWorkHeap []*waitingWork

var _ heap.Interface = (*waitingWorkHeap)(nil)

func (wwh *waitingWorkHeap) Len() int { return len(*wwh) }

func (wwh *waitingWorkHeap) Less(i, j int) bool {
	if (*wwh)[i].priority == (*wwh)[j].priority {
		return (*wwh)[i].createTime < (*wwh)[j].createTime
	}
	return (*wwh)[i].priority > (*wwh)[j].priority
}

func (wwh *waitingWorkHeap) Swap(i, j int) {
	(*wwh)[i], (*wwh)[j] = (*wwh)[j], (*wwh)[i]
	(*wwh)[i].index = i
	(*wwh)[j].index = j
}

func (wwh *waitingWorkHeap) Push(x interface{}) {
	n := len(*wwh)
	item := x.(*waitingWork)
	item.index = n
	*wwh = append(*wwh, item)
}

func (wwh *waitingWorkHeap) Pop() interface{} {
	old := *wwh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
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

type AdmissionQueueMetrics struct {
	Requested       *metric.Counter
	Admitted        *metric.Counter
	Errored         *metric.Counter
	WaitDurationSum *metric.Counter
	WaitDurations   *metric.Histogram
	WaitQueueLength *metric.Gauge
}

func (AdmissionQueueMetrics) MetricStruct() {}

func MakeAdmissionQueueMetrics(name string) AdmissionQueueMetrics {
	return AdmissionQueueMetrics{
		Requested:       metric.NewCounter(addName(name, requestedMeta)),
		Admitted:        metric.NewCounter(addName(name, admittedMeta)),
		Errored:         metric.NewCounter(addName(name, erroredMeta)),
		WaitDurationSum: metric.NewCounter(addName(name, waitDurationSumMeta)),
		WaitDurations: metric.NewLatency(
			addName(name, waitDurationsMeta), base.DefaultHistogramWindowInterval()),
		WaitQueueLength: metric.NewGauge(addName(name, waitQueueLengthMeta)),
	}
}
