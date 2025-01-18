// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiqueue

import (
	"container/heap"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admit_long"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Task represents a request for a Permit for a piece of work that needs to be
// done. It is created by a call to MultiQueue.Add. After creation,
// Task.GetWaitChan is called to get a permit, and after all work related to
// this task is done, MultiQueue.Release must be called so future tasks can
// run. Alternatively, if the user decides they no longer want to run their
// work, MultiQueue.Cancel can be called to release the permit without waiting
// for the permit. MultiQueue.Cancel *must* not be called if the caller has
// successfully received from GetWaitChan, since that represents the caller
// has accepted the permit.
type Task struct {
	priority  float64
	queueType int
	heapIdx   int
	permitC   chan *Permit
}

// GetWaitChan returns a permit channel which is used to wait for the permit to
// become available.
func (t *Task) GetWaitChan() <-chan *Permit {
	return t.permitC
}

func (t *Task) String() string {
	return redact.Sprintf("{Queue type : %d, Priority :%f}", t.queueType, t.priority).StripMarkers()
}

// notifyHeap is a standard go heap over tasks.
type notifyHeap []*Task

func (h notifyHeap) Len() int {
	return len(h)
}

func (h notifyHeap) Less(i, j int) bool {
	return h[j].priority < h[i].priority
}

func (h notifyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *notifyHeap) Push(x interface{}) {
	t := x.(*Task)
	// Set the index to the end, it will be moved later
	t.heapIdx = h.Len()
	*h = append(*h, t)
}

func (h *notifyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	// No longer in the heap so clear the index
	x.heapIdx = -1

	return x
}

// tryRemove attempts to remove the task from this queue by iterating through
// the queue. Will returns true if the task was successfully removed.
func (h *notifyHeap) tryRemove(task *Task) bool {
	if task.heapIdx < 0 {
		return false
	}
	heap.Remove(h, task.heapIdx)
	return true
}

type Granter interface {
	setRequester(roachpb.StoreID, Requester)
	tryGet() (bool, admit_long.WorkResourceUsageReporter)
}

// Requester mutex is ordered before Granter mutex.
//
// So hasWaiting and grant must not be called with Granter mutex held.
type Requester interface {
	hasWaiting() bool
	grant(admit_long.WorkResourceUsageReporter) bool
}

type ConcurrencyLimitGranter struct {
	requester Requester
	grantCh   chan struct{}
	mu        struct {
		syncutil.Mutex
		concurrencyLimit int
		remainingRuns    int
	}
}

var _ Granter = &ConcurrencyLimitGranter{}

func newConcurrencyLimitGranter(concurrentLimit int) *ConcurrencyLimitGranter {
	g := &ConcurrencyLimitGranter{
		grantCh: make(chan struct{}, 1),
	}
	g.mu.concurrencyLimit = concurrentLimit
	g.mu.remainingRuns = concurrentLimit
	return g
}

func (g *ConcurrencyLimitGranter) updateConcurrencyLimit(newLimit int) {
	g.mu.Lock()
	diff := newLimit - g.mu.concurrencyLimit
	tryGrant := diff > 0 && g.mu.remainingRuns == 0
	g.mu.remainingRuns = g.mu.remainingRuns + diff
	g.mu.concurrencyLimit = newLimit
	g.mu.Unlock()
	if tryGrant {
		g.tryGrant()
	}
}

func (g *ConcurrencyLimitGranter) getConcurrencyLimit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.concurrencyLimit
}

func (g *ConcurrencyLimitGranter) getAvailableLen() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return max(0, g.mu.remainingRuns)
}

func (g *ConcurrencyLimitGranter) tryGrant() {
	g.grantCh <- struct{}{}
	defer func() {
		<-g.grantCh
	}()
	// Consider the following sequence:
	// hasWaiting: true; tryGet: true; grant: false (because waiting work has been canceled)
	// meanwhile another tryGet has returned false, and there is work waiting.
	//
	// Hence, we loop back and call hasWaiting.
	for g.requester.hasWaiting() {
		success, reporter := g.tryGet()
		if !success {
			return
		}
		if !g.requester.grant(reporter) {
			reporter.WorkDone()
			g.mu.Lock()
			g.mu.remainingRuns++
			g.mu.Unlock()
		}
	}
}

func (g *ConcurrencyLimitGranter) setRequester(_ roachpb.StoreID, requester Requester) {
	g.requester = requester
}

func (g *ConcurrencyLimitGranter) tryGet() (bool, admit_long.WorkResourceUsageReporter) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.mu.remainingRuns > 0 {
		g.mu.remainingRuns--
		return true, clgReporter{g: g}
	}
	return false, nil
}

func (g *ConcurrencyLimitGranter) done() {
	g.mu.Lock()
	g.mu.remainingRuns++
	tryGrant := g.mu.remainingRuns > 0
	g.mu.Unlock()
	// After the Unlock, a tryGet can race ahead and grab a slot. This is fine
	// since this Granter only deals with a single Requester, and the Requester
	// is responsible for ensuring ordering among the various work items that
	// want a slot.
	if tryGrant {
		g.tryGrant()
	}
}

type clgReporter struct {
	g *ConcurrencyLimitGranter
}

var _ admit_long.WorkResourceUsageReporter = clgReporter{}

func (r clgReporter) WorkStart()                                   {}
func (r clgReporter) MeasureCPU(g int)                             {}
func (r clgReporter) CumulativeWriteBytes(w admit_long.WriteBytes) {}
func (r clgReporter) WorkDone() {
	r.g.done()
}
func (r clgReporter) GetIDForDebugging() uint64 { return 0 }

// MultiQueue is a type that round-robins through a set of typed queues, each
// independently prioritized. A MultiQueue is constructed with a concurrencySem
// which is the number of concurrent jobs this queue will allow to run. Tasks
// are added to the queue using MultiQueue.Add. That will return a channel that
// should be received from. It will be notified when the waiting job is ready to
// be run. Once the job is completed, MultiQueue.TaskDone must be called to
// return the Permit to the queue so that the next Task can be started.
type MultiQueue struct {
	granter        Granter
	mu             syncutil.Mutex
	mapping        map[int]int
	lastQueueIndex int
	outstanding    []notifyHeap
}

var _ Requester = &MultiQueue{}

// NewMultiQueue creates a new queue. The queue is not started, and start needs
// to be called on it first.
func NewMultiQueue(maxConcurrency int) *MultiQueue {
	g := newConcurrencyLimitGranter(maxConcurrency)
	return NewMultiQueueWithGranter(g)
}

func NewMultiQueueWithGranter(g Granter) *MultiQueue {
	queue := &MultiQueue{
		granter:        g,
		mapping:        make(map[int]int),
		lastQueueIndex: -1,
	}
	clg, ok := g.(*ConcurrencyLimitGranter)
	if ok {
		// The StoreID does not matter for the ConcurrencyLimitGranter, so register
		// now -- this also helps with unit tests.
		clg.setRequester(1, queue)
	}
	return queue
}

func (m *MultiQueue) SetStoreID(storeID roachpb.StoreID) {
	_, ok := m.granter.(*ConcurrencyLimitGranter)
	if !ok {
		m.granter.setRequester(storeID, m)
	}
}

// Permit is a token which is returned from a Task.GetWaitChan call.
type Permit struct {
	// valid is only used to guard against double Release.
	valid    bool
	Reporter admit_long.WorkResourceUsageReporter
}

// grant will run the next task in order round-robin through the queues and in
// priority order within a queue.
func (m *MultiQueue) grant(reporter admit_long.WorkResourceUsageReporter) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := 0; i < len(m.outstanding); i++ {
		// Start with the next queue in order and iterate through all empty queues.
		// If all queues are empty then return, as there is nothing to run.
		index := (m.lastQueueIndex + i + 1) % len(m.outstanding)
		if m.outstanding[index].Len() > 0 {
			task := heap.Pop(&m.outstanding[index]).(*Task)
			task.permitC <- &Permit{valid: true, Reporter: reporter}
			m.lastQueueIndex = index
			return true
		}
	}
	return false
}

func (m *MultiQueue) hasWaiting() bool {
	return m.QueueLen() > 0
}

// UpdateConcurrencyLimit updates the concurrencyLimit and remainingRuns field.
// We add the delta from new and old concurrencyLimit to remainingRuns.
func (m *MultiQueue) UpdateConcurrencyLimit(newLimit int) {
	m.granter.(*ConcurrencyLimitGranter).updateConcurrencyLimit(newLimit)
}

// Add returns a Task that must be closed (calling m.Release(..)) to
// release the Permit. The number of types is expected to
// be relatively small and not be changing over time.
func (m *MultiQueue) Add(queueType int, priority float64, maxQueueLength int64) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	currentLen := int64(m.queueLenLocked())
	queueTask := false
	var reporter admit_long.WorkResourceUsageReporter
	if currentLen == 0 {
		var success bool
		success, reporter = m.granter.tryGet()
		if !success {
			queueTask = true
		}
	} else {
		queueTask = true
	}
	if queueTask && maxQueueLength >= 0 && currentLen >= maxQueueLength {
		return nil, errors.Newf("queue is too long %d > %d", currentLen, maxQueueLength)
	}
	newTask := &Task{
		priority:  priority,
		permitC:   make(chan *Permit, 1),
		heapIdx:   -1,
		queueType: queueType,
	}
	if !queueTask {
		newTask.permitC <- &Permit{valid: true, Reporter: reporter}
		return newTask, nil
	}
	pos, ok := m.mapping[queueType]
	if !ok {
		// Append a new entry to both mapping and outstanding each time there is
		// a new queue type.
		pos = len(m.outstanding)
		m.mapping[queueType] = pos
		m.outstanding = append(m.outstanding, notifyHeap{})
	}
	heap.Push(&m.outstanding[pos], newTask)
	return newTask, nil
}

// Cancel will cancel a Task that has not started yet, that is, either
// task.permitC has no Permit, or has a Permit that has not been retrieved.
// This is useful if it is determined that it is no longer required to run
// this Task.
func (m *MultiQueue) Cancel(task *Task) {
	var permit *Permit
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		// Find the right queue and try to remove it. Queues monotonically grow, and a
		// Task will track its position within the queue.
		if task.heapIdx >= 0 {
			queueIdx := m.mapping[task.queueType]
			ok := m.outstanding[queueIdx].tryRemove(task)
			if !ok {
				panic("task not found in queue")
			}
			// Was in the heap, so the channel must be empty.
			select {
			case <-task.permitC:
				panic("channel is not empty")
			default:
			}
			// Close the permit channel. NB: no one should be waiting on this
			// channel, so this is just defensive.
			close(task.permitC)
			return
		}
		// Not in the heap. So must have been granted a Permit that has not been
		// retrieved.
		select {
		case permit = <-task.permitC:
		default:
			panic("channel is empty")
		}
		close(task.permitC)
	}()
	if permit != nil {
		m.Release(permit)
	}
}

// Release needs to be called once the Task that was running has completed and
// is no longer using system resources.
func (m *MultiQueue) Release(permit *Permit) {
	if !permit.valid {
		panic("double release of permit")
	}
	permit.valid = false
	permit.Reporter.WorkDone()
	permit.Reporter = nil
}

// AvailableLen returns the number of additional tasks that can be added without
// queueing. This will return 0 if there is anything queued.
//
// Must only be called for a MultiQueue paired with a ConcurrencyLimitGranter.
func (m *MultiQueue) AvailableLen() int {
	return m.granter.(*ConcurrencyLimitGranter).getAvailableLen()
}

// QueueLen returns the number of Tasks that are waiting for a Permit.
func (m *MultiQueue) QueueLen() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.queueLenLocked()
}

func (m *MultiQueue) queueLenLocked() int {
	count := 0
	for i := 0; i < len(m.outstanding); i++ {
		count += len(m.outstanding[i])
	}
	return count
}

// MaxConcurrency exposes the multi-queue's concurrency limit. Must only be
// called for a MultiQueue paired with a ConcurrencyLimitGranter.
func (m *MultiQueue) MaxConcurrency() int {
	return m.granter.(*ConcurrencyLimitGranter).getConcurrencyLimit()
}

// Lock ordering:
// Requester mutex < AdmitLongGranter.mu < admit_long.WorkGranter mutex.
//
// AdmitLongGranter.done will call
// admit_long.WorkGrantHandle.UsageReporter.WorkDone which can call back into
// AdmitLongGranter and into Requester. So Requester and AdmitLongGranter
// should not hold any mutexes when calling or forwarding.

type AdmitLongGranter struct {
	workGranter admit_long.WorkGranter
	roachpb.StoreID
	requester                    Requester
	withoutPermissionConcurrency atomic.Int64
}

var _ Granter = &AdmitLongGranter{}
var _ admit_long.WorkRequester = &AdmitLongGranter{}

func NewAdmitLongGranter(
	granter admit_long.WorkGranter, withoutPermissionConcurrency int,
) *AdmitLongGranter {
	g := &AdmitLongGranter{
		workGranter: granter,
	}
	g.withoutPermissionConcurrency.Store(int64(withoutPermissionConcurrency))
	return g
}

func (g *AdmitLongGranter) UpdateWithoutPermissionConcurrency(newLimit int) {
	g.withoutPermissionConcurrency.Store(int64(newLimit))
}

func (g *AdmitLongGranter) setRequester(storeID roachpb.StoreID, requester Requester) {
	g.StoreID = storeID
	g.requester = requester
	g.workGranter.RegisterRequester(admit_long.WorkCategoryAndStore{
		Category: admit_long.SnapshotRecv,
		Store:    g.StoreID,
	}, g, 1)
}

func (g *AdmitLongGranter) tryGet() (bool, admit_long.WorkResourceUsageReporter) {
	return g.workGranter.TryGet(admit_long.WorkCategoryAndStore{
		Category: admit_long.SnapshotRecv,
		Store:    g.StoreID,
	})
}

func (g *AdmitLongGranter) GetAllowedWithoutPermissionForStore() int {
	return int(g.withoutPermissionConcurrency.Load())
}

func (g *AdmitLongGranter) GetScore() (bool, admit_long.WorkScore) {
	return g.requester.hasWaiting(), admit_long.WorkScore{
		Optional: false,
		Score:    0,
	}
}

func (g *AdmitLongGranter) Grant(reporter admit_long.WorkResourceUsageReporter) bool {
	return g.requester.grant(reporter)
}

//
// Plumbing to use alternative MultiQueue and Granter.

// TODO: will need to plumb WorkGrantHandle to the caller.
