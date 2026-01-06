// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// WorkQueueWithCPUTimeTokenEstimators wraps WorkQueue, in order to provide
// CPU time token estimation needed by CPU time token AC. When Admit is
// called, the request hasn't yet executed, so we do not know how much CPU
// time will be used by goroutines used to service it. When AdmittedWorkDone
// is called, the request is done executing, so we have a measurement of CPU
// time used servicing the request, courtesy of grunning.
// WorkQueueWithCPUTimeTokenEstimators uses past measurements from grunning
// to make estimates at admission time.
//
// The estimator state is stored in a per-tenant fashion. This is worth
// doing, since workloads from different tenants are very heterogeneous in
// terms of CPU demand. The details of making estimations are left to
// cpuTimeTokenEstimator. WorkQueueWithCPUTimeTokenEstimators orchestrates
// a collection of cpuTimeTokenEstimators, including handling GC of
// estimators, in case a tenant hasn't run any recent workload.
type WorkQueueWithCPUTimeTokenEstimators struct {
	wc workQueueI
	mu struct {
		syncutil.Mutex
		globalEstimator     *cpuTimeTokenEstimator
		perTenantEstimators map[uint64]*cpuTimeTokenEstimator
	}
	timeSource timeutil.TimeSource
	closeCh    chan struct{}
}

func initWorkQueueWithCPUTimeTokenEstimators(
	wc workQueueI,
	timeSource timeutil.TimeSource,
) *WorkQueueWithCPUTimeTokenEstimators {
	q := &WorkQueueWithCPUTimeTokenEstimators{
		wc:         wc,
		timeSource: timeSource,
	}
	q.mu.globalEstimator = &cpuTimeTokenEstimator{}
	q.mu.perTenantEstimators = make(map[uint64]*cpuTimeTokenEstimator)
	return q
}

// AdmitResponse is the return value of Admit. We use a struct to enable
// passing certain internal information such as requestedCount from Admit
// to AdmittedWorkDone, without having to change code that calls the
// WorkQueue.
// TODO(josh): Move to work_queue.go in next commit.
type AdmitResponse struct {
	// If true (and if Err == nil), admission control is enabled.
	Enabled bool
	Err     error

	tenantID roachpb.TenantID
	// requestedCount is the number of slots or tokens taken at Admit time.
	// It is useful to return, so that in AdmittedWorkDone, we can adjust
	// the deduction, in cases where we have more information, such as in
	// CPU time token AC, where a grunning-based measurement of CPU time
	// is available by the time AdmittedWorkDone is called.
	requestedCount int64
}

// workQueueI abstracts WorkQueue for testing.
type workQueueI interface {
	Admit(ctx context.Context, info WorkInfo) AdmitResponse
	AdmittedWorkDone(resp AdmitResponse, cpuTime time.Duration)
}

// start starts a goroutine which periodically updates the estimators.
//
// The goroutine started by start also implements GC of the per-tenant data
// structures. GC is implemented to bound the size of the estimators in
// memory to be proportional to the number of active tenants, instead of to
// the process lifetime.
func (q *WorkQueueWithCPUTimeTokenEstimators) start() {
	ticker := q.timeSource.NewTicker(time.Second)
	go func() {
		for {
			select {
			case _ = <-ticker.Ch():
				q.updateEstimators()
			case <-q.closeCh:
				return
			}
		}
	}()
}

// updateEstimators iterates through all estimators owned by
// WorkQueueWithCPUTimeTokenEstimators. If an estimator has seen no recent
// requests, it is garbage-collected. Else, it is updated.
func (q *WorkQueueWithCPUTimeTokenEstimators) updateEstimators() {
	q.mu.Lock()
	defer q.mu.Unlock()
	_ = q.mu.globalEstimator.update() // global estimator is never GCed
	for tenantID, e := range q.mu.perTenantEstimators {
		if noActivity := e.update(); noActivity {
			delete(q.mu.perTenantEstimators, tenantID)
		}
	}
}

// See WorkQueue for the Admit docstring.
func (q *WorkQueueWithCPUTimeTokenEstimators) Admit(
	ctx context.Context, info WorkInfo,
) AdmitResponse {
	func() {
		q.mu.Lock()
		defer q.mu.Unlock()

		tenantID := info.TenantID.ToUint64()
		q.initEstimatorIfNeededLocked(tenantID)
		estimate := q.mu.perTenantEstimators[tenantID].estimateTokensToBeUsed()
		// For CPU time token AC, we always use the estimation made here as
		// info.RequestedCount.
		info.RequestedCount = estimate
	}()

	return q.wc.Admit(ctx, info)
}

// See WorkQueue for the AdmittedWorkDone docstring.
func (q *WorkQueueWithCPUTimeTokenEstimators) AdmittedWorkDone(
	resp AdmitResponse, cpuTime time.Duration,
) {
	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		tokensUsed := cpuTime.Nanoseconds()
		q.mu.globalEstimator.workDone(tokensUsed)

		tenantIDUint64 := resp.tenantID.ToUint64()
		q.initEstimatorIfNeededLocked(tenantIDUint64)
		q.mu.perTenantEstimators[tenantIDUint64].workDone(tokensUsed)
	}()

	q.wc.AdmittedWorkDone(resp, cpuTime)
}

func (q *WorkQueueWithCPUTimeTokenEstimators) initEstimatorIfNeededLocked(tenantID uint64) {
	_, ok := q.mu.perTenantEstimators[tenantID]
	if !ok {
		perTenantEstimator := &cpuTimeTokenEstimator{}
		perTenantEstimator.init(q.mu.globalEstimator.estimateTokensToBeUsed())
		q.mu.perTenantEstimators[tenantID] = perTenantEstimator
	}
}

// cpuTimeTokenEstimator estimates the CPU time token cost of future
// requests based on the observed costs of past requests. See
// WorkQueueWithCPUTimeTokenEstimators for the broader context.
//
// The estimator works by exponentially smoothing the mean token usage
// of recently completed work:
//
//   - workDone(tokens) should be called for every completed request to
//     record its token cost.
//
//   - update() should be called periodically (once per second).
//     Each call computes the mean token usage of all requests completed
//     since the last update and folds that mean into the current estimate
//     using exponential smoothing. The function returns true if no work
//     was recorded since the last call to update.
//
//   - estimateTokensToBeUsed() returns the current smoothed estimate (with
//     a minimum of 1).
//
// Rationale:
//
//   - **Mean over recent requests**: Workloads contain both cheap and
//     expensive requests. Using the mean CPU cost of a batch of completed
//     requests gives a representative estimate of average cost over short
//     windows.
//
//   - **Exponential smoothing**: A tenant's workload may shift over time.
//     Using exponential smoothing weights recent behavior more heavily than
//     older behavior, avoiding a stale lifetime-average estimate while still
//     filtering noise.
//
// The estimator resets its accumulated counts on each call to update.
type cpuTimeTokenEstimator struct {
	// Reset on every call to update.
	workDoneCount    int64
	cumulativeTokens int64

	estimate int64
}

func (e *cpuTimeTokenEstimator) init(initialEstimate int64) {
	*e = cpuTimeTokenEstimator{estimate: initialEstimate}
}

func (e *cpuTimeTokenEstimator) update() (noActivity bool) {
	if e.workDoneCount == 0 {
		return true
	}

	const alpha = 0.5
	mean := e.cumulativeTokens / e.workDoneCount
	e.estimate = int64(alpha*float64(mean) + (1-alpha)*float64(e.estimate))

	e.workDoneCount = 0
	e.cumulativeTokens = 0

	return false
}

func (e *cpuTimeTokenEstimator) estimateTokensToBeUsed() (tokens int64) {
	return max(1, e.estimate)
}

func (e *cpuTimeTokenEstimator) workDone(tokens int64) {
	e.workDoneCount++
	e.cumulativeTokens += tokens
}
