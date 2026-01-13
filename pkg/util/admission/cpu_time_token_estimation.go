// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

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
//
// Note that cpuTimeTokenEstimator is NOT thread-safe. It is meant to be used
// in WorkQueue, which handles locking via a WorkQueue-wide lock. This approach
// of protecting many resources in WorkQueue via a shared lock reduces the
// number of Lock calls made in Admit, etc. as compared to if we did locking
// within cpuTimeTokenEstimator.
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
