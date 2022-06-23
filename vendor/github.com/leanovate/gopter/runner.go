package gopter

import (
	"sync"
	"time"
)

type shouldStop func() bool

type worker func(int, shouldStop) *TestResult

type runner struct {
	sync.RWMutex
	parameters *TestParameters
	worker     worker
}

func (r *runner) mergeCheckResults(r1, r2 *TestResult) *TestResult {
	var result TestResult

	switch {
	case r1 == nil:
		return r2
	case r1.Status != TestPassed && r1.Status != TestExhausted:
		result = *r1
	case r2.Status != TestPassed && r2.Status != TestExhausted:
		result = *r2
	default:
		result.Status = TestExhausted

		if r1.Succeeded+r2.Succeeded >= r.parameters.MinSuccessfulTests &&
			float64(r1.Discarded+r2.Discarded) <= float64(r1.Succeeded+r2.Succeeded)*r.parameters.MaxDiscardRatio {
			result.Status = TestPassed
		}
	}

	result.Succeeded = r1.Succeeded + r2.Succeeded
	result.Discarded = r1.Discarded + r2.Discarded

	return &result
}

func (r *runner) runWorkers() *TestResult {
	var stopFlag Flag
	defer stopFlag.Set()

	start := time.Now()
	if r.parameters.Workers < 2 {
		result := r.worker(0, stopFlag.Get)
		result.Time = time.Since(start)
		return result
	}
	var waitGroup sync.WaitGroup
	waitGroup.Add(r.parameters.Workers)
	results := make(chan *TestResult, r.parameters.Workers)
	combinedResult := make(chan *TestResult)

	go func() {
		var combined *TestResult
		for result := range results {
			combined = r.mergeCheckResults(combined, result)
		}
		combinedResult <- combined
	}()
	for i := 0; i < r.parameters.Workers; i++ {
		go func(workerIdx int) {
			defer waitGroup.Done()
			results <- r.worker(workerIdx, stopFlag.Get)
		}(i)
	}
	waitGroup.Wait()
	close(results)

	result := <-combinedResult
	result.Time = time.Since(start)
	return result
}
