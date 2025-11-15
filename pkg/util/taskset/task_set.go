// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package taskset provides a generic work distribution mechanism for
// coordinating parallel workers. TaskSet hands out integer identifiers
// (TaskIDs) that workers can claim and process. The TaskIDs themselves have no
// inherent meaning - it's up to the caller to map each TaskID to actual work
// (e.g., file indices, key ranges, batch numbers, etc.).
//
// Example usage:
//
//	tasks := taskset.MakeTaskSet(100, 4)  // 100 work items, 4 workers
//
//	// Worker goroutine
//	for taskID := tasks.ClaimFirst(); !taskID.IsDone(); taskID = tasks.ClaimNext(taskID) {
//	    // Map taskID to actual work
//	    processFile(files[taskID])
//	    // or: processKeyRange(splits[taskID], splits[taskID+1])
//	    // or: processBatch(taskID*batchSize, (taskID+1)*batchSize)
//	}
package taskset

// TaskID is an abstract integer identifier for a unit of work. The TaskID
// itself has no inherent meaning - callers decide what each TaskID represents
// (e.g., which file to process, which key range to handle, etc.).
type TaskID int64

// taskIDDone is an internal sentinel value indicating no more tasks are available.
// Use TaskID.IsDone() to check if a task is done.
const taskIDDone = TaskID(-1)

func (t TaskID) IsDone() bool {
	return t == taskIDDone
}

// MakeTaskSet creates a new TaskSet with taskCount work items numbered 0
// through taskCount-1, pre-split for the expected number of workers.
//
// The TaskIDs are abstract identifiers with no inherent meaning - the caller
// decides what each TaskID represents. For example:
//   - File processing: MakeTaskSet(100, 4) with TaskID N → files[N]
//   - Key ranges: MakeTaskSet(100, 4) with TaskID N → range [splits[N-1], splits[N])
//   - Row batches: MakeTaskSet(100, 4) with TaskID N → rows [N*1000, (N+1)*1000)
//
// The numWorkers parameter enables better initial load balancing by dividing the
// task range into numWorkers equal spans upfront. For example, with 100 tasks
// and 4 workers:
//   - Worker 1: starts with task 0 from range [0, 25)
//   - Worker 2: starts with task 25 from range [25, 50)
//   - Worker 3: starts with task 50 from range [50, 75)
//   - Worker 4: starts with task 75 from range [75, 100)
//
// Each worker continues claiming sequential tasks from their region (maintaining
// locality), and can steal from other regions if they finish early.
//
// If the number of workers is unknown, use numWorkers=1 for a single span.
func MakeTaskSet(taskCount, numWorkers int64) TaskSet {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	if taskCount <= 0 {
		return TaskSet{unassigned: nil}
	}

	// Pre-split the task range into numWorkers equal spans
	spans := make([]taskSpan, 0, numWorkers)
	tasksPerWorker := taskCount / numWorkers
	remainder := taskCount % numWorkers

	start := TaskID(0)
	for i := int64(0); i < numWorkers; i++ {
		// Distribute remainder evenly by giving first 'remainder' workers one extra task
		spanSize := tasksPerWorker
		if i < remainder {
			spanSize++
		}
		if spanSize > 0 {
			end := start + TaskID(spanSize)
			spans = append(spans, taskSpan{start: start, end: end})
			start = end
		}
	}

	return TaskSet{unassigned: spans}
}

// TaskSet is a generic work distribution coordinator that manages a collection
// of abstract task identifiers (TaskIDs) that can be claimed by workers.
//
// TaskSet implements a work-stealing algorithm optimized for task locality:
// - When a worker completes task N, it tries to claim task N+1 (sequential locality)
// - If task N+1 is unavailable, it falls back to round-robin claiming from the first span
// - This balances load across workers while maintaining locality within each worker
//
// The TaskIDs themselves are just integers (0 through taskCount-1) with no
// inherent meaning. Callers map these identifiers to actual work units such as:
// - File indices (TaskID 5 → process files[5])
// - Key ranges (TaskID 5 → process range [splits[4], splits[5]))
// - Batch numbers (TaskID 5 → process rows [5000, 6000))
//
// TaskSet is NOT safe for concurrent use. Callers must ensure external
// synchronization if the TaskSet is accessed from multiple goroutines.
type TaskSet struct {
	unassigned []taskSpan
}

// ClaimFirst should be called when a worker claims its first task. It returns
// an abstract TaskID to process. The caller decides what this TaskID represents
// (e.g., which file to process, which key range to handle). Returns a TaskID
// where .IsDone() is true if no tasks are available.
//
// ClaimFirst is distinct from ClaimNext because ClaimFirst will always take
// from the first span and rotate it to the end (round-robin), whereas ClaimNext
// tries to claim the next sequential task for locality.
func (t *TaskSet) ClaimFirst() TaskID {
	if len(t.unassigned) == 0 {
		return taskIDDone
	}

	// Take the first task from the first span, then rotate that span to the end.
	// This provides round-robin distribution, ensuring each worker gets tasks
	// from different regions initially for better load balancing.
	span := t.unassigned[0]
	if span.size() == 0 {
		return taskIDDone
	}

	task := span.start
	span.start += 1

	if span.size() == 0 {
		// Span is exhausted, remove it
		t.removeSpan(0)
	} else {
		// Move the span to the end for round-robin distribution
		t.unassigned = append(t.unassigned[1:], span)
	}

	return task
}

// ClaimNext should be called when a worker has completed its current task. It
// returns the next abstract TaskID to process. The caller decides what this
// TaskID represents. Returns a TaskID where .IsDone() is true if no tasks are
// available.
//
// ClaimNext optimizes for locality by attempting to claim lastTask+1 first. If
// that task is unavailable, it falls back to ClaimFirst behavior (round-robin
// from the first span).
func (t *TaskSet) ClaimNext(lastTask TaskID) TaskID {
	next := lastTask + 1

	for i, span := range t.unassigned {
		if span.start != next {
			continue
		}

		span.start += 1

		if span.size() == 0 {
			t.removeSpan(i)
			return next
		}

		t.unassigned[i] = span
		return next
	}

	// If we didn't find the next task in the unassigned set, then we've
	// exhausted the span and need to claim from a different span.
	return t.ClaimFirst()
}

func (t *TaskSet) removeSpan(index int) {
	t.unassigned = append(t.unassigned[:index], t.unassigned[index+1:]...)
}
