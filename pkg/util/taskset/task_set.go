// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package taskset

type TaskId int64

// TaskIdDone is a special task id that indicates the task is done.
//
// TODO(jeffswenson): refactor the API of task set to return TaskIdDone instead
// of false.
const TaskIdDone = TaskId(-1)

func (t TaskId) IsDone() bool {
	return t == TaskIdDone
}

func MakeTaskSet(taskCount int64) TaskSet {
	// TODO(jeffswenson): Should this be initialized with a set of initial
	// workers? Right now it doesn't start with a perfect span split, so if tasks
	// sizes are evenly distributed, we might split spans more than necessary.
	return TaskSet{
		unassigned: []taskSpan{{start: 0, end: TaskId(taskCount)}},
	}
}

// TaskSet manages a collection of tasks that can be claimed by workers. It
// implements an algorithm similar to work stealing. When a worker marks task n
// as completed, it will claim task n+1 if it is available. If its not
// available, it will split the largest span of available tasks and claim task
// from the right side of the split.
//
// This algorithm is designed to allow maximize task locality.
type TaskSet struct {
	unassigned []taskSpan
}

// ClaimFirst should be called when a worker claims its first task. It returns
// the taskId to process and true if there are more tasks to claim.
//
// ClaimFirst is distinct from ClaimNext because ClaimFirst will always split
// the largest span in the unassigned set, whereas ClaimNext will assign from
// the same span until it is exhausted.
func (t *TaskSet) ClaimFirst() TaskId {
	if len(t.unassigned) == 0 {
		return TaskIdDone
	}

	// Find the largest span
	largest := 0
	for i := range t.unassigned {
		if t.unassigned[largest].size() < t.unassigned[i].size() {
			largest = i
		}
	}

	largestSpan := t.unassigned[largest]
	if largestSpan.size() == 0 {
		return TaskIdDone
	}
	if largestSpan.size() == 1 {
		t.lockedRemoveSpan(largest)
		return largestSpan.start
	}

	left, right := largestSpan.split()
	t.unassigned[largest] = left

	task := right.start
	right.start += 1
	if right.size() != 0 {
		t.insertSpan(right, largest+1)
	}

	return task
}

// ClaimNext should be called when a worker has completed its current task. It
// returns the taskId to process and true if there are more tasks to claim.
func (t *TaskSet) ClaimNext(lastTask TaskId) TaskId {
	next := lastTask + 1

	for i, span := range t.unassigned {
		if span.start != next {
			continue
		}

		span.start += 1

		if span.size() == 0 {
			t.lockedRemoveSpan(i)
			return next
		}

		t.unassigned[i] = span
		return next
	}

	// If we didn't find the next task in the unassigned set, then we've
	// exhausted the span and need to claim from a different span.
	return t.ClaimFirst()
}

func (t *TaskSet) insertSpan(span taskSpan, index int) {
	t.unassigned = append(t.unassigned, taskSpan{})
	copy(t.unassigned[index+1:], t.unassigned[index:])
	t.unassigned[index] = span
}

func (t *TaskSet) lockedRemoveSpan(index int) {
	t.unassigned = append(t.unassigned[:index], t.unassigned[index+1:]...)
}
