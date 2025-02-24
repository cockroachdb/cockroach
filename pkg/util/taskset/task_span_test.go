// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package taskset

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskSetSingleWorker(t *testing.T) {
	// When a single worker claims tasks from a taskSet, it should claim all
	// tasks in order.
	tasks := MakeTaskSet(10)
	var found []TaskId

	for next := tasks.ClaimFirst(); !next.IsDone(); next = tasks.ClaimNext(next) {
		found = append(found, next)
	}

	// The tasks are claimed in a weird order because it assumes the first span
	// is already claimed. It's possible to get a smoother task distribution by
	// allocating tasks and splitting the spans when initializing the workers.
	require.Equal(t, []TaskId{
		5, 6, 7, 8, 9,
		2, 3, 4,
		1,
		0,
	}, found)
}

func TestTaskSetParallel(t *testing.T) {
	taskCount := min(rand.Int63n(10000), 16)
	tasks := MakeTaskSet(taskCount)
	workers := make([]TaskId, 16)
	var found []TaskId

	for i := range workers {
		workers[i] = tasks.ClaimFirst()
		found = append(found, workers[i])
	}

	for {
		// pick a random worker to claim the next task
		workerIndex := rand.Intn(len(workers))
		prevTask := workers[workerIndex]
		next := tasks.ClaimNext(prevTask)
		if next.IsDone() {
			break
		}
		workers[workerIndex] = next
		found = append(found, next)
	}

	// build a map of the found tasks to ensure they are unique
	taskMap := make(map[TaskId]struct{})
	for _, task := range found {
		taskMap[task] = struct{}{}
	}
	require.Len(t, taskMap, int(taskCount))
}
