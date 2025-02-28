// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestIngestStrategy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	group, ctx := errgroup.WithContext(context.Background())

	const numWorkers = 10
	const numSpans = 1000

	workers := make([]workerKeys, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = workerKeys{
			routingKey:    fmt.Sprintf("worker-%d", i),
			sqlInstanceID: base.SQLInstanceID(i),
		}
	}

	spans := make([]roachpb.Span, numSpans)
	for i := 0; i < numSpans; i++ {
		spans[i] = roachpb.Span{
			Key:    roachpb.Key(fmt.Sprintf("key-%d", i)),
			EndKey: roachpb.Key(fmt.Sprintf("key-%d-end", i)),
		}
	}

	strategy := newIngestStrategy(spans, workers)
	group.Go(func() error {
		return strategy.run(ctx)
	})

	var workerInput []chan taskAssignment
	var workerTasks []map[taskset.TaskId]bool
	for i := 0; i < numWorkers; i++ {
		workerInput = append(workerInput, make(chan taskAssignment))
		workerTasks = append(workerTasks, make(map[taskset.TaskId]bool))
		group.Go(func() error {
			for assignment := range workerInput[i] {
				workerTasks[i][assignment.TaskID] = true
				strategy.taskCompletionCh <- taskCompletion{
					SqlInstanceID: assignment.SqlInstanceID,
					TaskID:        assignment.TaskID,
				}
			}
			return nil
		})
	}

	group.Go(func() error {
		// Send assignments to workers until there are no more tasks to assign.
		for assignment := range strategy.taskAssignmentCh {
			workerInput[assignment.SqlInstanceID] <- assignment
		}
		for i := 0; i < numWorkers; i++ {
			close(workerInput[i])
		}
		return nil
	})

	require.NoError(t, group.Wait())

	combinedTasks := make(map[taskset.TaskId]bool)
	for i := 0; i < numWorkers; i++ {
		require.NotEmpty(t, workerTasks[i], "Worker %d should have assigned tasks", i)
		for task := range workerTasks[i] {
			require.NotContains(t, combinedTasks, task, "Task %d assigned to multiple workers", task)
			combinedTasks[task] = true
		}
		t.Logf("Worker %d assigned %d tasks", i, len(workerTasks[i]))
	}
	require.Equal(t, len(combinedTasks), len(strategy.ingestSpans), "All tasks should be assigned")
}
