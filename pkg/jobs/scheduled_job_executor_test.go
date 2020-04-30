// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type statusTrackingExecutor struct {
	numExec int
	counts  map[Status]int
}

func (s *statusTrackingExecutor) ExecuteJob(_ context.Context, _ *ScheduledJob, _ *kv.Txn) error {
	s.numExec++
	return nil
}

func (s *statusTrackingExecutor) NotifyJobTermination(
	_ context.Context, md *JobMetadata, _ *ScheduledJob, _ *kv.Txn,
) error {
	s.counts[md.Status]++
	return nil
}

var _ ScheduledJobExecutor = &statusTrackingExecutor{}

func newScopedStatusTrackingExecutor(name string) (*statusTrackingExecutor, func()) {
	ex := &statusTrackingExecutor{counts: make(map[Status]int)}
	RegisterScheduledJobExecutorFactory(
		name,
		func(_ sqlutil.InternalExecutor) (ScheduledJobExecutor, error) {
			return ex, nil
		})
	return ex, func() {
		delete(registeredExecutorFactories, name)
	}
}

func TestNotifyJobTerminationExpectsTerminalState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, s := range []Status{
		StatusPending, StatusRunning, StatusPaused, StatusReverting,
		StatusCancelRequested, StatusPauseRequested,
	} {
		md := &JobMetadata{
			ID:      123,
			Status:  s,
			Payload: &jobspb.Payload{ScheduleID: 321},
		}
		require.Error(t, NotifyJobTermination(context.Background(), nil, md, nil, nil))
	}
}

func TestScheduledJobExecutorRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const executorName = "test-executor"
	instance, cleanupExecutor := newScopedStatusTrackingExecutor(executorName)
	defer cleanupExecutor()

	registered, err := NewScheduledJobExecutor(executorName, nil)
	require.NoError(t, err)
	require.Equal(t, instance, registered)
}

func TestJobTerminationNotification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	const executorName = "test-executor"
	ex, cleanupExecutor := newScopedStatusTrackingExecutor(executorName)
	defer cleanupExecutor()

	// Create a single job.
	schedule := h.newScheduledJob(t, "test_job", "test sql")
	schedule.rec.ExecutorType = executorName
	ctx := context.Background()
	require.NoError(t, schedule.Create(ctx, h.ex, nil))

	// Pretend it completes multiple runs with terminal statuses.
	for _, s := range []Status{StatusCanceled, StatusFailed, StatusSucceeded} {
		md := &JobMetadata{
			ID:      123,
			Status:  s,
			Payload: &jobspb.Payload{ScheduleID: schedule.ScheduleID()},
		}
		require.NoError(t, NotifyJobTermination(ctx, h.env, md, h.ex, nil))
	}

	// Verify counts.
	require.Equal(t, map[Status]int{StatusSucceeded: 1, StatusFailed: 1, StatusCanceled: 1}, ex.counts)
}
