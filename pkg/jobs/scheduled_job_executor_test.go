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

func newStatusTrackingExecutor() *statusTrackingExecutor {
	return &statusTrackingExecutor{counts: make(map[Status]int)}
}

func TestNotifyJobTerminationExpectsTerminalState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, s := range []Status{
		StatusPending, StatusRunning, StatusPaused, StatusReverting,
		StatusCancelRequested, StatusPauseRequested,
	} {
		md := &JobMetadata{
			ID:     123,
			Status: s,
		}
		require.Error(t, NotifyJobTermination(context.Background(), nil, md, 321, nil, nil))
	}
}

func TestScheduledJobExecutorRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const executorName = "test-executor"
	instance := newStatusTrackingExecutor()
	defer registerScopedScheduledJobExecutor(executorName, instance)()

	registered, err := NewScheduledJobExecutor(executorName, nil)
	require.NoError(t, err)
	require.Equal(t, instance, registered)
}

func TestJobTerminationNotification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	const executorName = "test-executor"
	ex := newStatusTrackingExecutor()
	defer registerScopedScheduledJobExecutor(executorName, ex)()

	// Create a single job.
	schedule := h.newScheduledJobForExecutor("test_job", executorName, nil)
	ctx := context.Background()
	require.NoError(t, schedule.Create(ctx, h.ex, nil))

	// Pretend it completes multiple runs with terminal statuses.
	for _, s := range []Status{StatusCanceled, StatusFailed, StatusSucceeded} {
		md := &JobMetadata{
			ID:      123,
			Status:  s,
			Payload: &jobspb.Payload{},
		}
		require.NoError(t, NotifyJobTermination(ctx, h.env, md, schedule.ScheduleID(), h.ex, nil))
	}

	// Verify counts.
	require.Equal(t, map[Status]int{StatusSucceeded: 1, StatusFailed: 1, StatusCanceled: 1}, ex.counts)
}
