// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type statusTrackingExecutor struct {
	numExec int
	counts  map[State]int
}

func (s *statusTrackingExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	s.numExec++
	return nil
}

func (s *statusTrackingExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobState State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	s.counts[jobState]++
	return nil
}

func (s *statusTrackingExecutor) Metrics() metric.Struct {
	return nil
}

func (s *statusTrackingExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *ScheduledJob,
) (string, error) {
	return "", errors.AssertionFailedf("unimplemented method: 'GetCreateScheduleStatement'")
}

var _ ScheduledJobExecutor = &statusTrackingExecutor{}

func newStatusTrackingExecutor() *statusTrackingExecutor {
	return &statusTrackingExecutor{counts: make(map[State]int)}
}

func TestScheduledJobExecutorRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const executorName = "test-executor"
	instance := newStatusTrackingExecutor()
	defer registerScopedScheduledJobExecutor(executorName, instance)()

	registered, err := GetScheduledJobExecutor(executorName)
	require.NoError(t, err)
	require.Equal(t, instance, registered)
}

func TestJobTerminationNotification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	const executorName = "test-executor"
	ex := newStatusTrackingExecutor()
	defer registerScopedScheduledJobExecutor(executorName, ex)()

	// Create a single job.
	schedule := h.newScheduledJobForExecutor("test_job", executorName, nil)
	ctx := context.Background()
	schedules := ScheduledJobDB(h.cfg.DB)
	require.NoError(t, schedules.Create(ctx, schedule))

	// Pretend it completes multiple runs with terminal statuses.
	for _, s := range []State{StateCanceled, StateFailed, StateSucceeded} {
		require.NoError(t, h.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return NotifyJobTermination(
				ctx, txn, h.env, 123, s, nil, schedule.ScheduleID(),
			)
		}))
	}

	// Verify counts.
	require.Equal(t, map[State]int{StateSucceeded: 1, StateFailed: 1, StateCanceled: 1}, ex.counts)
}
