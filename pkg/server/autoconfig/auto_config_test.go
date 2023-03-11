// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package autoconfig_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type testProvider struct {
	t *testing.T
}

var testTasks = []autoconfigpb.Task{
	{
		TaskID:      123,
		Description: "test task that creates a system table",
		MinVersion:  clusterversion.ByKey(clusterversion.V23_1Start),
		Payload: &autoconfigpb.Task_SimpleSQL{
			SimpleSQL: &autoconfigpb.SimpleSQL{
				Statements: []string{"CREATE TABLE IF NOT EXISTS system.foo(x INT)"},
			},
		},
	},
	{
		TaskID:      345,
		Description: "test task that fails with an error",
		MinVersion:  clusterversion.ByKey(clusterversion.V23_1Start),
		Payload: &autoconfigpb.Task_SimpleSQL{
			SimpleSQL: &autoconfigpb.SimpleSQL{
				Statements: []string{"SELECT invalid"},
			},
		},
	},
	{
		TaskID:      456,
		Description: "test task that creates another system table",
		MinVersion:  clusterversion.ByKey(clusterversion.V23_1Start),
		Payload: &autoconfigpb.Task_SimpleSQL{
			SimpleSQL: &autoconfigpb.SimpleSQL{
				Statements: []string{"CREATE TABLE IF NOT EXISTS system.bar(y INT)"},
			},
		},
	},
}

func (p *testProvider) RegisterTasksChannel() <-chan struct{} {
	p.t.Logf("runner has registered task channel")
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return ch
}

func (p *testProvider) ReportLastKnownCompletedTaskID(taskID uint64) {
	p.t.Logf("runner reports completed task %d", taskID)
}

func (p *testProvider) GetTasks() []autoconfigpb.Task {
	p.t.Logf("runner retrieving tasks")
	return testTasks
}

func TestAutoConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			AutoConfig: &autoconfig.TestingKnobs{
				Provider: &testProvider{t},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	t.Logf("waiting for runner job...")
	testutils.SucceedsSoon(t, func() error {
		var jobID int64
		if err := sqlDB.QueryRowContext(ctx, `SELECT id FROM system.jobs WHERE id = $1`,
			jobs.AutoConfigRunnerJobID).Scan(&jobID); err != nil {
			return err
		}
		t.Logf("found runner job: %d", jobID)
		return nil
	})

	// Wake up the runner job to accelerate the test.
	registry := s.JobRegistry().(*jobs.Registry)
	registry.NotifyToAdoptJobs()

	waitForTaskCompleted := func(taskID uint64) (result []byte) {
		completionMarker := autoconfig.MakeTaskInfoKey([]byte(autoconfig.InfoKeyCompletionPrefix), taskID)
		testutils.SucceedsSoon(t, func() error {
			err := sqlDB.QueryRowContext(ctx, `
SELECT value FROM system.job_info WHERE job_id = $1 AND info_key = $2 LIMIT 1`,
				jobs.AutoConfigRunnerJobID,
				completionMarker).Scan(&result)
			if err != nil {
				return err
			}
			t.Logf("found task completion: %q", string(result))
			return nil
		})
		return result
	}

	t.Logf("waiting for first task completion marker...")
	result := waitForTaskCompleted(testTasks[0].TaskID)
	require.Equal(t, []byte("task success"), result)

	t.Logf("check that the effects of the first task are visible")
	var unused int
	err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM system.foo`).Scan(&unused)
	require.NoError(t, err)

	t.Logf("waiting for 2nd task completion marker...")
	result = waitForTaskCompleted(testTasks[1].TaskID)
	require.Equal(t, []byte("task error"), result)

	t.Logf("waiting for 3rd task completion marker...")
	result = waitForTaskCompleted(testTasks[2].TaskID)
	require.Equal(t, []byte("task success"), result)

	t.Logf("check that the effects of the first tasks are visible")
	err = sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM system.bar`).Scan(&unused)
	require.NoError(t, err)
}
