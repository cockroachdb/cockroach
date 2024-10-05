// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package autoconfig_test

import (
	"context"
	gosql "database/sql"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

const testEnvID autoconfigpb.EnvironmentID = "my test env"

type testProvider struct {
	syncutil.Mutex

	t          *testing.T
	notifyCh   chan struct{}
	peekWaitCh chan struct{}
	tasks      []testTask
}

type testTask struct {
	task autoconfigpb.Task
	seen bool
}

var endProfileTask = autoconfigpb.Task{
	TaskID:      autoconfigpb.TaskID(math.MaxUint64),
	Description: "end of configuration profile",
	MinVersion:  clusterversion.TestingBinaryVersion,
	Payload: &autoconfigpb.Task_SimpleSQL{
		SimpleSQL: &autoconfigpb.SimpleSQL{},
	},
}

var testTasks = []testTask{
	{task: autoconfigpb.Task{
		TaskID:      123,
		Description: "test task that creates a system table",
		MinVersion:  clusterversion.TestingBinaryVersion,
		Payload: &autoconfigpb.Task_SimpleSQL{
			SimpleSQL: &autoconfigpb.SimpleSQL{
				UsernameProto: username.NodeUserName().EncodeProto(),
				NonTransactionalStatements: []string{
					// This checks that the non-txn part works properly: SET
					// CLUSTER SETTING can only be run outside of explicit txns.
					"SET CLUSTER SETTING cluster.label = 'woo'",
				},
				TransactionalStatements: []string{
					"CREATE TABLE IF NOT EXISTS system.foo(x INT)",
				},
			},
		},
	}},
	{task: autoconfigpb.Task{
		TaskID:      345,
		Description: "test task that fails with an error",
		MinVersion:  clusterversion.TestingBinaryVersion,
		Payload: &autoconfigpb.Task_SimpleSQL{
			SimpleSQL: &autoconfigpb.SimpleSQL{
				TransactionalStatements: []string{"SELECT invalid"},
			},
		},
	}},
	{task: autoconfigpb.Task{
		TaskID:      456,
		Description: "test task that creates another system table",
		MinVersion:  clusterversion.TestingBinaryVersion,
		Payload: &autoconfigpb.Task_SimpleSQL{
			SimpleSQL: &autoconfigpb.SimpleSQL{
				UsernameProto:           username.NodeUserName().EncodeProto(),
				TransactionalStatements: []string{"CREATE TABLE IF NOT EXISTS system.bar(y INT)"},
			},
		},
	}},
	{task: endProfileTask},
}

func (p *testProvider) EnvUpdate() <-chan struct{} {
	p.t.Logf("runner has registered env update channel")
	return p.notifyCh
}

func (p *testProvider) ActiveEnvironments() []autoconfigpb.EnvironmentID {
	p.Lock()
	defer p.Unlock()
	if len(p.tasks) > 0 {
		return []autoconfigpb.EnvironmentID{testEnvID}
	}
	return nil
}

func (p *testProvider) Pop(
	_ context.Context, envID autoconfigpb.EnvironmentID, taskID autoconfigpb.TaskID,
) error {
	p.Lock()
	defer p.Unlock()

	p.t.Logf("runner reports completed task %d (env %q)", taskID, envID)
	for len(p.tasks) > 0 {
		if taskID >= p.tasks[0].task.TaskID {
			p.t.Logf("popping task %d from queue", p.tasks[0].task.TaskID)
			p.tasks = p.tasks[1:]
			continue
		}
		break
	}
	return nil
}

func (p *testProvider) head() (bool, *testTask) {
	p.Lock()
	defer p.Unlock()

	if len(p.tasks) == 0 {
		return false, nil
	}
	return true, &p.tasks[0]
}

func (p *testProvider) Peek(
	ctx context.Context, envID autoconfigpb.EnvironmentID,
) (autoconfigpb.Task, error) {
	p.t.Logf("runner peeking (env %q)", envID)
	hasTask, tt := p.head()
	if !hasTask {
		return autoconfigpb.Task{}, acprovider.ErrNoMoreTasks
	}
	if !tt.seen {
		// seen ensures that the runner job won't have to wait a second
		// time when peeking the task.
		select {
		case <-ctx.Done():
			return autoconfigpb.Task{}, ctx.Err()
		case <-p.peekWaitCh:
		}
	}

	p.Lock()
	defer p.Unlock()
	tt.seen = true
	return tt.task, nil
}

func TestAutoConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	provider := &testProvider{
		t:          t,
		notifyCh:   make(chan struct{}, 1),
		peekWaitCh: make(chan struct{}),
		tasks:      testTasks,
	}
	provider.notifyCh <- struct{}{}

	ctx := context.Background()
	noWait := time.Duration(0)
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109462),

		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			AutoConfig: &autoconfig.TestingKnobs{
				Provider: provider,
			},
			Server: &server.TestingKnobs{
				AutoConfigProfileStartupWaitTime: &noWait,
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

	waitForTaskCompleted := func(taskID autoconfigpb.TaskID) (result []byte) {
		taskRef := autoconfig.InfoKeyTaskRef{Environment: testEnvID, Task: taskID}
		completionMarker := taskRef.EncodeCompletionMarkerKey()
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

	provider.peekWaitCh <- struct{}{}
	t.Logf("waiting for first task completion marker...")
	result := waitForTaskCompleted(testTasks[0].task.TaskID)
	require.Equal(t, []byte("task success"), result)

	t.Logf("check that the effects of the first task are visible")
	var unused int
	err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM system.foo`).Scan(&unused)
	require.NoError(t, err)

	provider.peekWaitCh <- struct{}{}
	t.Logf("waiting for 2nd task completion marker...")
	result = waitForTaskCompleted(testTasks[1].task.TaskID)
	require.Equal(t, []byte("task error"), result)

	provider.peekWaitCh <- struct{}{}
	t.Logf("waiting for 3rd task completion marker...")
	result = waitForTaskCompleted(testTasks[2].task.TaskID)
	require.Equal(t, []byte("task success"), result)

	t.Logf("check that the effects of the first tasks are visible")
	err = sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM system.bar`).Scan(&unused)
	require.NoError(t, err)
}

func TestAutoConfigWaitAtStartupTimesOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	provider := &testProvider{
		t:          t,
		notifyCh:   make(chan struct{}, 1),
		peekWaitCh: make(chan struct{}),
		tasks: []testTask{
			{task: autoconfigpb.Task{
				TaskID:      123,
				Description: "test task that takes longer than the maxWait",
				MinVersion:  clusterversion.TestingBinaryVersion,
				Payload: &autoconfigpb.Task_SimpleSQL{
					SimpleSQL: &autoconfigpb.SimpleSQL{
						UsernameProto: username.NodeUserName().EncodeProto(),
						TransactionalStatements: []string{
							"SELECT pg_sleep(300)",
						},
					},
				},
			}},
			{task: endProfileTask},
		},
	}
	provider.notifyCh <- struct{}{}
	close(provider.peekWaitCh)

	ctx := context.Background()
	shortWait := 50 * time.Millisecond
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109462),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			AutoConfig: &autoconfig.TestingKnobs{
				Provider: provider,
			},
			Server: &server.TestingKnobs{
				AutoConfigProfileStartupWaitTime: &shortWait,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// To keep us honest, assert that the task hasn't been
	// completed even though server startup has returned.
	taskRef := autoconfig.InfoKeyTaskRef{Environment: testEnvID, Task: provider.tasks[0].task.TaskID}
	completionMarker := taskRef.EncodeCompletionMarkerKey()
	var result bool
	err := sqlDB.QueryRowContext(ctx, `SELECT true FROM system.job_info WHERE job_id = $1 AND info_key = $2 LIMIT 1`,
		jobs.AutoConfigRunnerJobID,
		completionMarker).Scan(&result)
	require.Error(t, err, gosql.ErrNoRows)
}

func TestAutoConfigWaitAtStartup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	provider := &testProvider{
		t:          t,
		notifyCh:   make(chan struct{}, 1),
		peekWaitCh: make(chan struct{}),
		tasks: []testTask{
			{task: autoconfigpb.Task{
				TaskID:      123,
				Description: "create-test-table",
				MinVersion:  clusterversion.TestingBinaryVersion,
				Payload: &autoconfigpb.Task_SimpleSQL{
					SimpleSQL: &autoconfigpb.SimpleSQL{
						UsernameProto: username.NodeUserName().EncodeProto(),
						TransactionalStatements: []string{
							"SELECT pg_sleep(1)",
							"CREATE TABLE system.t (pk int primary key)",
							"INSERT INTO system.t VALUES (1)",
						},
					},
				},
			}},
			{task: endProfileTask},
		},
	}
	provider.notifyCh <- struct{}{}
	close(provider.peekWaitCh)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109462),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			AutoConfig: &autoconfig.TestingKnobs{
				Provider: provider,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// The side-effects of the profile execution should be
	// immediately visible.
	var value int
	err := sqlDB.QueryRow("SELECT pk FROM system.t").Scan(&value)
	require.NoError(t, err)
	require.Equal(t, 1, value)
}
