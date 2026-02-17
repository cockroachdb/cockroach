// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

var (
	taskRowColumns = []string{
		"id",
		"type",
		"state",
		"consumer_id",
		"creation_datetime",
		"update_datetime",
		"payload",
		"error",
		"reference",
		"concurrency_key",
	}
	insertTaskQuery          = regexp.MustCompile(`INSERT INTO tasks\s+\(\s*id,\s*type,\s*state,\s*consumer_id,\s*creation_datetime,\s*update_datetime,\s*payload,\s*error,\s*reference,\s*concurrency_key\s*\)`)
	selectTaskByIDQuery      = regexp.MustCompile(regexp.QuoteMeta("SELECT id, type, state, consumer_id, creation_datetime, update_datetime, payload, error, reference, concurrency_key FROM tasks WHERE id = $1"))
	updateStateQuery         = regexp.MustCompile(regexp.QuoteMeta("UPDATE tasks SET state = $1, update_datetime = $2 WHERE id = $3"))
	updateErrorQuery         = regexp.MustCompile(regexp.QuoteMeta("UPDATE tasks SET error = $1, update_datetime = $2 WHERE id = $3"))
	claimNextTaskQuery       = regexp.MustCompile(`(?s)UPDATE tasks\s+SET state = \$1, consumer_id = \$2, update_datetime = \$3.*RETURNING id, type, state, consumer_id, creation_datetime, update_datetime, payload, error, reference, concurrency_key`)
	purgeTasksQuery          = regexp.MustCompile(regexp.QuoteMeta("DELETE FROM tasks WHERE state = $1 AND update_datetime < $2"))
	statsQuery               = regexp.MustCompile(regexp.QuoteMeta("SELECT state, type, count(*) FROM tasks GROUP BY state, type ORDER BY state, type"))
	cleanupStaleQuery        = regexp.MustCompile(`(?s)UPDATE tasks\s+SET state = \$1, consumer_id = NULL, update_datetime = \$2.*LIMIT \$5`)
	mostRecentCompletedQuery = regexp.MustCompile(`(?s)SELECT id, type, state, consumer_id, creation_datetime, update_datetime, payload, error, reference, concurrency_key\s+FROM tasks\s+WHERE type = \$1 AND state = \$2\s+ORDER BY update_datetime DESC\s+LIMIT 1`)
)

func newMockedTasksRepo(t *testing.T, opts ...Options) (*CRDBTasksRepo, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	var options Options
	if len(opts) > 0 {
		options = opts[0]
	}

	repo := NewTasksRepository(db, options)

	t.Cleanup(func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	return repo, mock
}

func TestCreateTask_InsertsRowUsingNullPayload(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	task := &tasks.Task{
		ID:    uuid.MakeV4(),
		Type:  "cluster-sync",
		State: tasks.TaskStatePending,
	}

	mock.ExpectExec(insertTaskQuery.String()).
		WithArgs(
			task.GetID().String(),
			task.GetType(),
			string(task.GetState()),
			nil,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			nil,
			nil,
			nil,
			nil,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := repo.CreateTask(context.Background(), logger.DefaultLogger, task)
	require.NoError(t, err)
}

func TestGetTask_ReturnsTask(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	taskID := uuid.MakeV4()
	now := timeutil.Now()
	rows := sqlmock.NewRows(taskRowColumns).AddRow(
		taskID.String(),
		"cluster-sync",
		string(tasks.TaskStatePending),
		nil,
		now,
		now,
		[]byte(`{"key":"value"}`),
		nil,
		nil,
		nil,
	)

	mock.ExpectQuery(selectTaskByIDQuery.String()).
		WithArgs(taskID.String()).
		WillReturnRows(rows)

	task, err := repo.GetTask(context.Background(), logger.DefaultLogger, taskID)
	require.NoError(t, err)
	require.Equal(t, taskID, task.GetID())
	require.Equal(t, tasks.TaskStatePending, task.GetState())
}

func TestGetTask_ReturnsErrWhenNotFound(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	taskID := uuid.MakeV4()
	mock.ExpectQuery(selectTaskByIDQuery.String()).
		WithArgs(taskID.String()).
		WillReturnError(gosql.ErrNoRows)

	_, err := repo.GetTask(context.Background(), logger.DefaultLogger, taskID)
	require.ErrorIs(t, err, rtasks.ErrTaskNotFound)
}

func TestUpdateState_ReturnsErrWhenNoRowsAffected(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	taskID := uuid.MakeV4()
	mock.ExpectExec(updateStateQuery.String()).
		WithArgs(string(tasks.TaskStateRunning), sqlmock.AnyArg(), taskID.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := repo.UpdateState(context.Background(), logger.DefaultLogger, taskID, tasks.TaskStateRunning)
	require.ErrorIs(t, err, rtasks.ErrTaskNotFound)
}

func TestUpdateState_Succeeds(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	taskID := uuid.MakeV4()
	mock.ExpectExec(updateStateQuery.String()).
		WithArgs(string(tasks.TaskStateRunning), sqlmock.AnyArg(), taskID.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := repo.UpdateState(context.Background(), logger.DefaultLogger, taskID, tasks.TaskStateRunning)
	require.NoError(t, err)
}

func TestUpdateError_Succeeds(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	taskID := uuid.MakeV4()
	mock.ExpectExec(updateErrorQuery.String()).
		WithArgs("boom", sqlmock.AnyArg(), taskID.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := repo.UpdateError(context.Background(), logger.DefaultLogger, taskID, "boom")
	require.NoError(t, err)
}

func TestUpdateError_ReturnsErrWhenTaskMissing(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	taskID := uuid.MakeV4()
	mock.ExpectExec(updateErrorQuery.String()).
		WithArgs("boom", sqlmock.AnyArg(), taskID.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := repo.UpdateError(context.Background(), logger.DefaultLogger, taskID, "boom")
	require.ErrorIs(t, err, rtasks.ErrTaskNotFound)
}

func TestGetStatistics_ReturnsCounts(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	rows := sqlmock.NewRows([]string{"state", "type", "count"}).
		AddRow(string(tasks.TaskStatePending), "cluster-sync", 2).
		AddRow(string(tasks.TaskStateRunning), "cluster-sync", 1)

	mock.ExpectQuery(statsQuery.String()).WillReturnRows(rows)

	stats, err := repo.GetStatistics(context.Background(), logger.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 2, stats[tasks.TaskStatePending]["cluster-sync"])
	require.Equal(t, 1, stats[tasks.TaskStateRunning]["cluster-sync"])
}

func TestPurgeTasks_ReturnsDeletedCount(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	mock.ExpectExec(purgeTasksQuery.String()).
		WithArgs(string(tasks.TaskStateDone), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 3))

	deleted, err := repo.PurgeTasks(context.Background(), logger.DefaultLogger, time.Hour, tasks.TaskStateDone)
	require.NoError(t, err)
	require.Equal(t, 3, deleted)
}

func TestClaimNextTask_ReturnsTask(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	taskID := uuid.MakeV4()
	now := timeutil.Now()
	rows := sqlmock.NewRows(taskRowColumns).AddRow(
		taskID.String(),
		"cluster-sync",
		string(tasks.TaskStateRunning),
		"instance-a",
		now,
		now,
		nil,
		nil,
		nil,
		nil,
	)

	instanceID := "instance-a"
	mock.ExpectQuery(claimNextTaskQuery.String()).
		WithArgs(
			string(tasks.TaskStateRunning),
			instanceID,
			sqlmock.AnyArg(),
			string(tasks.TaskStatePending),
			string(tasks.TaskStateRunning),
		).
		WillReturnRows(rows)

	task, found, err := repo.claimNextTask(context.Background(), logger.DefaultLogger, instanceID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taskID, task.GetID())
	require.Equal(t, tasks.TaskStateRunning, task.GetState())
}

func TestClaimNextTask_ReturnsFalseWhenNoPendingTasks(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	instanceID := "instance-a"
	mock.ExpectQuery(claimNextTaskQuery.String()).
		WithArgs(
			string(tasks.TaskStateRunning),
			instanceID,
			sqlmock.AnyArg(),
			string(tasks.TaskStatePending),
			string(tasks.TaskStateRunning),
		).
		WillReturnError(gosql.ErrNoRows)

	task, found, err := repo.claimNextTask(context.Background(), logger.DefaultLogger, instanceID)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, task)
}

func TestClaimNextTask_ReturnsFalseOnConcurrencyKeyRunningConflict(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	instanceID := "instance-a"
	mock.ExpectQuery(claimNextTaskQuery.String()).
		WithArgs(
			string(tasks.TaskStateRunning),
			instanceID,
			sqlmock.AnyArg(),
			string(tasks.TaskStatePending),
			string(tasks.TaskStateRunning),
		).
		WillReturnError(
			errors.New(`duplicate key value violates unique constraint "idx_tasks_concurrency_key_running_unique"`),
		)

	task, found, err := repo.claimNextTask(context.Background(), logger.DefaultLogger, instanceID)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, task)
}

func TestCleanupStaleTasks_UsesConfiguredTimeout(t *testing.T) {
	repo, mock := newMockedTasksRepo(t, Options{HealthTimeout: 30 * time.Second})

	mock.ExpectExec(cleanupStaleQuery.String()).
		WithArgs(
			string(tasks.TaskStatePending),
			sqlmock.AnyArg(),
			string(tasks.TaskStateRunning),
			"30 seconds",
			5,
		).
		WillReturnResult(sqlmock.NewResult(0, 2))

	repo.cleanupStaleTasks(context.Background(), logger.DefaultLogger, 5)
}

func TestGetMostRecentCompletedTaskOfType_ReturnsTask(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	taskID := uuid.MakeV4()
	now := timeutil.Now()
	rows := sqlmock.NewRows(taskRowColumns).AddRow(
		taskID.String(),
		"cluster-sync",
		string(tasks.TaskStateDone),
		nil,
		now,
		now,
		nil,
		nil,
		nil,
		nil,
	)

	mock.ExpectQuery(mostRecentCompletedQuery.String()).
		WithArgs("cluster-sync", string(tasks.TaskStateDone)).
		WillReturnRows(rows)

	task, err := repo.GetMostRecentCompletedTaskOfType(context.Background(), logger.DefaultLogger, "cluster-sync")
	require.NoError(t, err)
	require.NotNil(t, task)
	require.Equal(t, taskID, task.GetID())
}

func TestGetMostRecentCompletedTaskOfType_ReturnsNilWhenMissing(t *testing.T) {
	repo, mock := newMockedTasksRepo(t)

	mock.ExpectQuery(mostRecentCompletedQuery.String()).
		WithArgs("cluster-sync", string(tasks.TaskStateDone)).
		WillReturnError(gosql.ErrNoRows)

	task, err := repo.GetMostRecentCompletedTaskOfType(context.Background(), logger.DefaultLogger, "cluster-sync")
	require.NoError(t, err)
	require.Nil(t, task)
}
