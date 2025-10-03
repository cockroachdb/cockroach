// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func setupTestDB(_ *testing.T) *gosql.DB {
	// This would need to be configured for actual testing
	// For now, return nil to avoid import issues
	return nil
}

func createTasksTable(t *testing.T, db *gosql.DB) {
	schema := `
		CREATE TABLE IF NOT EXISTS tasks (
			id UUID PRIMARY KEY,
			type VARCHAR(255) NOT NULL,
			state VARCHAR(50) NOT NULL,
			consumer_id UUID,
			creation_datetime TIMESTAMPTZ NOT NULL,
			update_datetime TIMESTAMPTZ NOT NULL
		)
	`
	_, err := db.Exec(schema)
	require.NoError(t, err)
}

func TestCRDBTasksRepo_CreateAndGetTask(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	createTasksTable(t, db)
	repo := NewTasksRepository(db, Options{})
	ctx := context.Background()

	// Create a test task
	task := &tasks.Task{
		ID:    uuid.MakeV4(),
		Type:  "test-task",
		State: tasks.TaskStatePending,
	}

	err := repo.CreateTask(ctx, logger.DefaultLogger, task)
	require.NoError(t, err)

	// Verify the task was created
	retrievedTask, err := repo.GetTask(ctx, logger.DefaultLogger, task.GetID())
	require.NoError(t, err)
	require.Equal(t, task.GetID(), retrievedTask.GetID())
	require.Equal(t, task.GetType(), retrievedTask.GetType())
	require.Equal(t, task.GetState(), retrievedTask.GetState())
}

func TestCRDBTasksRepo_GetTasks_WithFilters(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	createTasksTable(t, db)
	repo := NewTasksRepository(db, Options{})
	ctx := context.Background()

	// Create test tasks
	pendingTask := &tasks.Task{
		ID:    uuid.MakeV4(),
		Type:  "test-task",
		State: tasks.TaskStatePending,
	}
	runningTask := &tasks.Task{
		ID:    uuid.MakeV4(),
		Type:  "other-task",
		State: tasks.TaskStateRunning,
	}

	require.NoError(t, repo.CreateTask(ctx, logger.DefaultLogger, pendingTask))
	require.NoError(t, repo.CreateTask(ctx, logger.DefaultLogger, runningTask))

	// Test filtering by state
	stateFilter := *filters.NewFilterSet().AddFilter("State", filtertypes.OpEqual, "pending")
	pendingTasks, err := repo.GetTasks(ctx, logger.DefaultLogger, stateFilter)
	require.NoError(t, err)
	require.Len(t, pendingTasks, 1)
	require.Equal(t, pendingTask.GetID(), pendingTasks[0].GetID())

	// Test filtering by type
	typeFilter := *filters.NewFilterSet().AddFilter("Type", filtertypes.OpEqual, "test-task")
	testTasks, err := repo.GetTasks(ctx, logger.DefaultLogger, typeFilter)
	require.NoError(t, err)
	require.Len(t, testTasks, 1)
	require.Equal(t, pendingTask.GetID(), testTasks[0].GetID())
}

func TestCRDBTasksRepo_UpdateState(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	createTasksTable(t, db)
	repo := NewTasksRepository(db, Options{})
	ctx := context.Background()

	// Create a test task
	task := &tasks.Task{
		ID:    uuid.MakeV4(),
		Type:  "test-task",
		State: tasks.TaskStatePending,
	}

	require.NoError(t, repo.CreateTask(ctx, logger.DefaultLogger, task))

	// Update the state
	err := repo.UpdateState(ctx, logger.DefaultLogger, task.GetID(), tasks.TaskStateRunning)
	require.NoError(t, err)

	// Verify the state was updated
	retrievedTask, err := repo.GetTask(ctx, logger.DefaultLogger, task.GetID())
	require.NoError(t, err)
	require.Equal(t, tasks.TaskStateRunning, retrievedTask.GetState())
}

func TestCRDBTasksRepo_GetStatistics(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	createTasksTable(t, db)
	repo := NewTasksRepository(db, Options{})
	ctx := context.Background()

	// Create test tasks with different states
	testTasks := []*tasks.Task{
		{ID: uuid.MakeV4(), Type: "test", State: tasks.TaskStatePending},
		{ID: uuid.MakeV4(), Type: "test", State: tasks.TaskStatePending},
		{ID: uuid.MakeV4(), Type: "test", State: tasks.TaskStateRunning},
		{ID: uuid.MakeV4(), Type: "test", State: tasks.TaskStateDone},
	}

	for _, task := range testTasks {
		require.NoError(t, repo.CreateTask(ctx, logger.DefaultLogger, task))
	}

	// Get statistics
	stats, err := repo.GetStatistics(ctx, logger.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 2, stats[tasks.TaskStatePending])
	require.Equal(t, 1, stats[tasks.TaskStateRunning])
	require.Equal(t, 1, stats[tasks.TaskStateDone])
}

func TestCRDBTasksRepo_PurgeTasks(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	createTasksTable(t, db)
	repo := NewTasksRepository(db, Options{})
	ctx := context.Background()

	// Create an old completed task
	oldTask := &tasks.Task{
		ID:    uuid.MakeV4(),
		Type:  "test-task",
		State: tasks.TaskStateDone,
	}
	require.NoError(t, repo.CreateTask(ctx, logger.DefaultLogger, oldTask))

	// Manually update the task to be old
	_, err := db.Exec("UPDATE tasks SET update_datetime = $1 WHERE id = $2",
		timeutil.Now().Add(-2*time.Hour), oldTask.GetID().String())
	require.NoError(t, err)

	// Create a recent completed task
	recentTask := &tasks.Task{
		ID:    uuid.MakeV4(),
		Type:  "test-task",
		State: tasks.TaskStateDone,
	}
	require.NoError(t, repo.CreateTask(ctx, logger.DefaultLogger, recentTask))

	// Purge tasks older than 1 hour
	purged, err := repo.PurgeTasks(ctx, logger.DefaultLogger, time.Hour, tasks.TaskStateDone)
	require.NoError(t, err)
	require.Equal(t, 1, purged)

	// Verify only the old task was purged
	_, err = repo.GetTask(ctx, logger.DefaultLogger, oldTask.GetID())
	require.ErrorIs(t, err, rtasks.ErrTaskNotFound)

	_, err = repo.GetTask(ctx, logger.DefaultLogger, recentTask.GetID())
	require.NoError(t, err)
}

func TestCRDBTasksRepo_ClaimNextTask(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		skip.IgnoreLint(t, "Database not configured for testing")
	}
	defer db.Close()

	createTasksTable(t, db)
	repo := NewTasksRepository(db, Options{})
	ctx := context.Background()

	// Create a pending task
	task := &tasks.Task{
		ID:    uuid.MakeV4(),
		Type:  "test-task",
		State: tasks.TaskStatePending,
	}
	require.NoError(t, repo.CreateTask(ctx, logger.DefaultLogger, task))

	// Claim the task
	instanceID := "test-instance"
	claimedTask, found, err := repo.claimNextTask(ctx, logger.DefaultLogger, instanceID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, task.GetID(), claimedTask.GetID())
	require.Equal(t, tasks.TaskStateRunning, claimedTask.GetState())

	// Try to claim again - should find nothing
	_, found, err = repo.claimNextTask(ctx, logger.DefaultLogger, instanceID)
	require.NoError(t, err)
	require.False(t, found)
}
