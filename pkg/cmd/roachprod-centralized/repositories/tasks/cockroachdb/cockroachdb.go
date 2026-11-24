// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// PollingInterval is the fixed interval for polling tasks
	PollingInterval = 500 * time.Millisecond
)

// CRDBTasksRepo is a CockroachDB implementation of the tasks repository.
type CRDBTasksRepo struct {
	db *gosql.DB

	// Health timeout for detecting dead workers
	healthTimeout time.Duration
}

// Options for configuring the CockroachDB tasks repository.
type Options struct {
	// HealthTimeout is how long an instance can be unhealthy before being considered dead (default: 3s)
	HealthTimeout time.Duration
}

// NewTasksRepository creates a new CockroachDB tasks repository.
func NewTasksRepository(db *gosql.DB, opts Options) *CRDBTasksRepo {
	if opts.HealthTimeout == 0 {
		opts.HealthTimeout = 3 * time.Second
	}

	return &CRDBTasksRepo{
		db:            db,
		healthTimeout: opts.HealthTimeout,
	}
}

// GetTasks returns tasks based on the provided filters with total count for pagination.
func (r *CRDBTasksRepo) GetTasks(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]tasks.ITask, int, error) {

	// Build both SELECT and COUNT queries
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(tasks.Task{}),
	)
	baseSelectQuery := "SELECT id, type, state, consumer_id, creation_datetime, update_datetime, payload, error FROM tasks"
	baseCountQuery := "SELECT count(*) FROM tasks"

	// Default sort: creation_datetime DESC
	defaultSort := &filtertypes.SortParams{
		SortBy:    "CreationDatetime",
		SortOrder: filtertypes.SortDescending,
	}

	selectQuery, countQuery, args, err := qb.BuildWithCount(baseSelectQuery, baseCountQuery, &filterSet, defaultSort)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to build queries")
	}

	l.Debug("querying tasks from database",
		slog.String("select_query", selectQuery),
		slog.String("count_query", countQuery),
		slog.Int("args_count", len(args)),
		slog.Any("args", args),
	)

	// Execute COUNT query
	var totalCount int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, errors.Wrap(err, "failed to count tasks")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to query tasks")
	}
	defer rows.Close()

	// Scan results
	var result []tasks.ITask
	for rows.Next() {
		task, err := r.scanTask(rows)
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to scan task")
		}
		result = append(result, task)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "error iterating rows")
	}

	return result, totalCount, nil
}

// GetTask returns a single task by ID.
func (r *CRDBTasksRepo) GetTask(
	ctx context.Context, l *logger.Logger, taskID uuid.UUID,
) (tasks.ITask, error) {
	query := "SELECT id, type, state, consumer_id, creation_datetime, update_datetime, payload, error FROM tasks WHERE id = $1"

	row := r.db.QueryRowContext(ctx, query, taskID.String())
	task, err := r.scanTask(row)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, rtasks.ErrTaskNotFound
		}
		return nil, errors.Wrap(err, "failed to get task")
	}

	return task, nil
}

// CreateTask creates a new task in the database.
func (r *CRDBTasksRepo) CreateTask(ctx context.Context, l *logger.Logger, task tasks.ITask) error {
	query := `
		INSERT INTO tasks (id, type, state, consumer_id, creation_datetime, update_datetime, payload, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	now := timeutil.Now()
	task.SetCreationDatetime(now)
	task.SetUpdateDatetime(now)

	var consumerID *string
	// Note: consumer_id is NULL for pending tasks, only set when claimed

	// Handle payload - use interface{} with nil for tasks without options
	// to ensure PostgreSQL receives NULL instead of empty string
	var payload interface{}
	if p := task.GetPayload(); len(p) > 0 {
		payload = p
	} else {
		payload = nil // Explicitly nil, not []byte(nil)
	}

	_, err := r.db.ExecContext(ctx, query,
		task.GetID().String(),
		task.GetType(),
		string(task.GetState()),
		consumerID, // NULL for new tasks
		task.GetCreationDatetime(),
		task.GetUpdateDatetime(),
		payload, // NULL for tasks without options, JSONB bytes otherwise
		nil,     // Error is NULL for new tasks
	)

	if err != nil {
		return errors.Wrap(err, "failed to create task")
	}

	return nil
}

// UpdateState updates the state of a task.
func (r *CRDBTasksRepo) UpdateState(
	ctx context.Context, l *logger.Logger, taskID uuid.UUID, state tasks.TaskState,
) error {
	query := "UPDATE tasks SET state = $1, update_datetime = $2 WHERE id = $3"

	result, err := r.db.ExecContext(ctx, query, string(state), timeutil.Now(), taskID.String())
	if err != nil {
		return errors.Wrap(err, "failed to update task state")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	}

	if rowsAffected == 0 {
		return rtasks.ErrTaskNotFound
	}

	return nil
}

// UpdateError sets the error message for a task.
func (r *CRDBTasksRepo) UpdateError(
	ctx context.Context, l *logger.Logger, taskID uuid.UUID, errorMsg string,
) error {
	query := "UPDATE tasks SET error = $1, update_datetime = $2 WHERE id = $3"

	result, err := r.db.ExecContext(ctx, query, errorMsg, timeutil.Now(), taskID.String())
	if err != nil {
		return errors.Wrap(err, "failed to update task error")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	}

	if rowsAffected == 0 {
		return rtasks.ErrTaskNotFound
	}

	return nil
}

// GetStatistics returns task statistics grouped by state.
func (r *CRDBTasksRepo) GetStatistics(
	ctx context.Context, l *logger.Logger,
) (rtasks.Statistics, error) {
	query := "SELECT state, type, count(*) FROM tasks GROUP BY state, type ORDER BY state, type"

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get statistics")
	}
	defer rows.Close()

	stats := make(rtasks.Statistics)
	for rows.Next() {
		var state string
		var taskType string
		var count int
		if err := rows.Scan(&state, &taskType, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan statistics")
		}

		// Initialize nested map if needed
		taskState := tasks.TaskState(state)
		if stats[taskState] == nil {
			stats[taskState] = make(map[string]int)
		}

		stats[taskState][taskType] = count
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating statistics rows")
	}

	return stats, nil
}

// PurgeTasks deletes tasks that are in the specified state and older than the given duration.
func (r *CRDBTasksRepo) PurgeTasks(
	ctx context.Context, l *logger.Logger, olderThan time.Duration, state tasks.TaskState,
) (int, error) {
	query := "DELETE FROM tasks WHERE state = $1 AND update_datetime < $2"
	cutoff := timeutil.Now().Add(-olderThan)

	result, err := r.db.ExecContext(ctx, query, string(state), cutoff)
	if err != nil {
		return 0, errors.Wrap(err, "failed to purge tasks")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "failed to get purged count")
	}

	return int(rowsAffected), nil
}

// GetTasksForProcessing implements distributed task processing with fixed interval polling.
func (r *CRDBTasksRepo) GetTasksForProcessing(
	ctx context.Context, l *logger.Logger, taskChan chan<- tasks.ITask, instanceID string,
) error {
	ticker := time.NewTicker(PollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Clean up stale tasks (distributed cleanup)
			r.cleanupStaleTasks(ctx, l, 2)

			// Try to claim a task
			task, found, err := r.claimNextTask(ctx, l, instanceID)
			if err != nil {
				return errors.Wrap(err, "failed to claim task")
			}

			if found {
				taskChan <- task
			}
		}
	}
}

// claimNextTask atomically claims the next pending task.
func (r *CRDBTasksRepo) claimNextTask(
	ctx context.Context, l *logger.Logger, instanceID string,
) (tasks.ITask, bool, error) {
	query := `
		UPDATE tasks
		SET state = $1, consumer_id = $2, update_datetime = $3
		WHERE id = (
			SELECT id FROM tasks
			WHERE state = $4
			ORDER BY creation_datetime
			LIMIT 1
		)
		RETURNING id, type, state, consumer_id, creation_datetime, update_datetime, payload, error
	`

	now := timeutil.Now()
	row := r.db.QueryRowContext(ctx, query,
		string(tasks.TaskStateRunning),
		instanceID,
		now,
		string(tasks.TaskStatePending),
	)

	task, err := r.scanTask(row)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, false, nil // No tasks available
		}
		return nil, false, errors.Wrap(err, "failed to claim task")
	}

	return task, true, nil
}

// cleanupStaleTasks removes tasks whose consumer (worker) is no longer healthy.
// This detects dead workers by checking their heartbeat in the instance_health table.
func (r *CRDBTasksRepo) cleanupStaleTasks(ctx context.Context, l *logger.Logger, limit int) {

	query := `
		UPDATE tasks
		SET state = $1, consumer_id = NULL, update_datetime = $2
		WHERE id IN (
			SELECT id FROM tasks
			WHERE state = $3
			AND consumer_id IS NOT NULL
			AND NOT EXISTS (
				SELECT 1
				FROM instance_health
				WHERE instance_id = tasks.consumer_id
				AND last_heartbeat > now() - $4::INTERVAL
			)
			LIMIT $5
		)
	`

	result, err := r.db.ExecContext(ctx, query,
		string(tasks.TaskStatePending),
		timeutil.Now(),
		string(tasks.TaskStateRunning),
		fmt.Sprintf("%.0f seconds", r.healthTimeout.Seconds()),
		limit,
	)
	if err != nil {
		l.Warn("failed to cleanup stale tasks",
			slog.String("operation", "cleanupStaleTasks"),
			slog.Int("limit", limit),
			slog.Any("error", err))
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err == nil && rowsAffected > 0 {
		l.Debug("cleaned up stale tasks from dead workers",
			slog.String("operation", "cleanupStaleTasks"),
			slog.Int64("cleaned", rowsAffected),
		)
	}
}

// GetMostRecentCompletedTaskOfType returns the most recently completed task of the given type.
func (r *CRDBTasksRepo) GetMostRecentCompletedTaskOfType(
	ctx context.Context, l *logger.Logger, taskType string,
) (tasks.ITask, error) {
	query := `
		SELECT id, type, state, consumer_id, creation_datetime, update_datetime, payload, error
		FROM tasks
		WHERE type = $1 AND state = $2
		ORDER BY update_datetime DESC
		LIMIT 1
	`

	row := r.db.QueryRowContext(ctx, query, taskType, string(tasks.TaskStateDone))
	task, err := r.scanTask(row)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, nil // No completed task of this type found
		}
		return nil, errors.Wrap(err, "failed to get most recent completed task")
	}

	return task, nil
}

// scanTask scans a database row into a Task struct.
// Always returns a base tasks.Task instance - service layer is responsible for
// upgrading to concrete types when needed.
func (r *CRDBTasksRepo) scanTask(
	scanner interface {
		Scan(dest ...interface{}) error
	},
) (tasks.ITask, error) {
	var id, taskType, state string
	var consumerID *string
	var creationTime, updateTime time.Time
	var payload []byte
	var taskError gosql.NullString

	err := scanner.Scan(&id, &taskType, &state, &consumerID, &creationTime, &updateTime, &payload, &taskError)
	if err != nil {
		return nil, err
	}

	taskID, err := uuid.FromString(id)
	if err != nil {
		return nil, errors.Wrap(err, "invalid task ID")
	}

	// Create base task instance
	task := &tasks.Task{
		ID:               taskID,
		Type:             taskType,
		State:            tasks.TaskState(state),
		Payload:          payload,
		CreationDatetime: creationTime,
		UpdateDatetime:   updateTime,
	}
	if taskError.Valid {
		task.Error = taskError.String
	}

	return task, nil
}
