// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"context"
	"fmt"
	"time"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	TaskServiceName = "tasks"
)

const (
	// Options defaults
	// DefaultTasksTimeout is the default timeout for tasks.
	DefaultTasksTimeout = 30 * time.Second
	// DefaultTasksWorkers is the default number of workers processing tasks.
	DefaultTasksWorkers = 1
)

var (
	// ErrTaskNotFound is the error returned when a task is not found.
	ErrTaskNotFound = utils.NewPublicError(fmt.Errorf("task not found"))
	// ErrTaskTypeNotManaged is the error returned for unmanaged task types.
	ErrTaskTypeNotManaged = fmt.Errorf("task type not managed")
	// ErrUnknownTaskState is the error returned for unknown task states.
	ErrUnknownTaskType = fmt.Errorf("unknown task type")
	// ErrTaskTimeout is the error returned when a task processing times out.
	ErrTaskTimeout = fmt.Errorf("task processing timeout")
	// ErrShutdownTimeout is the error returned when the service shutdown times out.
	ErrShutdownTimeout = fmt.Errorf("service shutdown timeout")
	// ErrMetricsCollectionDisabled is returned when metrics collection is disabled
	// and a metrics-related operation is attempted.
	ErrMetricsCollectionDisabled = fmt.Errorf("metrics collection is disabled")
)

const (
	PermissionViewAll = TaskServiceName + ":view:all"
	PermissionViewOwn = TaskServiceName + ":view:own"
)

// IService defines the interface for the tasks service, which manages background task processing
// and provides CRUD operations for tasks. This service handles task lifecycle management,
// worker orchestration, and integration with other services that need to schedule work.
type IService interface {
	// GetTasks retrieves multiple tasks based on the provided filters and pagination parameters.
	// Returns tasks, total count (for pagination), and error.
	GetTasks(context.Context, *logger.Logger, *pkgauth.Principal, InputGetAllTasksDTO) ([]tasks.ITask, int, error)
	// GetTask retrieves a single task by its ID.
	GetTask(context.Context, *logger.Logger, *pkgauth.Principal, InputGetTaskDTO) (tasks.ITask, error)
	// CreateTask creates a new task and stores it in the repository for processing.
	CreateTask(context.Context, *logger.Logger, tasks.ITask) (tasks.ITask, error)
	// CreateTaskIfNotAlreadyPlanned creates a new task only if a similar task isn't already pending.
	// This prevents duplicate work from being scheduled (used for ad-hoc tasks).
	CreateTaskIfNotAlreadyPlanned(context.Context, *logger.Logger, tasks.ITask) (tasks.ITask, error)
	// CreateTaskIfNotRecentlyScheduled creates a task if no recent task of the same type exists.
	// This prevents duplicate scheduled tasks when multiple instances run periodic jobs.
	CreateTaskIfNotRecentlyScheduled(context.Context, *logger.Logger, tasks.ITask, time.Duration) (tasks.ITask, error)
	// RegisterTasksService registers a service that provides background tasks for processing.
	RegisterTasksService(ITasksService)
	// GetMostRecentCompletedTaskOfType returns the most recently completed task of the given type.
	// Returns nil if no completed task of that type exists.
	GetMostRecentCompletedTaskOfType(context.Context, *logger.Logger, string) (tasks.ITask, error)
	// WaitForTaskCompletion blocks until the task reaches a final state (done/failed) or timeout.
	WaitForTaskCompletion(context.Context, *logger.Logger, uuid.UUID, time.Duration) error
}

// Options contains configuration parameters for the tasks service.
type Options struct {
	// Workers specifies the number of concurrent workers that process tasks.
	// Higher values increase throughput but consume more resources.
	Workers int

	// WorkersEnabled indicates whether task workers are running
	WorkersEnabled bool

	// CollectMetrics is a flag to enable metrics collection.
	CollectMetrics bool

	// DefaultTasksTimeout is the timeout for tasks.
	DefaultTasksTimeout time.Duration

	// PurgeDoneTaskOlderThan is the duration after which tasks in done state
	// are purged from the repository.
	PurgeDoneTaskOlderThan time.Duration
	// PurgeFailedTaskOlderThan is the duration after which tasks in failed
	// state are purged from the repository.
	PurgeFailedTaskOlderThan time.Duration
	// PurgeTasksInterval is the value for how often tasks are purged from the
	// repository.
	PurgeTasksInterval time.Duration
	// StatisticsUpdateInterval is the value for how often the tasks statistics
	// are updated.
	StatisticsUpdateInterval time.Duration
}

// InputGetAllTasksDTO is the data transfer object to get all tasks.
type InputGetAllTasksDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// NewInputGetAllTasksDTO creates a new InputGetAllTasksDTO with proper defaults.
func NewInputGetAllTasksDTO() InputGetAllTasksDTO {
	return InputGetAllTasksDTO{
		Filters: *filters.NewFilterSet(),
	}
}

// InputGetTaskDTO is the data transfer object to get a task.
type InputGetTaskDTO struct {
	ID uuid.UUID `json:"id" binding:"required"`
}

// ITasksService is the interface for a service that handles tasks.
type ITasksService interface {
	// GetTaskServiceName returns the unique name of the service.
	GetTaskServiceName() string
	// GetHandledTasks returns a map of task types to task instances
	// that this service can handle.
	GetHandledTasks() (tasks map[string]ITask)
	// CreateTaskInstance creates a new instance of a task of the given type.
	// This is used by the repository to reconstruct tasks from the database
	// with the correct concrete type and service references injected.
	CreateTaskInstance(taskType string) (tasks.ITask, error)
}

// ITask is the interface for a task that can be processed by the service.
// It embeds tasks.ITask to inherit all task data methods (GetID, GetType, etc.)
// and adds the Process method for task execution.
type ITask interface {
	tasks.ITask
	// Process executes the task and returns an error if it fails.
	// The provided context includes any executor-enforced timeout and
	// should be used for all downstream work.
	Process(context.Context, *logger.Logger) error
}

// ITaskWithTimeout is an interface for tasks that have a timeout.
type ITaskWithTimeout interface {
	// GetTimeout returns the timeout duration for the task.
	GetTimeout() time.Duration
}

// TimeoutGetter is an interface for getting a timeout value.
// Used by the task executor to get the default timeout.
type TimeoutGetter interface {
	GetTimeout() time.Duration
}
