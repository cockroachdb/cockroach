// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
)

const (
	// ControllerPath is the path for the tasks controller.
	ControllerPath = "/v1/tasks"
)

// InputGetAllDTO is input data transfer object that handles Stripe-style query parameters
// for the tasks.GetAll() controller.
// Examples: ?type=test&state[in]=pending,running
type InputGetAllDTO struct {
	Type             stripe.FilterValue[string]          `stripe:"type" validate:"omitempty,max=50"`
	State            stripe.FilterValue[tasks.TaskState] `stripe:"state" validate:"omitempty,oneof=pending running done failed"`
	CreationDatetime stripe.FilterValue[time.Time]       `stripe:"creation_datetime" validate:"omitempty"`
	UpdateDatetime   stripe.FilterValue[time.Time]       `stripe:"update_datetime" validate:"omitempty"`
}

// ToFilterSet converts the InputGetAllDTO to a filters.FilterSet for use by the service layer
func (dto *InputGetAllDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// ToServiceInputGetAllDTO converts the InputGetAllDTO to the service layer DTO
func (dto *InputGetAllDTO) ToServiceInputGetAllDTO() stasks.InputGetAllTasksDTO {
	return stasks.InputGetAllTasksDTO{
		Filters: dto.ToFilterSet(),
	}
}

// TasksResultError encapsulates error responses from tasks controller operations.
// It implements the IResultDTO interface to provide consistent error handling across task endpoints.
type TasksResultError struct {
	Error error
}

// GetError returns the error from the TasksResultError.
func (dto *TasksResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error responses.
func (dto *TasksResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode returns the status code associated
// with the TasksResultError. This handles tasks-specific error mappings.
func (dto *TasksResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	// Tasks-specific error mappings
	case errors.Is(err, stasks.ErrTaskNotFound):
		return http.StatusNotFound
	default:
		// Fall back to generic error handling
		return controllers.GetGenericStatusCode(err)
	}
}

// TasksResult is the output data transfer object for the tasks controller.
// It embeds PaginationMetadata to automatically implement IPaginatedResult.
type TasksResult struct {
	TasksResultError
	controllers.PaginationMetadata
	Data []tasks.ITask `json:"data,omitempty"`
}

// GetData returns the data from the TasksResult
func (dto *TasksResult) GetData() any {
	return dto.Data
}

// NewTasksListResult creates a new TasksResult with pagination metadata.
// The pagination parameter is optional - if nil, startIndex defaults to 1.
func NewTasksListResult(
	tasksList []tasks.ITask, totalCount int, pagination *filtertypes.PaginationParams, err error,
) *TasksResult {
	result := &TasksResult{}
	result.TasksResultError.Error = err

	if err != nil {
		return result
	}

	result.Data = tasksList
	result.TotalCount = totalCount
	result.Count = len(tasksList)
	result.StartIndex = 1
	if pagination != nil {
		result.StartIndex = pagination.StartIndex
	}

	return result
}

// TaskResult is the output data transfer object for the tasks controller.
type TaskResult struct {
	TasksResultError
	Data tasks.ITask `json:"data,omitempty"`
}

// GetData returns the data from the TaskResult
func (dto *TaskResult) GetData() any {
	return dto.Data
}

// NewTaskResult creates a new TaskResult.
func NewTaskResult(task tasks.ITask, err error) *TaskResult {
	return &TaskResult{
		TasksResultError: TasksResultError{Error: err},
		Data:             task,
	}
}
