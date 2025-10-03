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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings"
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
	Type             bindings.FilterValue[string]          `stripe:"type" validate:"omitempty,max=50"`
	State            bindings.FilterValue[tasks.TaskState] `stripe:"state" validate:"omitempty,oneof=pending running done failed"`
	CreationDatetime bindings.FilterValue[time.Time]       `stripe:"creation_datetime" validate:"omitempty"`
	UpdateDatetime   bindings.FilterValue[time.Time]       `stripe:"update_datetime" validate:"omitempty"`
}

// ToFilterSet converts the InputGetAllDTO to a filters.FilterSet for use by the service layer
func (dto *InputGetAllDTO) ToFilterSet() filtertypes.FilterSet {
	return bindings.ToFilterSet(*dto)
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
type TasksResult struct {
	TasksResultError
	Data []tasks.ITask `json:"data,omitempty"`
}

// GetData returns the data from the TasksResult
func (dto *TasksResult) GetData() any {
	return dto.Data
}

// FromService converts the return value of the tasks service to a TaskResult.
func (dto *TasksResult) FromService(tasks []tasks.ITask, err error) *TasksResult {
	dto.Data = tasks
	dto.Error = err
	return dto
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

// FromService converts the return values of the task service to a TaskResult.
func (dto *TaskResult) FromService(task tasks.ITask, err error) *TaskResult {
	dto.Data = task
	dto.Error = err
	return dto
}
