// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	"github.com/cockroachdb/errors"
)

// InputGetAllDTO is input data transfer object that handles requests parameters
// for the tasks.GetAll() controller.
type InputGetAllDTO struct {
	Type  string `form:"type" binding:"omitempty,alphanum"`
	State string `form:"state" binding:"omitempty,oneof=pending running done failed"`
}

// ToServiceInputGetAllDTO converts the InputGetAllDTO data transfer object
// to a tasks' service InputGetAllDTO.
func (dto *InputGetAllDTO) ToServiceInputGetAllDTO() stasks.InputGetAllTasksDTO {
	return stasks.InputGetAllTasksDTO{
		Type:  dto.Type,
		State: tasks.TaskState(dto.State),
	}
}

type TasksResultError struct {
	Error error `json:"error"`
}

// GetError returns the error from the TasksErrorDTO.
func (dto *TasksResultError) GetError() error {
	return dto.Error
}

// GetAssociatedStatusCode returns the status code associated
// with the TasksResultError.
func (dto *TasksResultError) GetAssociatedStatusCode() int {

	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, stasks.ErrTaskNotFound):
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
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
