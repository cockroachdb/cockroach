// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
)

const (
	// ControllerPath is the path for the public DNS controller.
	ControllerPath = "/v1/public-dns"
)

var (
	ErrWrongClusterName = fmt.Errorf("name in URL does not match name in body")
)

type PublicDNSResultError struct {
	Error error
}

// GetError returns the error from the PublicDNSResultError.
func (dto *PublicDNSResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error responses.
func (dto *PublicDNSResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode returns the status code associated
// with the PublicDNSResultError. This handles public DNS-specific error mappings.
func (dto *PublicDNSResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	// No public DNS-specific error mappings
	default:
		// Fall back to generic error handling
		return controllers.GetGenericStatusCode(err)
	}
}

// TaskResult is the output data transfer object for the public DNS controller
// that handles tasks.
type TaskResult struct {
	PublicDNSResultError
	Data tasks.ITask `json:"data,omitempty"`
}

// GetData returns the data from the TaskResult
func (dto *TaskResult) GetData() any {
	return dto.Data
}

// NewTaskResult creates a new TaskResult.
func NewTaskResult(task tasks.ITask, err error) *TaskResult {
	return &TaskResult{
		PublicDNSResultError: PublicDNSResultError{Error: err},
		Data:                 task,
	}
}
