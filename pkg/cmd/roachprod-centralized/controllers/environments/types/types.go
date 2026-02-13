// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/errors"
)

// ControllerPath is the base path for the environments controller.
const ControllerPath = "/v1/environments"

// -- Controller DTOs --

// InputCreateEnvironmentDTO is the controller-level DTO for creating an
// environment.
type InputCreateEnvironmentDTO struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
}

// ToServiceDTO converts the controller DTO to the service-layer DTO.
func (dto *InputCreateEnvironmentDTO) ToServiceDTO() envtypes.InputCreateDTO {
	return envtypes.InputCreateDTO{
		Name:        dto.Name,
		Description: dto.Description,
	}
}

// InputUpdateEnvironmentDTO is the controller-level DTO for updating an
// environment.
type InputUpdateEnvironmentDTO struct {
	Description string `json:"description"`
}

// ToServiceDTO converts the controller DTO to the service-layer DTO.
func (dto *InputUpdateEnvironmentDTO) ToServiceDTO() envtypes.InputUpdateDTO {
	return envtypes.InputUpdateDTO{
		Description: dto.Description,
	}
}

// -- Result types --

// EnvironmentsResultError is the base error type for environment results.
type EnvironmentsResultError struct {
	Error error
}

// GetError returns the error.
func (dto *EnvironmentsResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error-only results.
func (dto *EnvironmentsResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode maps service errors to HTTP status codes.
func (dto *EnvironmentsResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, envtypes.ErrEnvironmentNotFound):
		return http.StatusNotFound
	case errors.Is(err, envtypes.ErrEnvironmentAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, envtypes.ErrEnvironmentHasProvisionings):
		return http.StatusConflict
	case errors.Is(err, envtypes.ErrVariableNotFound):
		return http.StatusNotFound
	case errors.Is(err, envtypes.ErrVariableAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, envtypes.ErrSecretWriteFailed):
		return http.StatusInternalServerError
	case errors.Is(err, envtypes.ErrSecretVerifyFailed):
		return http.StatusBadRequest
	case errors.Is(err, envtypes.ErrInvalidEnvironmentName),
		errors.Is(err, envtypes.ErrInvalidVariableKey),
		errors.Is(err, envtypes.ErrInvalidVariableType):
		return http.StatusBadRequest
	default:
		return controllers.GetGenericStatusCode(err)
	}
}

// EnvironmentResult wraps a single environment response.
type EnvironmentResult struct {
	EnvironmentsResultError
	Data *envmodels.Environment `json:"data,omitempty"`
}

// GetData returns the environment data.
func (dto *EnvironmentResult) GetData() any {
	return dto.Data
}

// NewEnvironmentResult creates a new EnvironmentResult.
func NewEnvironmentResult(data *envmodels.Environment, err error) *EnvironmentResult {
	return &EnvironmentResult{
		EnvironmentsResultError: EnvironmentsResultError{Error: err},
		Data:                    data,
	}
}

// EnvironmentsResult wraps a list of environments.
type EnvironmentsResult struct {
	EnvironmentsResultError
	Data []envmodels.Environment `json:"data,omitempty"`
}

// GetData returns the environments data.
func (dto *EnvironmentsResult) GetData() any {
	return dto.Data
}

// NewEnvironmentsResult creates a new EnvironmentsResult.
func NewEnvironmentsResult(data []envmodels.Environment, err error) *EnvironmentsResult {
	return &EnvironmentsResult{
		EnvironmentsResultError: EnvironmentsResultError{Error: err},
		Data:                    data,
	}
}

// DeleteResult wraps a delete operation response (no data, just error).
type DeleteResult struct {
	EnvironmentsResultError
}

// NewDeleteResult creates a new DeleteResult.
func NewDeleteResult(err error) *DeleteResult {
	return &DeleteResult{
		EnvironmentsResultError: EnvironmentsResultError{Error: err},
	}
}
