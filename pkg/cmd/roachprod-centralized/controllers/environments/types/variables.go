// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
)

// InputCreateVariableDTO is the controller-level DTO for creating a variable.
type InputCreateVariableDTO struct {
	Key   string                       `json:"key" binding:"required"`
	Value string                       `json:"value"`
	Type  envmodels.EnvironmentVarType `json:"type"`
}

// ToServiceDTO converts the controller DTO to the service-layer DTO.
func (dto *InputCreateVariableDTO) ToServiceDTO() envtypes.InputCreateVariableDTO {
	return envtypes.InputCreateVariableDTO{
		Key:   dto.Key,
		Value: dto.Value,
		Type:  dto.Type,
	}
}

// InputUpdateVariableDTO is the controller-level DTO for updating a variable.
type InputUpdateVariableDTO struct {
	Value string                       `json:"value"`
	Type  envmodels.EnvironmentVarType `json:"type"`
}

// ToServiceDTO converts the controller DTO to the service-layer DTO.
func (dto *InputUpdateVariableDTO) ToServiceDTO() envtypes.InputUpdateVariableDTO {
	return envtypes.InputUpdateVariableDTO{
		Value: dto.Value,
		Type:  dto.Type,
	}
}

// VariableResult wraps a single variable response.
type VariableResult struct {
	EnvironmentsResultError
	Data *envmodels.EnvironmentVariable `json:"data,omitempty"`
}

// GetData returns the variable data.
func (dto *VariableResult) GetData() any {
	return dto.Data
}

// NewVariableResult creates a new VariableResult.
func NewVariableResult(data *envmodels.EnvironmentVariable, err error) *VariableResult {
	return &VariableResult{
		EnvironmentsResultError: EnvironmentsResultError{Error: err},
		Data:                    data,
	}
}

// VariablesResult wraps a list of variables.
type VariablesResult struct {
	EnvironmentsResultError
	Data []envmodels.EnvironmentVariable `json:"data,omitempty"`
}

// GetData returns the variables data.
func (dto *VariablesResult) GetData() any {
	return dto.Data
}

// NewVariablesResult creates a new VariablesResult.
func NewVariablesResult(data []envmodels.EnvironmentVariable, err error) *VariablesResult {
	return &VariablesResult{
		EnvironmentsResultError: EnvironmentsResultError{Error: err},
		Data:                    data,
	}
}
