// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ControllerPath is the base path for the provisionings controller.
const ControllerPath = "/v1/provisionings"

// TemplatesPath is the base path for the templates sub-resource.
const TemplatesPath = ControllerPath + "/templates"

// -- Input DTOs --

// InputCreateProvisioningDTO is the controller-level DTO for creating a
// provisioning.
type InputCreateProvisioningDTO struct {
	Environment  string                 `json:"environment" binding:"required"`
	TemplateType string                 `json:"template_type" binding:"required"`
	Variables    map[string]interface{} `json:"variables"`
	Lifetime     string                 `json:"lifetime"`
}

// ToServiceDTO converts the controller DTO to the service-layer DTO.
func (dto *InputCreateProvisioningDTO) ToServiceDTO() provtypes.InputCreateDTO {
	return provtypes.InputCreateDTO{
		Environment:  dto.Environment,
		TemplateType: dto.TemplateType,
		Variables:    dto.Variables,
		Lifetime:     dto.Lifetime,
	}
}

// InputGetAllProvisioningsDTO is the controller-level DTO for listing
// provisionings with stripe filter bindings.
type InputGetAllProvisioningsDTO struct {
	State       stripe.FilterValue[string] `stripe:"state"`
	Environment stripe.FilterValue[string] `stripe:"environment"`
	Owner       stripe.FilterValue[string] `stripe:"owner"`
}

// ToFilterSet converts the stripe-bound DTO to a FilterSet.
func (dto *InputGetAllProvisioningsDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// ToServiceDTO converts the controller DTO to the service-layer DTO.
func (dto *InputGetAllProvisioningsDTO) ToServiceDTO() provtypes.InputGetAllDTO {
	return provtypes.InputGetAllDTO{
		Filters: dto.ToFilterSet(),
	}
}

// -- Result types --

// ProvisioningsResultError is the base error type for provisioning results.
type ProvisioningsResultError struct {
	Error error
}

// GetError returns the error.
func (dto *ProvisioningsResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error-only results.
func (dto *ProvisioningsResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode maps service errors to HTTP status codes.
func (dto *ProvisioningsResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, provtypes.ErrProvisioningNotFound):
		return http.StatusNotFound
	case errors.Is(err, provtypes.ErrProvisioningAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, provtypes.ErrTemplateNotFound):
		return http.StatusBadRequest
	case errors.Is(err, provtypes.ErrEnvironmentNotFound):
		return http.StatusBadRequest
	case errors.Is(err, provtypes.ErrInvalidState):
		return http.StatusConflict
	case errors.Is(err, provtypes.ErrTaskInProgress):
		return http.StatusConflict
	default:
		return controllers.GetGenericStatusCode(err)
	}
}

// ProvisioningResult wraps a single provisioning response.
type ProvisioningResult struct {
	ProvisioningsResultError
	Data   *provmodels.Provisioning `json:"data,omitempty"`
	taskID *uuid.UUID
}

// GetData returns the provisioning data.
func (dto *ProvisioningResult) GetData() any {
	return dto.Data
}

// GetTaskID returns the task ID if a task was created, nil otherwise.
func (dto *ProvisioningResult) GetTaskID() *uuid.UUID {
	return dto.taskID
}

// NewProvisioningResult creates a new ProvisioningResult.
func NewProvisioningResult(
	data *provmodels.Provisioning, taskID *uuid.UUID, err error,
) *ProvisioningResult {
	return &ProvisioningResult{
		ProvisioningsResultError: ProvisioningsResultError{Error: err},
		Data:                     data,
		taskID:                   taskID,
	}
}

// ProvisioningsResult wraps a list of provisionings.
type ProvisioningsResult struct {
	ProvisioningsResultError
	Data []provmodels.Provisioning `json:"data,omitempty"`
}

// GetData returns the provisionings data.
func (dto *ProvisioningsResult) GetData() any {
	return dto.Data
}

// NewProvisioningsResult creates a new ProvisioningsResult.
func NewProvisioningsResult(data []provmodels.Provisioning, err error) *ProvisioningsResult {
	return &ProvisioningsResult{
		ProvisioningsResultError: ProvisioningsResultError{Error: err},
		Data:                     data,
	}
}

// TemplatesResult wraps a list of templates.
type TemplatesResult struct {
	ProvisioningsResultError
	Data []provmodels.Template `json:"data,omitempty"`
}

// GetData returns the templates data.
func (dto *TemplatesResult) GetData() any {
	return dto.Data
}

// NewTemplatesResult creates a new TemplatesResult.
func NewTemplatesResult(data []provmodels.Template, err error) *TemplatesResult {
	return &TemplatesResult{
		ProvisioningsResultError: ProvisioningsResultError{Error: err},
		Data:                     data,
	}
}

// TemplateResult wraps a single template response.
type TemplateResult struct {
	ProvisioningsResultError
	Data *provmodels.Template `json:"data,omitempty"`
}

// GetData returns the template data.
func (dto *TemplateResult) GetData() any {
	return dto.Data
}

// NewTemplateResult creates a new TemplateResult.
func NewTemplateResult(data *provmodels.Template, err error) *TemplateResult {
	return &TemplateResult{
		ProvisioningsResultError: ProvisioningsResultError{Error: err},
		Data:                     data,
	}
}

// PlanResult wraps a plan output response.
type PlanResult struct {
	ProvisioningsResultError
	Data json.RawMessage `json:"data,omitempty"`
}

// GetData returns the plan data.
func (dto *PlanResult) GetData() any {
	return dto.Data
}

// NewPlanResult creates a new PlanResult.
func NewPlanResult(data json.RawMessage, err error) *PlanResult {
	return &PlanResult{
		ProvisioningsResultError: ProvisioningsResultError{Error: err},
		Data:                     data,
	}
}

// OutputsResult wraps provisioning outputs.
type OutputsResult struct {
	ProvisioningsResultError
	Data map[string]interface{} `json:"data,omitempty"`
}

// GetData returns the outputs data.
func (dto *OutputsResult) GetData() any {
	return dto.Data
}

// NewOutputsResult creates a new OutputsResult.
func NewOutputsResult(data map[string]interface{}, err error) *OutputsResult {
	return &OutputsResult{
		ProvisioningsResultError: ProvisioningsResultError{Error: err},
		Data:                     data,
	}
}

// DeleteResult wraps a delete operation response.
type DeleteResult struct {
	ProvisioningsResultError
}

// NewDeleteResult creates a new DeleteResult.
func NewDeleteResult(err error) *DeleteResult {
	return &DeleteResult{
		ProvisioningsResultError: ProvisioningsResultError{Error: err},
	}
}
