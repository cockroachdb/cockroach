// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	clustermodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
)

const (
	// ControllerPath is the path for the clusters controller.
	ControllerPath = "/v1/clusters"
)

var (
	ErrWrongClusterName = fmt.Errorf("name in URL does not match name in body")
)

// InputGetAllDTO is input data transfer object that handles requests parameters
// for the clusters.GetAll() controller.
type InputGetAllDTO struct {
	Name stripe.FilterValue[string] `stripe:"name" validate:"omitempty,max=50,min=3"`
}

// ToFilterSet converts the InputGetAllDTO to a filters.FilterSet for use by the service layer
func (dto *InputGetAllDTO) ToFilterSet() filtertypes.FilterSet {
	return stripe.ToFilterSet(*dto)
}

// ToServiceInputGetAllDTO converts the InputGetAllDTO data transfer object
// to a clusters' service InputGetAllDTO.
func (dto *InputGetAllDTO) ToServiceInputGetAllDTO() clustermodels.InputGetAllClustersDTO {
	return clustermodels.InputGetAllClustersDTO{
		Filters: dto.ToFilterSet(),
	}
}

// InputRegisterClusterDTO is input data transfer object that handles requests
// parameters for the clusters.Register() controller.
type InputRegisterClusterDTO struct {
	cloudcluster.Cluster
}

// Validate performs security and business logic validation for InputRegisterClusterDTO
func (dto *InputRegisterClusterDTO) Validate() error {
	// Validate cluster name
	if err := controllers.ValidateInputSecurity(dto.Cluster.Name, 64); err != nil {
		return err
	}
	return nil
}

// ToServiceInputRegisterClusterDTO converts the InputRegisterClusterDTO data transfer object
// to a clusters' service InputRegisterClusterDTO.
func (dto *InputRegisterClusterDTO) ToServiceInputRegisterClusterDTO() clustermodels.InputRegisterClusterDTO {
	return clustermodels.InputRegisterClusterDTO{
		Cluster: dto.Cluster,
	}
}

// InputRegisterClusterUpdateDTO is input data transfer object that handles requests
// parameters for the clusters.RegisterUpdate() controller.
type InputRegisterClusterUpdateDTO struct {
	cloudcluster.Cluster
}

// Validate performs security and business logic validation for InputRegisterClusterUpdateDTO
func (dto *InputRegisterClusterUpdateDTO) Validate() error {
	// Validate cluster name
	if err := controllers.ValidateInputSecurity(dto.Cluster.Name, 64); err != nil {
		return err
	}
	return nil
}

// ToServiceInputCreateClusterDTO converts the InputUpdateClusterDTO data transfer object
// to a clusters' service InputCreateClusterDTO.
func (dto *InputRegisterClusterUpdateDTO) ToServiceInputRegisterClusterUpdateDTO() clustermodels.InputRegisterClusterUpdateDTO {
	return clustermodels.InputRegisterClusterUpdateDTO{
		Cluster: dto.Cluster,
	}
}

type ClustersResultError struct {
	Error error
}

// GetError returns the error from the ClustersResultError.
func (dto *ClustersResultError) GetError() error {
	return dto.Error
}

// GetData returns nil for error responses.
func (dto *ClustersResultError) GetData() any {
	return nil
}

// GetAssociatedStatusCode returns the status code associated
// with the ClustersResultError. This handles clusters-specific error mappings.
func (dto *ClustersResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	// Clusters-specific error mappings
	case errors.Is(err, clustermodels.ErrClusterNotFound):
		return http.StatusNotFound
	case errors.Is(err, clustermodels.ErrClusterAlreadyExists):
		return http.StatusConflict
	default:
		// Fall back to generic error handling
		return controllers.GetGenericStatusCode(err)
	}
}

// ClustersResult is the output data transfer object for the clusters controller
// that handles multiple clusters.
type ClustersResult struct {
	ClustersResultError
	Data *cloudcluster.Clusters `json:"data,omitempty"`
}

// GetData returns the data from the ClustersResult
func (dto *ClustersResult) GetData() any {
	return dto.Data
}

// NewClustersResult creates a new ClustersResult.
func NewClustersResult(data *cloudcluster.Clusters, err error) *ClustersResult {
	return &ClustersResult{
		ClustersResultError: ClustersResultError{Error: err},
		Data:                data,
	}
}

// ClusterResult is the output data transfer object for the clusters controller
// that handles a single cluster.
type ClusterResult struct {
	ClustersResultError
	Data *cloudcluster.Cluster `json:"data,omitempty"`
}

// GetData returns the data from the ClusterResult
func (dto *ClusterResult) GetData() any {
	return dto.Data
}

// NewClusterResult creates a new ClusterResult.
func NewClusterResult(data *cloudcluster.Cluster, err error) *ClusterResult {
	return &ClusterResult{
		ClustersResultError: ClustersResultError{Error: err},
		Data:                data,
	}
}

// TaskResult is the output data transfer object for the clusters controller
// that handles tasks.
type TaskResult struct {
	ClustersResultError
	Data tasks.ITask `json:"data,omitempty"`
}

// GetData returns the data from the TaskResult
func (dto *TaskResult) GetData() any {
	return dto.Data
}

// NewTaskResult creates a new TaskResult.
func NewTaskResult(task tasks.ITask, err error) *TaskResult {
	return &TaskResult{
		ClustersResultError: ClustersResultError{Error: err},
		Data:                task,
	}
}
