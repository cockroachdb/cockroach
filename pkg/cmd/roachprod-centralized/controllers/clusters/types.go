// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/errors"
)

// InputGetAllDTO is input data transfer object that handles requests parameters
// for the clusters.GetAll() controller.
type InputGetAllDTO struct {
	Username string `form:"username" binding:"omitempty,alphanum"`
}

// ToServiceInputGetAllDTO converts the InputGetAllDTO data transfer object
// to a clusters' service InputGetAllDTO.
func (dto *InputGetAllDTO) ToServiceInputGetAllDTO() clusters.InputGetAllClustersDTO {
	return clusters.InputGetAllClustersDTO{
		Username: dto.Username,
	}
}

// InputCreateClusterDTO is input data transfer object that handles requests
// parameters for the clusters.Create() controller.
type InputCreateClusterDTO struct {
	OperationBeginTime time.Time `json:"operation-begin-time" binding:"required"`
	OperationEndTime   time.Time `json:"operation-end-time" binding:"required"`
	cloud.Cluster
}

// ToServiceInputCreateClusterDTO converts the InputCreateClusterDTO data transfer object
// to a clusters' service InputCreateClusterDTO.
func (dto *InputCreateClusterDTO) ToServiceInputCreateClusterDTO() clusters.InputCreateClusterDTO {
	return clusters.InputCreateClusterDTO{
		Cluster: dto.Cluster,
	}
}

// InputUpdateClusterDTO is input data transfer object that handles requests
// parameters for the clusters.Create() controller.
type InputUpdateClusterDTO struct {
	OperationBeginTime time.Time `json:"operation-begin-time" binding:"required"`
	OperationEndTime   time.Time `json:"operation-end-time" binding:"required"`
	cloud.Cluster
}

// ToServiceInputCreateClusterDTO converts the InputUpdateClusterDTO data transfer object
// to a clusters' service InputCreateClusterDTO.
func (dto *InputUpdateClusterDTO) ToServiceInputUpdateClusterDTO() clusters.InputUpdateClusterDTO {
	return clusters.InputUpdateClusterDTO{
		Cluster: dto.Cluster,
	}
}

type ClustersResultError struct {
	Error error `json:"error"`
}

// GetError returns the error from the ClustersResultError.
func (dto *ClustersResultError) GetError() error {
	return dto.Error
}

// GetAssociatedStatusCode returns the status code associated
// with the ClustersResultError.
func (dto *ClustersResultError) GetAssociatedStatusCode() int {
	err := dto.GetError()
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, clusters.ErrClusterNotFound):
		return http.StatusNotFound
	case errors.Is(err, clusters.ErrClusterAlreadyExists):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// ClustersResult is the output data transfer object for the clusters controller
// that handles multiple clusters.
type ClustersResult struct {
	ClustersResultError
	Data *cloud.Clusters `json:"data,omitempty"`
}

// GetData returns the data from the ClustersResult
func (dto *ClustersResult) GetData() any {
	return dto.Data
}

// FromService converts the result of the clusters service to a ClustersResult.
func (dto *ClustersResult) FromService(data cloud.Clusters, err error) *ClustersResult {
	dto.Data = &data
	dto.Error = err
	return dto
}

// ClusterResult is the output data transfer object for the clusters controller
// that handles a single cluster.
type ClusterResult struct {
	ClustersResultError
	Data *cloud.Cluster `json:"data,omitempty"`
}

// GetData returns the data from the ClusterResult
func (dto *ClusterResult) GetData() any {
	return dto.Data
}

// FromService converts the result of the clusters service to a ClusterResult.
func (dto *ClusterResult) FromService(data *cloud.Cluster, err error) *ClusterResult {
	dto.Data = data
	dto.Error = err
	return dto
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

// FromService converts the result of the clusters service to a TaskResult.
func (dto *TaskResult) FromService(task tasks.ITask, err error) *TaskResult {
	dto.Data = task
	dto.Error = err
	return dto
}
