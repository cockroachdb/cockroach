// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

const (
	TaskServiceName = "clusters"
)

var (
	// ErrClusterNotFound is the error returned when a cluster is not found.
	ErrClusterNotFound = utils.NewPublicError(fmt.Errorf("cluster not found"))
	// ErrClusterAlreadyExists is the error returned when a cluster already
	// exists.
	ErrClusterAlreadyExists = utils.NewPublicError(fmt.Errorf("cluster already exists"))
	// ErrShutdownTimeout is the error returned when the service shutdown times out.
	ErrShutdownTimeout = fmt.Errorf("service shutdown timeout")
)

// IService is the interface for the clusters service.
type IService interface {
	SyncClouds(context.Context, *logger.Logger) (tasks.ITask, error)
	GetAllClusters(context.Context, *logger.Logger, InputGetAllClustersDTO) (cloudcluster.Clusters, error)
	GetAllDNSZoneVMs(context.Context, *logger.Logger, string) (vm.List, error)
	GetCluster(context.Context, *logger.Logger, InputGetClusterDTO) (*cloudcluster.Cluster, error)
	RegisterCluster(context.Context, *logger.Logger, InputRegisterClusterDTO) (*cloudcluster.Cluster, error)
	RegisterClusterUpdate(context.Context, *logger.Logger, InputRegisterClusterUpdateDTO) (*cloudcluster.Cluster, error)
	RegisterClusterDelete(context.Context, *logger.Logger, InputRegisterClusterDeleteDTO) error
	Sync(ctx context.Context, l *logger.Logger) (cloudcluster.Clusters, error)
}

// InputGetAllClustersDTO is the data transfer object to get all clusters.
type InputGetAllClustersDTO struct {
	Filters filtertypes.FilterSet `json:"filters,omitempty"`
}

// InputGetClusterDTO is the data transfer object to get a cluster.
type InputGetClusterDTO struct {
	Name string `json:"name" binding:"required"`
}

// InputRegisterClusterDTO is the data transfer object to register a new cluster.
type InputRegisterClusterDTO struct {
	cloudcluster.Cluster
}

// InputRegisterClusterUpdateDTO is the data transfer object to register an update to a cluster.
type InputRegisterClusterUpdateDTO struct {
	cloudcluster.Cluster
}

// InputRegisterClusterDeleteDTO is the data transfer object to register a cluster deletion.
type InputRegisterClusterDeleteDTO struct {
	Name string `json:"name" binding:"required"`
}
