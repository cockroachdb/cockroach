// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
)

// IOperation is an interface for operations on clusters.
type IOperation interface {
	ApplyOnRepository(ctx context.Context, l *logger.Logger, store clusters.IClustersRepository) error
	ApplyOnStagingClusters(ctx context.Context, l *logger.Logger, clusters cloudcluster.Clusters) error
}

// OperationCreate represents a create operation.
type OperationCreate struct {
	Cluster cloudcluster.Cluster
}

// ApplyOnRepository applies the operation on the repository.
func (o OperationCreate) ApplyOnRepository(
	ctx context.Context, l *logger.Logger, store clusters.IClustersRepository,
) error {
	return store.StoreCluster(ctx, l, o.Cluster)
}

// ApplyOnStagingClusters applies the operation on the staging clusters.
func (o OperationCreate) ApplyOnStagingClusters(
	ctx context.Context, l *logger.Logger, clusters cloudcluster.Clusters,
) error {
	clusters[o.Cluster.Name] = &o.Cluster
	return nil
}

// OperationDelete represents a delete operation.
type OperationDelete struct {
	Cluster cloudcluster.Cluster
}

// ApplyOnRepository applies the operation on the repository.
func (o OperationDelete) ApplyOnRepository(
	ctx context.Context, l *logger.Logger, store clusters.IClustersRepository,
) error {
	return store.DeleteCluster(ctx, l, o.Cluster)
}

// ApplyOnStagingClusters applies the operation on the staging clusters.
func (o OperationDelete) ApplyOnStagingClusters(
	ctx context.Context, l *logger.Logger, clusters cloudcluster.Clusters,
) error {
	delete(clusters, o.Cluster.Name)
	return nil
}

// OperationUpdate is an operation to update a cluster.
type OperationUpdate struct {
	Cluster cloudcluster.Cluster
}

// ApplyOnRepository applies the operation on the repository.
func (o OperationUpdate) ApplyOnRepository(
	ctx context.Context, l *logger.Logger, store clusters.IClustersRepository,
) error {
	return store.StoreCluster(ctx, l, o.Cluster)
}

// ApplyOnStagingClusters applies the operation on the staging clusters.
func (o OperationUpdate) ApplyOnStagingClusters(
	ctx context.Context, l *logger.Logger, clusters cloudcluster.Clusters,
) error {
	clusters[o.Cluster.Name] = &o.Cluster
	return nil
}
