// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/internal/operations"
	sclustertasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// SyncClouds creates a task to sync the clouds.
func (s *Service) SyncClouds(ctx context.Context, l *logger.Logger) (tasks.ITask, error) {
	// Create a task to sync the clouds if one is not already planned.
	return s._taskService.CreateTaskIfNotAlreadyPlanned(ctx, l, sclustertasks.NewTaskSync())
}

// GetAllClusters returns all clusters.
func (s *Service) GetAllClusters(
	ctx context.Context, l *logger.Logger, input types.InputGetAllClustersDTO,
) (cloudcluster.Clusters, error) {

	c, err := s._store.GetClusters(ctx, l, input.Filters)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (s *Service) GetAllDNSZoneVMs(
	ctx context.Context, l *logger.Logger, dnsZone string,
) (vm.List, error) {
	c, err := s._store.GetClusters(ctx, l, *filters.NewFilterSet())
	if err != nil {
		return nil, err
	}

	var vms vm.List
	for _, cluster := range c {
		for _, vm := range cluster.VMs {
			if vm.PublicDNSZone != dnsZone {
				continue
			}
			vms = append(vms, vm)
		}
	}

	return vms, nil
}

// GetCluster returns a cluster.
func (s *Service) GetCluster(
	ctx context.Context, l *logger.Logger, input types.InputGetClusterDTO,
) (*cloudcluster.Cluster, error) {

	c, err := s._store.GetCluster(ctx, l, input.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return nil, types.ErrClusterNotFound
		}
		return nil, err
	}

	return &c, nil
}

// RegisterCluster registers a cluster with the state that was externally created.
func (s *Service) RegisterCluster(
	ctx context.Context, l *logger.Logger, input types.InputRegisterClusterDTO,
) (*cloudcluster.Cluster, error) {

	// Check that the cluster does not already exist.
	_, err := s._store.GetCluster(ctx, l, input.Cluster.Name)
	if err == nil {
		return nil, types.ErrClusterAlreadyExists
	}

	// Create the operation.
	op := operations.OperationCreate{
		Cluster: input.Cluster,
	}

	// Apply the operation on the current repository.
	err = op.ApplyOnRepository(ctx, l, s._store)
	if err != nil {
		return nil, err
	}

	// If another instance is syncing, enqueue this operation for later replay
	opData, err := s.operationToOperationData(op)
	if err != nil {
		l.Error("failed to convert operation to data",
			slog.Any("error", err),
			slog.String("cluster_name", input.Cluster.Name),
			slog.String("operation_type", "create"),
		)
	} else {
		enqueued, err := s.conditionalEnqueueOperationWithHealthCheck(ctx, l, opData)
		if err != nil {
			l.Error("failed to enqueue operation",
				slog.Any("error", err),
				slog.String("cluster_name", input.Cluster.Name),
				slog.String("operation_type", "create"),
				slog.String("operation_id", opData.ID),
			)
		} else if enqueued {
			l.Debug("operation enqueued for sync replay", slog.String("operation_id", opData.ID))
		}
	}

	// After a cluster registration, we trigger a task to create its DNS records.
	dnsZone, createRecords, deleteRecords := s.computeDNSRecordChanges(
		ctx, l,
		cloudcluster.Cluster{VMs: vm.List{}},
		input.Cluster,
	)

	err = s.enqueueManageDNSRecordTask(ctx, l, input.Cluster.Name, dnsZone, createRecords, deleteRecords)
	if err != nil {
		// We log the error but do not fail the cluster registration.
		l.Error("failed to enqueue public DNS records creation task",
			slog.Any("error", err),
			slog.String("cluster_name", input.Cluster.Name),
			slog.String("trigger_operation", "register_cluster"),
		)
	}

	// Return the created cluster.
	return &input.Cluster, nil
}

// RegisterClusterUpdate registers an external update to a cluster with the state.
func (s *Service) RegisterClusterUpdate(
	ctx context.Context, l *logger.Logger, input types.InputRegisterClusterUpdateDTO,
) (*cloudcluster.Cluster, error) {

	// Check that the cluster exists.
	c, err := s._store.GetCluster(ctx, l, input.Cluster.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return nil, types.ErrClusterNotFound
		}
		return nil, err
	}

	// Create the operation.
	op := operations.OperationUpdate{
		Cluster: input.Cluster,
	}

	// Apply the operation on the current repository.
	err = op.ApplyOnRepository(ctx, l, s._store)
	if err != nil {
		return nil, err
	}

	// If another instance is syncing, enqueue this operation for later replay
	opData, err := s.operationToOperationData(operations.OperationUpdate{Cluster: input.Cluster})
	if err != nil {
		l.Error("failed to convert operation to data",
			slog.Any("error", err),
			slog.String("cluster_name", input.Cluster.Name),
			slog.String("operation_type", "update"),
		)
	} else {
		enqueued, err := s.conditionalEnqueueOperationWithHealthCheck(ctx, l, opData)
		if err != nil {
			l.Error("failed to enqueue operation",
				slog.Any("error", err),
				slog.String("cluster_name", input.Cluster.Name),
				slog.String("operation_type", "update"),
				slog.String("operation_id", opData.ID),
			)
		} else if enqueued {
			l.Debug("operation enqueued for sync replay", slog.String("operation_id", opData.ID))
		}
	}

	// After a cluster update, we trigger a task to update its DNS records.
	dnsZone, createRecords, deleteRecords := s.computeDNSRecordChanges(ctx, l, c, input.Cluster)

	err = s.enqueueManageDNSRecordTask(ctx, l, c.Name, dnsZone, createRecords, deleteRecords)
	if err != nil {
		// We log the error but do not fail the cluster registration.
		l.Error("failed to enqueue public DNS records update task",
			slog.Any("error", err),
			slog.String("cluster_name", c.Name),
			slog.String("trigger_operation", "register_cluster_update"),
		)
	}

	return &input.Cluster, nil
}

// RegisterClusterDelete registers the external deletion of a cluster with the state.
func (s *Service) RegisterClusterDelete(
	ctx context.Context, l *logger.Logger, input types.InputRegisterClusterDeleteDTO,
) error {

	// Check that the cluster exists.
	c, err := s._store.GetCluster(ctx, l, input.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return types.ErrClusterNotFound
		}
		return err
	}

	// Create the operation.
	op := operations.OperationDelete{
		Cluster: c,
	}

	// Apply the operation on the current repository.
	err = op.ApplyOnRepository(ctx, l, s._store)
	if err != nil {
		return err
	}

	// If another instance is syncing, enqueue this operation for later replay
	opData, err := s.operationToOperationData(operations.OperationDelete{Cluster: c})
	if err != nil {
		l.Error("failed to convert operation to data",
			slog.Any("error", err),
			slog.String("cluster_name", input.Name),
			slog.String("operation_type", "delete"),
		)
	} else {
		enqueued, err := s.conditionalEnqueueOperationWithHealthCheck(ctx, l, opData)
		if err != nil {
			l.Error("failed to enqueue operation",
				slog.Any("error", err),
				slog.String("cluster_name", input.Name),
				slog.String("operation_type", "delete"),
				slog.String("operation_id", opData.ID),
			)
		} else if enqueued {
			l.Debug("operation enqueued for sync replay", slog.String("operation_id", opData.ID))
		}
	}

	// After a cluster deletion, we trigger a task to delete its DNS records.
	dnsZone, createRecords, deleteRecords := s.computeDNSRecordChanges(
		ctx, l,
		c,
		cloudcluster.Cluster{VMs: vm.List{}},
	)

	err = s.enqueueManageDNSRecordTask(ctx, l, c.Name, dnsZone, createRecords, deleteRecords)
	if err != nil {
		// We log the error but do not fail the cluster registration.
		l.Error("failed to enqueue public DNS records deletion task",
			slog.Any("error", err),
			slog.String("cluster_name", c.Name),
			slog.String("trigger_operation", "register_cluster_delete"),
		)
	}

	return nil
}
