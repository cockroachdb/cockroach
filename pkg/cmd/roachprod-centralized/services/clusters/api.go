// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"log/slog"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/internal/operations"
	sclustertasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// SyncClouds creates a task to sync the clouds.
func (s *Service) SyncClouds(
	ctx context.Context, l *logger.Logger, _ *pkgauth.Principal,
) (tasks.ITask, error) {
	// Create a task to sync the clouds if one is not already planned.
	return s._taskService.CreateTaskIfNotAlreadyPlanned(ctx, l, sclustertasks.NewTaskSync())
}

// GetAllClusters returns all clusters.
func (s *Service) GetAllClusters(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	input types.InputGetAllClustersDTO,
) (cloudcluster.Clusters, error) {

	c, err := s._store.GetClusters(ctx, l, input.Filters)
	if err != nil {
		return nil, err
	}

	// If the principal has view:all permission, return all clusters directly.
	if principal.HasPermission(types.PermissionViewAll) {
		return c, nil
	}

	// Filter clusters based on view permissions.
	filteredClusters := make(cloudcluster.Clusters, 0)
	for name, cluster := range c {
		if !checkClusterAccessPermission(principal, cluster, types.PermissionView, s.providerEnvironments) {
			continue
		}

		filteredClusters[name] = cluster
	}

	return filteredClusters, nil
}

// GetCluster returns a cluster.
func (s *Service) GetCluster(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	input types.InputGetClusterDTO,
) (*cloudcluster.Cluster, error) {

	c, err := s._store.GetCluster(ctx, l, input.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return nil, types.ErrClusterNotFound
		}
		return nil, err
	}

	// Check view permissions
	if !checkClusterAccessPermission(principal, &c, types.PermissionView, s.providerEnvironments) {
		l.Warn("principal not authorized to access cluster",
			slog.String("principal_email", func() string {
				if principal.User != nil {
					return principal.User.Email
				}
				return "unknown"
			}()),
			slog.String("cluster_name", input.Name),
		)
		return nil, types.ErrClusterNotFound
	}

	return &c, nil
}

// RegisterCluster registers a cluster with the state that was externally created.
func (s *Service) RegisterCluster(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	input types.InputRegisterClusterDTO,
) (*cloudcluster.Cluster, error) {

	// Check create permissions
	if !checkClusterCreatePermission(principal, &input.Cluster, s.providerEnvironments) {
		return nil, types.ErrForbidden
	}

	// Cluster's owner is set to the principal's user email or service account name.
	if principal.User != nil {
		input.Cluster.User = principal.User.Email
	} else if principal.ServiceAccount != nil {
		if principal.DelegatedFrom != nil && principal.DelegatedFromEmail != "" {
			input.Cluster.User = principal.DelegatedFromEmail
		} else {
			input.Cluster.User = principal.ServiceAccount.Name
		}
	} else {
		// This should not happen as only user or service account principals
		// can call this method.
		return nil, types.ErrForbidden
	}

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
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	input types.InputRegisterClusterUpdateDTO,
) (*cloudcluster.Cluster, error) {

	// Check that the cluster exists.
	c, err := s._store.GetCluster(ctx, l, input.Cluster.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return nil, types.ErrClusterNotFound
		}
		return nil, err
	}

	// Check update permissions, don't reveal existence otherwise
	if !checkClusterAccessPermission(principal, &c, types.PermissionUpdate, s.providerEnvironments) {
		return nil, types.ErrClusterNotFound
	}

	// Preserve immutable fields
	input.Name = c.Name
	input.CreatedAt = c.CreatedAt
	input.Cluster.User = c.User

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
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	input types.InputRegisterClusterDeleteDTO,
) error {

	// Check that the cluster exists.
	c, err := s._store.GetCluster(ctx, l, input.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return types.ErrClusterNotFound
		}
		return err
	}

	// Check delete permissions, don't reveal existence otherwise
	if !checkClusterAccessPermission(principal, &c, types.PermissionDelete, s.providerEnvironments) {
		return types.ErrClusterNotFound
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

// checkClusterCreatePermission verifies the principal has permission to create
// the cluster based on their scoped permissions for the cluster's cloud providers.
func checkClusterCreatePermission(
	principal *pkgauth.Principal,
	cluster *cloudcluster.Cluster,
	providerEnvironments map[string]string,
) bool {
	for _, providerID := range cluster.CloudProviders {
		env, ok := providerEnvironments[providerID]
		if !ok {
			// No environment mapping for this provider — deny access.
			return false
		}

		if !principal.HasPermissionScoped(types.PermissionCreate, env) {
			return false
		}
	}

	return true
}

// checkClusterAccessPermission verifies the principal has the required
// permission for all cloud providers in the cluster. Each providerID in
// cluster.CloudProviders is resolved to an environment name via the
// providerEnvironments map; this environment name is then used as the
// permission scope when calling HasPermissionScoped. Access is denied if
// a providerID has no mapped environment.
func checkClusterAccessPermission(
	principal *pkgauth.Principal,
	cluster *cloudcluster.Cluster,
	requiredPermission string, // e.g., "clusters:view", "clusters:delete"
	providerEnvironments map[string]string,
) bool {
	// Check ownership. For users compare email, for service accounts
	// compare name or delegated user email.
	isUserOwner := principal.User != nil && principal.User.Email == cluster.User

	isSAOwner := false
	if principal.ServiceAccount != nil {
		if principal.ServiceAccount.Name == cluster.User {
			isSAOwner = true
		} else if principal.DelegatedFrom != nil && principal.DelegatedFromEmail == cluster.User {
			isSAOwner = true
		}
	}
	isOwner := isUserOwner || isSAOwner

	allClustersPermission := requiredPermission + ":all"
	ownClustersPermission := requiredPermission + ":own"

	for _, providerID := range cluster.CloudProviders {
		env, ok := providerEnvironments[providerID]
		if !ok {
			// No environment mapping for this provider — deny access.
			return false
		}

		hasPermission := principal.HasPermissionScoped(allClustersPermission, env)

		// Check "own" variant if allowed and user is owner
		if !hasPermission && isOwner {
			hasPermission = principal.HasPermissionScoped(ownClustersPermission, env)
		}

		if !hasPermission {
			return false
		}
	}

	return true
}
