// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"log/slog"
	"strings"

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
		if !checkClusterAccessPermission(principal, cluster, types.PermissionView) {
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
	if !checkClusterAccessPermission(principal, &c, types.PermissionView) {
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
	if !checkClusterAccessPermission(principal, &input.Cluster, types.PermissionCreate) {
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

	// Check update permissions, don't reveal existence otherwise
	if !checkClusterAccessPermission(principal, &input.Cluster, types.PermissionUpdate) {
		return nil, types.ErrClusterNotFound
	}

	// Check that the cluster exists.
	c, err := s._store.GetCluster(ctx, l, input.Cluster.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return nil, types.ErrClusterNotFound
		}
		return nil, err
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
	if !checkClusterAccessPermission(principal, &c, types.PermissionDelete) {
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

// extractProviderAccounts parses CloudProviders strings into provider:account pairs
func extractProviderAccounts(cloudProviders []string) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{})
	for _, providerAccount := range cloudProviders {
		elems := strings.SplitN(providerAccount, "-", 2)
		if len(elems) != 2 {
			continue
		}

		provider := elems[0]
		account := elems[1]

		if _, ok := result[provider]; !ok {
			result[provider] = make(map[string]struct{})
		}
		result[provider][account] = struct{}{}
	}
	return result
}

// checkClusterAccessPermission is a helper function that verifies the principal
// has the required permission for all provider:account pairs in the cluster.
// It handles both "all" and "own" variants of the permission.
func checkClusterAccessPermission(
	principal *pkgauth.Principal,
	cluster *cloudcluster.Cluster,
	requiredPermission string, // e.g., "clusters:view", "clusters:delete"
) bool {
	providerAccounts := extractProviderAccounts(cluster.CloudProviders)

	// Check ownership. for users compare email, for service accounts compare name
	// For users, check ownership by email
	isUserOwner := principal.User != nil && principal.User.Email == cluster.User

	// For service accounts, check ownership by service account name
	// or delegated user email if the service account is delegated.
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

	for provider, accounts := range providerAccounts {
		for account := range accounts {
			hasPermission := principal.HasPermissionScoped(allClustersPermission, provider, account)

			// Check "own" variant if allowed and user is owner
			if !hasPermission && isOwner {
				hasPermission = principal.HasPermissionScoped(ownClustersPermission, provider, account)
			}

			if !hasPermission {
				return false
			}
		}
	}

	return true
}
