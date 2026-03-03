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
			slog.String("principal", principal.GetOwnerIdentifier()),
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
	input.Cluster.User = principal.GetOwnerIdentifier()
	if input.Cluster.User == "" {
		// This should not happen as only user or service account principals can call this method.
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

	// Block updates to provisioning-managed clusters — their state is
	// controlled by the provisioning lifecycle.
	if c.IsProvisioningManaged() {
		return nil, types.ErrClusterManagedByProvisioning
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

	// Block deletion of provisioning-managed clusters — these must be
	// destroyed via the provisioning lifecycle, not the cluster API.
	if c.IsProvisioningManaged() {
		return types.ErrClusterManagedByProvisioning
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

// RegisterClusterInternal registers a cluster without auth checks.
// Used by hook executors that run inside task workers where no principal
// is available. The provisioning was already auth-checked at creation time.
func (s *Service) RegisterClusterInternal(
	ctx context.Context, l *logger.Logger, cluster cloudcluster.Cluster,
) error {
	// Idempotent: skip if cluster already exists.
	_, err := s._store.GetCluster(ctx, l, cluster.Name)
	if err == nil {
		l.Info("cluster already registered, skipping",
			slog.String("cluster_name", cluster.Name))
		return nil
	}
	if !errors.Is(err, clusters.ErrClusterNotFound) {
		return errors.Wrapf(err, "check cluster existence %q", cluster.Name)
	}

	op := operations.OperationCreate{Cluster: cluster}
	if err := op.ApplyOnRepository(ctx, l, s._store); err != nil {
		return err
	}

	// Enqueue sync operation (same pattern as RegisterCluster).
	opData, err := s.operationToOperationData(op)
	if err != nil {
		l.Error("failed to convert operation to data",
			slog.Any("error", err),
			slog.String("cluster_name", cluster.Name),
			slog.String("operation_type", "create"),
		)
	} else {
		if _, enqErr := s.conditionalEnqueueOperationWithHealthCheck(ctx, l, opData); enqErr != nil {
			l.Error("failed to enqueue operation",
				slog.Any("error", enqErr),
				slog.String("cluster_name", cluster.Name),
			)
		}
	}

	// Trigger DNS record creation.
	dnsZone, createRecords, deleteRecords := s.computeDNSRecordChanges(
		ctx, l, cloudcluster.Cluster{VMs: vm.List{}}, cluster)
	if err := s.enqueueManageDNSRecordTask(ctx, l, cluster.Name, dnsZone, createRecords, deleteRecords); err != nil {
		l.Error("failed to enqueue DNS records creation task",
			slog.Any("error", err),
			slog.String("cluster_name", cluster.Name),
		)
	}

	return nil
}

// UnregisterClusterInternal removes a cluster without auth checks.
// Verifies ownership: only deletes if the cluster's VMs carry a
// vm.TagProvisioningIdentifier label matching the given identifier.
// Idempotent: returns nil if cluster doesn't exist.
func (s *Service) UnregisterClusterInternal(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	provisioningOwner string,
	provisioningIdentifier string,
) error {
	c, err := s._store.GetCluster(ctx, l, clusterName)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			l.Info("cluster not found, skipping unregister",
				slog.String("cluster_name", clusterName))
			return nil
		}
		return err
	}

	// Ownership check 1: cluster owner must match provisioning owner.
	if c.User != provisioningOwner {
		return errors.Newf(
			"cluster %q owner mismatch (cluster user=%q, provisioning owner=%q)",
			clusterName, c.User, provisioningOwner,
		)
	}

	// Ownership check 2: every VM must carry the provisioning_identifier label.
	for i, machine := range c.VMs {
		vmOwnerID := machine.Labels[vm.TagProvisioningIdentifier]
		if vmOwnerID != provisioningIdentifier {
			return errors.Newf(
				"cluster %q VM[%d]=%q owner label mismatch (expected %s=%q, got %q)",
				clusterName, i, machine.Name, vm.TagProvisioningIdentifier, provisioningIdentifier, vmOwnerID,
			)
		}
	}

	op := operations.OperationDelete{Cluster: c}
	if err := op.ApplyOnRepository(ctx, l, s._store); err != nil {
		return err
	}

	// Enqueue sync operation.
	opData, err := s.operationToOperationData(op)
	if err != nil {
		l.Error("failed to convert operation to data",
			slog.Any("error", err),
			slog.String("cluster_name", clusterName),
			slog.String("operation_type", "delete"),
		)
	} else {
		if _, enqErr := s.conditionalEnqueueOperationWithHealthCheck(ctx, l, opData); enqErr != nil {
			l.Error("failed to enqueue operation",
				slog.Any("error", enqErr),
				slog.String("cluster_name", clusterName),
			)
		}
	}

	// Trigger DNS record deletion.
	dnsZone, createRecords, deleteRecords := s.computeDNSRecordChanges(
		ctx, l, c, cloudcluster.Cluster{VMs: vm.List{}})
	if err := s.enqueueManageDNSRecordTask(ctx, l, c.Name, dnsZone, createRecords, deleteRecords); err != nil {
		l.Error("failed to enqueue DNS records deletion task",
			slog.Any("error", err),
			slog.String("cluster_name", c.Name),
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
	isOwner := principal.GetOwnerIdentifier() == cluster.User

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
