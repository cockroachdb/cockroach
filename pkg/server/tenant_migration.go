// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradecluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// TenantMigrationServer is an implementation of the Migration service for
// tenants. The RPCs here are used to power the upgrades infrastructure for
// tenants.
type TenantMigrationServer struct {
	sqlServer *SQLServer

	// We use this mutex to serialize attempts to bump the cluster version.
	syncutil.Mutex
}

var _ serverpb.MigrationServer = &TenantMigrationServer{}

func newTenantMigrationServer(server *SQLServer) *TenantMigrationServer {
	return &TenantMigrationServer{
		sqlServer: server,
	}
}

// validateTargetClusterVersion is a helper function for the interface defined
// ValidateTargetClusterVersion. Specifically, it validates that the following
// version invariant holds:
//
//	tenant's min supported <= upgrade target <= tenant's binary
func validateTargetClusterVersion(
	ctx context.Context,
	tenantVersion clusterversion.Handle,
	targetCV *clusterversion.ClusterVersion,
	instanceID base.SQLInstanceID,
) error {
	if targetCV.Less(tenantVersion.MinSupportedVersion()) {
		err := errors.Newf("requested tenant cluster upgrade version %s is less than the "+
			"binary's minimum supported version %s for SQL server instance %d",
			targetCV, tenantVersion.MinSupportedVersion(),
			instanceID)
		log.Warningf(ctx, "%v", err)
		return err
	}

	if tenantVersion.LatestVersion().Less(targetCV.Version) {
		err := errors.Newf("sql server %d is running a binary version %s which is "+
			"less than the attempted upgrade version %s",
			instanceID,
			tenantVersion.LatestVersion(), targetCV)
		log.Warningf(ctx, "%v", err)
		return errors.WithHintf(err,
			"upgrade sql server %d binary to version %s (or higher) to allow tenant upgrade to succeed",
			instanceID,
			targetCV,
		)
	}
	return nil
}

// ValidateTargetClusterVersion implements the MigrationServer interface.
// It's used to verify that each tenant server is running a binary that's recent
// enough to support the upgrade.
func (m *TenantMigrationServer) ValidateTargetClusterVersion(
	ctx context.Context, req *serverpb.ValidateTargetClusterVersionRequest,
) (*serverpb.ValidateTargetClusterVersionResponse, error) {
	ctx = m.sqlServer.AnnotateCtx(ctx)
	ctx = logtags.AddTag(ctx, "validate-tenant-cluster-version", nil)

	if err := validateTargetClusterVersion(
		ctx,
		m.sqlServer.settingsWatcher.GetTenantClusterVersion(),
		req.ClusterVersion,
		m.sqlServer.SQLInstanceID()); err != nil {
		return nil, err
	}

	resp := &serverpb.ValidateTargetClusterVersionResponse{}
	return resp, nil
}

// BumpClusterVersion implements the MigrationServer interface. It's used to
// inform us of a cluster version bump. When called for tenants we update the
// tenant server's active cluster version.
func (m *TenantMigrationServer) BumpClusterVersion(
	ctx context.Context, req *serverpb.BumpClusterVersionRequest,
) (*serverpb.BumpClusterVersionResponse, error) {
	opName := upgradecluster.BumpClusterVersionOpName
	ctx = m.sqlServer.AnnotateCtx(ctx)
	ctx = logtags.AddTag(ctx, opName, nil)

	if err := m.sqlServer.stopper.RunTaskWithErr(ctx, opName, func(
		ctx context.Context,
	) error {
		m.Lock()
		defer m.Unlock()
		return bumpTenantClusterVersion(ctx,
			m.sqlServer.settingsWatcher.GetTenantClusterVersion(),
			*req.ClusterVersion,
			m.sqlServer.SQLInstanceID())
	}); err != nil {
		return nil, err
	}
	return &serverpb.BumpClusterVersionResponse{}, nil
}

// bumpTenantClusterVersion increases the active version for a tenant server.
// This logic is much more straightforward for tenant upgrades because tenants
// don't store an engine version in addition to the active version.
func bumpTenantClusterVersion(
	ctx context.Context,
	tenantCV clusterversion.Handle,
	newCV clusterversion.ClusterVersion,
	instanceID base.SQLInstanceID,
) error {
	activeCV := tenantCV.ActiveVersion(ctx)
	log.Infof(ctx, "bumping cluster version from %v to %v on instance %s", activeCV, newCV, instanceID.String())
	if !activeCV.Less(newCV.Version) {
		// Nothing to do.
		return nil
	}
	// NOTE: at the time of this comment's writing, it was accurate. If any
	// logic has since changed in upgrademanager.Manager.Migrate(), this comment
	// may have rotted. Please confirm the logic in there before relying on this
	// comment's accuracy. You've been warned.
	//
	// In upgrademanger.Manager.Migrate(), the tenant upgrade interlock first
	// checks that all running SQL servers are at an acceptable binary version
	// for the upgrade to proceed. Once that check passes, it will proceed to
	// perform the upgrade, first by bumping to a fence version, then by
	// performing the actual upgrade migration, and finally bumping to the
	// upgrade version. It's possible that after the check and before the first
	// bump, that a new SQL server has started up with an invalid binary (its
	// binary version is too low for the pending upgrade). To catch that case as
	// early as possible, we check again here and prevent the bump from going
	// through if we find that the binary version is no longer valid. Note that
	// we only have to worry about a new SQL server starting between the _first_
	// check and the fence bump, since once that fence bump occurs (and is
	// persisted to the settings table), new SQL servers with old binaries will
	// be prevented from starting.
	if err := validateTargetClusterVersion(ctx, tenantCV, &newCV, instanceID); err != nil {
		return err
	}

	// We bump the local version gate here.
	if err := tenantCV.SetActiveVersion(ctx, newCV); err != nil {
		return err
	}
	log.Infof(ctx, "active cluster version setting is now %s (up from %s)",
		newCV.PrettyPrint(), activeCV.PrettyPrint())
	return nil
}

// SyncAllEngines implements the MigrationServer interface.
func (m *TenantMigrationServer) SyncAllEngines(
	ctx context.Context, _ *serverpb.SyncAllEnginesRequest,
) (*serverpb.SyncAllEnginesResponse, error) {
	return nil, errors.AssertionFailedf("tenants upgrades do not have to sync engines")
}

// PurgeOutdatedReplicas implements the MigrationServer interface.
func (m *TenantMigrationServer) PurgeOutdatedReplicas(
	ctx context.Context, req *serverpb.PurgeOutdatedReplicasRequest,
) (*serverpb.PurgeOutdatedReplicasResponse, error) {
	return nil, errors.AssertionFailedf("tenants upgrades do not require replica purging")
}

// WaitForSpanConfigSubscription implements the MigrationServer interface.
func (m *TenantMigrationServer) WaitForSpanConfigSubscription(
	ctx context.Context, _ *serverpb.WaitForSpanConfigSubscriptionRequest,
) (*serverpb.WaitForSpanConfigSubscriptionResponse, error) {
	return nil, errors.AssertionFailedf("tenants upgrades do not have to wait for span config subscription")
}
