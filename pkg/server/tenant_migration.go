// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
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

// ValidateTargetClusterVersion implements the MigrationServer interface.
// It's used to verify that each tenant pod is running a binary that's recent
// enough to support the upgrade.
func (m *TenantMigrationServer) ValidateTargetClusterVersion(
	ctx context.Context, req *serverpb.ValidateTargetClusterVersionRequest,
) (*serverpb.ValidateTargetClusterVersionResponse, error) {
	ctx = m.sqlServer.AnnotateCtx(ctx)
	ctx = logtags.AddTag(ctx, "validate-tenant-cluster-version", nil)

	targetCV := req.ClusterVersion
	tenantVersion := m.sqlServer.settingsWatcher.GetTenantClusterVersion()

	// We're validating that the following version invariant holds:
	//   tenant's min supported <= upgrade target <= tenant's binary
	if targetCV.Less(tenantVersion.BinaryMinSupportedVersion()) {
		msg := fmt.Sprintf("requested tenant cluster upgrade version %s is less than the tenant "+
			"binary's minimum supported version %s",
			targetCV, tenantVersion.BinaryMinSupportedVersion())
		log.Warningf(ctx, "%s", msg)
		return nil, errors.Newf("%s", redact.Safe(msg))
	}

	if tenantVersion.BinaryVersion().Less(targetCV.Version) {
		msg := fmt.Sprintf("sql pod %s is running a binary version %s which is "+
			"less than the attempted upgrade version %s",
			m.sqlServer.sqlIDContainer.SQLInstanceID().String(),
			tenantVersion.BinaryVersion(), targetCV)
		log.Warningf(ctx, "%s", msg)
		err := errors.Newf("%s", redact.Safe(msg))
		return nil, errors.WithHintf(err,
			"upgrade sql pod %s binary to version %s (or higher) to allow tenant upgrade to succeed",
			m.sqlServer.sqlIDContainer.SQLInstanceID().String(),
			targetCV,
		)
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
	opName := "bump-cluster-version"
	ctx = m.sqlServer.AnnotateCtx(ctx)
	ctx = logtags.AddTag(ctx, opName, nil)

	if err := m.sqlServer.stopper.RunTaskWithErr(ctx, opName, func(
		ctx context.Context,
	) error {
		m.Lock()
		defer m.Unlock()
		return bumpTenantClusterVersion(ctx,
			m.sqlServer.settingsWatcher.GetTenantClusterVersion(),
			*req.ClusterVersion)
	}); err != nil {
		return nil, err
	}
	return &serverpb.BumpClusterVersionResponse{}, nil
}

// bumpTenantClusterVersion increases the active version for a tenant pod. This
// logic is much more straightforward for tenant upgrades because tenants don't
// store an engine version in addition to the active version.
func bumpTenantClusterVersion(
	ctx context.Context, tenantCV clusterversion.Handle, newCV clusterversion.ClusterVersion,
) error {
	activeCV := tenantCV.ActiveVersion(ctx)
	if !activeCV.Less(newCV.Version) {
		// Nothing to do.
		return nil
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
