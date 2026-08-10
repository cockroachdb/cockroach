// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationutils

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
)

// CheckSourceExternalConnectionUsage requires the caller to hold USAGE on the
// external connection named as the replication source. A non-external source
// has no connection to check and is a no-op.
//
// Replication sources only gained this check in v26.4, so it is gated on
// RequireUsagePrivilegeOnExternalConnections.
func CheckSourceExternalConnectionUsage(
	ctx context.Context, p sql.PlanHookState, configUri streamclient.ConfigUri,
) error {
	if !sqlclustersettings.RequireUsagePrivilegeOnExternalConnections.Get(&p.ExecCfg().Settings.SV) {
		return nil
	}
	name, ok := configUri.ExternalConnectionName()
	if !ok {
		return nil
	}
	ecPrivilege := &syntheticprivilege.ExternalConnectionPrivilege{
		ConnectionName: name,
	}
	return p.CheckPrivilege(ctx, ecPrivilege, privilege.USAGE)
}
