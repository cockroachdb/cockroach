// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// sqlStatsReplicationFactorChange overrides the zone configurations of
// system.statement_statistics and system.transaction_statistics to use a
// replication factor of 3 instead of inheriting the system tenant default of
// 5. These tables store observability data; the loss-tolerance permitted by 3x
// replication is acceptable, while halving the storage footprint of two of the
// largest system tables.
//
// This function runs in two contexts:
//   - As a permanent step in bootstrapCluster, so newly-created clusters get the
//     reduced replication factor at bootstrap.
//   - As a version-gated tenant upgrade tied to
//     V26_3_ReduceSQLStatsReplicationFactor, so existing clusters apply the
//     change as part of the upgrade.
func sqlStatsReplicationFactorChange(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	id, _, _ := readerTenantInfo(ctx, d)
	if id.IsSet() {
		// Don't perform the upgrade for read-from-standby tenants; their zone
		// configurations are managed by the source cluster.
		return nil
	}

	// Backup tests skip zone-config bootstrap because these tables are not part
	// of backup; honor the same knob here so those tests don't fail trying to
	// configure zones for tables they never created.
	if knobs := d.TestingKnobs; knobs != nil && knobs.SkipZoneConfigBootstrap {
		return nil
	}

	tables := []string{
		"system.statement_statistics",
		"system.transaction_statistics",
	}
	for _, table := range tables {
		if _, err := d.DB.Executor().ExecEx(
			ctx,
			"set-sql-stats-num-replicas",
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
			fmt.Sprintf("ALTER TABLE %s CONFIGURE ZONE USING num_replicas = $1", table),
			3,
		); err != nil {
			return err
		}
	}
	return nil
}
