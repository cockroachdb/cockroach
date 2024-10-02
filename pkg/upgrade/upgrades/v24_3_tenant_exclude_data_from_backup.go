// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// tenantExcludeDataFromBackup marks some system tables with low GC
// TTL as excluded from backup. This step is only performed on
// non-system tenants, as a similar migration was already performed on
// the system tenant in the 24.1 cycle (see #120144).
func tenantExcludeDataFromBackup(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if d.Codec.ForSystemTenant() {
		return nil
	}

	return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		for _, tableName := range []catconstants.SystemTableName{
			catconstants.ReplicationConstraintStatsTableName,
			catconstants.ReplicationStatsTableName,
			catconstants.TenantUsageTableName,
			catconstants.LeaseTableName,
			catconstants.SpanConfigurationsTableName,
		} {
			if _, err := txn.ExecEx(
				ctx,
				"mark-table-excluded-from-backup",
				txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				fmt.Sprintf("ALTER TABLE system.public.%s SET (exclude_data_from_backup = true)", tableName),
			); err != nil {
				return errors.Wrapf(err, "failed to set exclude_data_from_backup on table %s", tableName)
			}
		}

		return nil
	})
}
