// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// deleteVersionTenantSettings deletes rows in the
// `system.tenant_settings` table for the 'version' key. This can be
// potentially wrong data that may prevent tenants from being able to
// upgrade. For more details, see #125702.
func deleteVersionTenantSettings(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	_, err := d.DB.Executor().ExecEx(
		ctx, "delete-all-tenants-version-setting", nil /* txn */, sessiondata.NodeUserSessionDataOverride,
		"DELETE FROM system.tenant_settings WHERE name = $1",
		clusterversion.KeyVersionSetting,
	)

	return err
}
