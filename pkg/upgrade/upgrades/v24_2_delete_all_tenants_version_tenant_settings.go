// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// allTenantsID is the value used for the `tenant_id` column in the
// `system.tenant_settings` table to encode overrides that apply to
// *all* tenants.
const allTenantsID = 0

// deleteAllTenantsVersionTenantSettings deletes bad data in the
// `system.tenant_settings` table possibly introduced as part of an
// upgrade to 23.1. For more details, see #125702.
func deleteAllTenantsVersionTenantSettings(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	_, err := d.DB.Executor().ExecEx(
		ctx, "delete-all-tenants-version-setting", nil /* txn */, sessiondata.NodeUserSessionDataOverride,
		"DELETE FROM system.tenant_settings WHERE tenant_id = $1 AND name = $2",
		allTenantsID, clusterversion.KeyVersionSetting,
	)

	return err
}
