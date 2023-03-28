// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// systemPrivilegesTableMigration creates the system.privileges table.
func systemJobInfoTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return createSystemTable(
		ctx, d.DB.KV(), d.Settings, d.Codec, systemschema.SystemJobInfoTable,
	)
}

const alterPayloadToNullableQuery = `
ALTER TABLE system.jobs ALTER COLUMN payload DROP NOT NULL
`

// alterPayloadColumnToNullable runs a schema change to drop the NOT NULL
// constraint on the system.jobs payload column.
func alterPayloadColumnToNullable(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	_, err := d.InternalExecutor.ExecEx(ctx, "set-job-payload-nullable", nil,
		sessiondata.NodeUserSessionDataOverride, alterPayloadToNullableQuery)
	return err
}
