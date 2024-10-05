// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// systemPrivilegesTableMigration creates the system.job_info table.
func systemJobInfoTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Create the job_info table proper.
	if err := createSystemTable(
		ctx, d.DB, d.Settings, d.Codec, systemschema.SystemJobInfoTable, tree.LocalityLevelTable,
	); err != nil {
		return err
	}

	// Also bump the version of the descriptor for system.jobs. This
	// ensures that this migration synchronizes with all other
	// migrations that touch jobs, and concurrent transactions that
	// perform DDL operations. We need this because just creating a new
	// table above does not synchronize with other accesses to
	// system.jobs on its own.
	return d.DB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) (err error) {
		collection := txn.Descriptors()
		t, err := collection.MutableByID(txn.KV()).Desc(ctx, keys.JobsTableID)
		if err != nil {
			return err
		}
		return collection.WriteDesc(ctx, true /* kvTracing */, t, txn.KV())
	})
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
