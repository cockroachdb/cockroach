// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

func interleavedTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	txn := d.DB.NewTxn(ctx, "interleaved-check")
	rows, err := d.InternalExecutor.QueryRowEx(ctx, "check-for-interleaved", txn, sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"select * from crdb_internal.interleaved")
	if err != nil {
		return err
	}
	if rows != nil {
		return errors.Newf("interleaved tables are no longer supported")
	}
	return nil
}

func interleavedTablesRemovedMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	// Next validate entire cluster is at InterleavedCreationBlockedMigration
	if !d.Settings.Version.IsActive(ctx, clusterversion.InterleavedCreationBlockedMigration) {
		return errors.Newf("interleaved tables can still be created")
	}
	txn := d.DB.NewTxn(ctx, "interleaved-check")
	rows, err := d.InternalExecutor.QueryRowEx(ctx, "check-for-interleaved", txn, sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"select * from crdb_internal.interleaved")
	if err != nil {
		return err
	}
	if rows != nil {
		return errors.Newf("interleaved tables are no longer supported at this version, please drop or uninterleave them.")
	}
	return nil
}
