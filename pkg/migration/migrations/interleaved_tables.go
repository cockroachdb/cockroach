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

func interleavedTablesRemovedMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	txn := d.DB.NewTxn(ctx, "interleaved-check")
	rows, err := d.InternalExecutor.QueryRowEx(ctx, "check-for-interleaved", txn, sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT * FROM crdb_internal.interleaved LIMIT 1;")
	if err != nil {
		return err
	}
	if rows != nil {
		return errors.Newf("interleaved tables are no longer supported at this version, please drop or uninterleave them.")
	}
	return nil
}
