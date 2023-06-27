// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// FirstUpgradeFromReleasePrecondition is the precondition check for upgrading
// from any supported major release.
//
// This precondition function performs health checks on the catalog metadata.
// This prevents cluster version upgrades from proceeding when it detects
// known corruptions. This forces cluster operations to repair the metadata,
// which may be somewhat annoying, but it's considered a lesser evil to being
// stuck in a mixed-version state due to a later upgrade step throwing
// errors.
//
// The implementation of this function may evolve in the future to include
// other checks beyond querying the invalid_objects virtual table, which
// mainly checks descriptor validity, as that has historically been the main
// source of problems.
func FirstUpgradeFromReleasePrecondition(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	const q = `SELECT count(*) FROM "".crdb_internal.invalid_objects AS OF SYSTEM TIME '-10s'`
	row, err := d.InternalExecutor.QueryRow(ctx, "query-invalid-objects", nil /* txn */, q)
	if err != nil {
		return err
	}
	if n := row[0].String(); n != "0" {
		return errors.AssertionFailedf("%s row(s) found in \"\".crdb_internal.invalid_objects, expected none", n)
	}
	return nil
}
