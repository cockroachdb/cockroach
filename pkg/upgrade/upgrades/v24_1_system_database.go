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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// alterSystemDatabaseSurvivalGoal sets the survival goal on the system database
// to be SURVIVE REGION.
func alterSystemDatabaseSurvivalGoal(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	if err := deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		systemDB, err := txn.Descriptors().ByID(txn.KV()).Get().Database(ctx, keys.SystemDatabaseID)
		if err != nil {
			return err
		}
		if !systemDB.IsMultiRegion() {
			return nil
		}
		// Configure the system database as survive region, we will automatically
		// leave regional by row tables as survive zone.
		_, err = txn.ExecEx(ctx, "alter-database-survival-goal", txn.KV(), /* txn */
			sessiondata.NodeUserSessionDataOverride, `
ALTER DATABASE system SURVIVE REGION FAILURE;
`)
		return err
	}); err != nil {
		return err
	}
	return bumpSystemDatabaseSchemaVersion(ctx, cv, deps)
}
