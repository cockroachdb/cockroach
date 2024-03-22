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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
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
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// A subset of tables are missing zone config inheritance, so repair their
	// zone config values.
	if deps.Codec.ForSystemTenant() {
		if err := deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			ba := txn.KV().NewBatch()
			for _, descID := range []descpb.ID{systemschema.ReplicationConstraintStatsTable.GetID(),
				systemschema.ReplicationStatsTable.GetID(),
				systemschema.TenantUsageTable.GetID()} {
				zoneCfg, err := txn.Descriptors().GetZoneConfig(ctx, txn.KV(), descID)
				if err != nil {
					return err
				}
				if zoneCfg == nil {
					continue
				}
				// Write the update zone config out with inheritance set to the
				// defaults.
				zoneCfg.ZoneConfigProto().InheritedConstraints = true
				zoneCfg.ZoneConfigProto().InheritedLeasePreferences = true
				if err := txn.Descriptors().WriteZoneConfigToBatch(ctx,
					false, /*kvTrace*/
					ba,
					descID,
					zoneCfg); err != nil {
					return err
				}
			}
			return txn.KV().Run(ctx, ba)
		}); err != nil {
			return err
		}

		// Setup the GC TTL on the system.lease table if one has not been
		// set yet.
		if err := deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			zoneCfg, err := txn.Descriptors().GetZoneConfig(ctx, txn.KV(), keys.LeaseTableID)
			if err != nil {
				return err
			}
			if zoneCfg == nil ||
				zoneCfg.ZoneConfigProto().GC == nil {
				_, err := txn.ExecEx(ctx,
					"setup-lease-table-ttl",
					txn.KV(), /* txn */
					sessiondata.NodeUserSessionDataOverride,
					"ALTER TABLE system.lease  CONFIGURE ZONE USING gc.ttlseconds=600")
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}
	return bumpSystemDatabaseSchemaVersion(ctx, cv, deps)
}
