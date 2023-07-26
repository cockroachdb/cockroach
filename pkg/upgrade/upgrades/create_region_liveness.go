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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// createRegionLivenessTables creates the system.region_liveness table.
func createRegionLivenessTables(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	setDBLocality := false
	// Since this is a re-use of an old key space, invalid data might exists,
	// so lets clear it out first.
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		systemDB, err := txn.Descriptors().ByID(txn.KV()).WithoutNonPublic().Get().Database(ctx, keys.SystemDatabaseID)
		if err != nil {
			return err
		}
		if systemDB.IsMultiRegion() {
			setDBLocality = true
		}
		tablePrefix := d.Codec.TablePrefix(keys.RegionLivenessTableID)
		_, err = txn.KV().DelRange(ctx, tablePrefix, tablePrefix.PrefixEnd(), false)
		return err
	}); err != nil {
		return err
	}

	regionLivenessBuilder := tabledesc.NewBuilder(systemschema.RegionLivenessTable.TableDesc())
	regionLivenessMutable := regionLivenessBuilder.BuildExistingMutableTable()
	if setDBLocality {
		regionLivenessMutable.SetTableLocalityGlobal()
	}
	if err := createSystemTable(ctx, d.DB.KV(), d.Settings, d.Codec,
		regionLivenessMutable); err != nil {
		return err
	}

	// After creating this table, we need to set locality on it if the system
	// database is already multi-region.
	if setDBLocality {
		// Additionally, we need to set type of the region column
		if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			systemDB, err := txn.Descriptors().ByID(txn.KV()).WithoutNonPublic().Get().Database(ctx, keys.SystemDatabaseID)
			if err != nil {
				return err
			}
			regionEnumID, err := systemDB.MultiRegionEnumID()
			if err != nil {
				return err
			}

			tbl, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, keys.RegionLivenessTableID)
			if err != nil {
				return err
			}

			column := catalog.FindColumnByName(tbl, "crdb_region")
			if column == nil {
				return errors.AssertionFailedf("unable to find region column")
			}

			enumTypeDesc, err := txn.Descriptors().MutableByID(txn.KV()).Type(ctx, regionEnumID)
			if err != nil {
				return err
			}
			column.ColumnDesc().Type = enumTypeDesc.AsTypesT()
			// Add a back reference to the table
			enumTypeDesc.AddReferencingDescriptorID(tbl.ID)

			if err := txn.Descriptors().WriteDesc(ctx, false /* kvTrace*/, tbl, txn.KV()); err != nil {
				return err
			}
			return txn.Descriptors().WriteDesc(ctx, false /* kvTrace*/, enumTypeDesc, txn.KV())
		}); err != nil {
			return err
		}
	}

	return nil
}
