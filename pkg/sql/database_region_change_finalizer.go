// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// databaseRegionChangeFinalizer encapsulates the logic and state for finalizing
// a region metadata operation on a multi-region database. This includes methods
// to update partitions and zone configurations as well as leases on REGIONAL BY
// ROW tables.
type databaseRegionChangeFinalizer struct {
	dbID                descpb.ID
	typeID              descpb.ID
	regionalByRowTables []descpb.ID
}

// newDatabaseRegionChangeFinalizer returns a databaseRegionChangeFinalizer.
func newDatabaseRegionChangeFinalizer(
	dbID descpb.ID, typeID descpb.ID,
) *databaseRegionChangeFinalizer {
	return &databaseRegionChangeFinalizer{
		dbID:   dbID,
		typeID: typeID,
	}
}

// finalize updates the zone configurations of the database and all enclosed
// REGIONAL BY ROW tables once the region promotion/demotion is complete. The
// caller must call waitToUpdateLeases once the provided transaction commits.
func (r *databaseRegionChangeFinalizer) finalize(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, execCfg *ExecutorConfig,
) error {
	if err := r.updateDatabaseZoneConfig(ctx, txn, descsCol, execCfg); err != nil {
		return err
	}
	return r.repartitionRegionalByRowTables(ctx, txn, descsCol, execCfg)
}

// updateDatabaseZoneConfig updates the zone config of the database that
// encloses the multi-region enum such that there is an entry for all PUBLIC
// region values.
func (r *databaseRegionChangeFinalizer) updateDatabaseZoneConfig(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, execCfg *ExecutorConfig,
) error {

	regionConfig, err := SynthesizeRegionConfig(ctx, txn, r.dbID, descsCol)
	if err != nil {
		return err
	}
	return ApplyZoneConfigFromDatabaseRegionConfig(
		ctx,
		r.dbID,
		regionConfig,
		txn,
		execCfg,
	)
}

// repartitionRegionalByRowTables re-partitions all REGIONAL BY ROW tables
// contained in the database. repartitionRegionalByRowTables adds a partition
// and corresponding zone configuration for all PUBLIC enum members (regions)
// on the multi-region enum.
func (r *databaseRegionChangeFinalizer) repartitionRegionalByRowTables(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, execCfg *ExecutorConfig,
) error {
	var repartitionedTableIDs []descpb.ID

	p, cleanup := NewInternalPlanner(
		"repartition-regional-by-row-tables",
		txn,
		security.RootUserName(),
		&MemoryMetrics{},
		execCfg,
		sessiondatapb.SessionData{},
		WithDescCollection(descsCol),
	)
	defer cleanup()
	localPlanner := p.(*planner)

	_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
		ctx, txn, r.dbID, tree.DatabaseLookupFlags{
			Required: true,
		})
	if err != nil {
		return err
	}

	b := txn.NewBatch()
	regionConfig, err := SynthesizeRegionConfig(ctx, txn, r.dbID, descsCol)
	if err != nil {
		return err
	}

	err = localPlanner.forEachMutableTableInDatabase(ctx, dbDesc,
		func(ctx context.Context, tableDesc *tabledesc.Mutable) error {
			if !tableDesc.IsLocalityRegionalByRow() || tableDesc.Dropped() {
				// We only need to re-partition REGIONAL BY ROW tables. Even then, we
				// don't need to (can't) repartition a REGIONAL BY ROW table if it has
				// been dropped.
				return nil
			}

			colName, err := tableDesc.GetRegionalByRowTableRegionColumnName()
			if err != nil {
				return err
			}
			partitionAllBy := partitionByForRegionalByRow(regionConfig, colName)

			// oldPartitioningDescs saves the old partitioning descriptors for each
			// index that is repartitioned. This is later used to remove zone
			// configurations from any partitions that are removed.
			oldPartitioningDescs := make(map[descpb.IndexID]descpb.PartitioningDescriptor)

			// Update the partitioning on all indexes of the table that aren't being
			// dropped.
			for _, index := range tableDesc.NonDropIndexes() {
				newIdx, err := CreatePartitioning(
					ctx,
					localPlanner.extendedEvalCtx.Settings,
					localPlanner.EvalContext(),
					tableDesc,
					*index.IndexDesc(),
					partitionAllBy,
					nil,  /* allowedNewColumnName*/
					true, /* allowImplicitPartitioning */
				)
				if err != nil {
					return err
				}

				oldPartitioningDescs[index.GetID()] = index.IndexDesc().Partitioning

				// Update the index descriptor proto's partitioning.
				index.IndexDesc().Partitioning = newIdx.Partitioning
			}

			// Remove zone configurations that applied to partitions that were removed
			// in the previous step. This requires all indexes to have been
			// repartitioned such that there is no partitioning on the removed enum
			// value. This is because `deleteRemovedPartitionZoneConfigs` generates
			// subzone spans for the entire table (all indexes) downstream for each
			// index. Spans can only be generated if partitioning values are present on
			// the type descriptor (removed enum values obviously aren't), so we must
			// remove the partition from all indexes before trying to delete zone
			// configurations.
			for _, index := range tableDesc.NonDropIndexes() {
				oldPartitioning := oldPartitioningDescs[index.GetID()]

				// Remove zone configurations that reference partition values we removed
				// in the previous step.
				if err = deleteRemovedPartitionZoneConfigs(
					ctx,
					txn,
					tableDesc,
					index.IndexDesc(),
					&oldPartitioning,
					&index.IndexDesc().Partitioning,
					execCfg,
				); err != nil {
					return err
				}
			}

			// Update the zone configurations now that the partition's been added.
			if err := ApplyZoneConfigForMultiRegionTable(
				ctx,
				txn,
				localPlanner.ExecCfg(),
				regionConfig,
				tableDesc,
				ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
			); err != nil {
				return err
			}

			if err := localPlanner.Descriptors().WriteDescToBatch(ctx, false /* kvTrace */, tableDesc, b); err != nil {
				return err
			}

			repartitionedTableIDs = append(repartitionedTableIDs, tableDesc.GetID())
			return nil
		})
	if err != nil {
		return err
	}

	if err := txn.Run(ctx, b); err != nil {
		return err
	}

	r.regionalByRowTables = repartitionedTableIDs

	return nil
}

// waitToUpdateLeases ensures that the entire cluster has been updated to the
// latest descriptor version for all regional by row tables that were previously
// repartitioned.
func (r *databaseRegionChangeFinalizer) waitToUpdateLeases(
	ctx context.Context, leaseMgr *lease.Manager,
) error {
	for _, tbID := range r.regionalByRowTables {
		if err := WaitToUpdateLeases(ctx, leaseMgr, tbID); err != nil {
			if !errors.Is(err, catalog.ErrDescriptorNotFound) {
				return err
			}
			// Swallow.
			log.Infof(ctx,
				"could not find table %d to be repartitioned when adding/removing regions on "+
					"enum %d, assuming it was dropped and moving on",
				tbID,
				r.typeID,
			)
		}
	}
	return nil
}
