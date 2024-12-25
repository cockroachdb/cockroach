// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// databaseRegionChangeFinalizer encapsulates the logic and state for finalizing
// a region metadata operation on a multi-region database. This includes methods
// to update partitions and zone configurations as well as leases on REGIONAL BY
// ROW tables.
type databaseRegionChangeFinalizer struct {
	dbID   descpb.ID
	typeID descpb.ID

	localPlanner        *planner
	cleanupFunc         func()
	regionalByRowTables []*tabledesc.Mutable
}

// newDatabaseRegionChangeFinalizer returns a databaseRegionChangeFinalizer.
// It pre-fetches all REGIONAL BY ROW tables from the database.
func newDatabaseRegionChangeFinalizer(
	ctx context.Context, txn descs.Txn, execCfg *ExecutorConfig, dbID descpb.ID, typeID descpb.ID,
) (*databaseRegionChangeFinalizer, error) {
	p, cleanup := NewInternalPlanner(
		"repartition-regional-by-row-tables",
		txn.KV(),
		username.NodeUserName(),
		&MemoryMetrics{},
		execCfg,
		txn.SessionData(),
		WithDescCollection(txn.Descriptors()),
	)
	localPlanner := p.(*planner)

	var regionalByRowTables []*tabledesc.Mutable
	if err := func() error {
		dbDesc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, dbID)
		if err != nil {
			return err
		}

		return localPlanner.forEachMutableTableInDatabase(
			ctx,
			dbDesc,
			func(ctx context.Context, scName string, tableDesc *tabledesc.Mutable) error {
				if !tableDesc.IsLocalityRegionalByRow() || tableDesc.Dropped() {
					// We only need to re-partition REGIONAL BY ROW tables. Even then, we
					// don't need to (can't) repartition a REGIONAL BY ROW table if it has
					// been dropped.
					return nil
				}
				regionalByRowTables = append(regionalByRowTables, tableDesc)
				return nil
			},
		)
	}(); err != nil {
		cleanup()
		return nil, err
	}

	return &databaseRegionChangeFinalizer{
		dbID:                dbID,
		typeID:              typeID,
		localPlanner:        localPlanner,
		cleanupFunc:         cleanup,
		regionalByRowTables: regionalByRowTables,
	}, nil
}

// cleanup cleans up remaining objects on the databaseRegionChangeFinalizer.
func (r *databaseRegionChangeFinalizer) cleanup() {
	if r.cleanupFunc != nil {
		r.cleanupFunc()
		r.cleanupFunc = nil
	}
}

// finalize updates the zone configurations of the database and all enclosed
// REGIONAL BY ROW tables once the region promotion/demotion is complete.
func (r *databaseRegionChangeFinalizer) finalize(ctx context.Context, txn descs.Txn) error {
	if err := r.updateDatabaseZoneConfig(ctx, txn); err != nil {
		return err
	}
	if err := r.preDrop(ctx, txn); err != nil {
		return err
	}
	return r.updateGlobalTablesZoneConfig(ctx, txn)
}

// preDrop is called in advance of dropping regions from a multi-region
// database. This function just re-partitions the REGIONAL BY ROW tables in
// advance of the type descriptor change, to ensure that the table and type
// descriptors never become incorrect (from a query perspective). For more info,
// see the callers.
func (r *databaseRegionChangeFinalizer) preDrop(ctx context.Context, txn descs.Txn) error {
	repartitioned, zoneConfigUpdates, err := r.repartitionRegionalByRowTables(ctx, txn)
	if err != nil {
		return err
	}
	for _, update := range zoneConfigUpdates {
		if _, err := writeZoneConfigUpdate(
			ctx, txn,
			r.localPlanner.ExtendedEvalContext().Tracing.KVTracingEnabled(),
			update,
		); err != nil {
			return err
		}
	}
	b := txn.KV().NewBatch()
	for _, t := range repartitioned {
		const kvTrace = false
		if err := r.localPlanner.Descriptors().WriteDescToBatch(
			ctx, kvTrace, t, b,
		); err != nil {
			return err
		}
	}
	return txn.KV().Run(ctx, b)
}

// updateGlobalTablesZoneConfig refreshes all global tables' zone configs so
// that their zone configs are refreshes after a newly-added region goes out of
// being a transitioning region. This function only applies if the database is
// in PLACEMENT RESTRICTED because if the database is in PLACEMENT DEFAULT, it
// will inherit the database's constraints. In the RESTRICTED case, however,
// constraints must be explicitly refreshed when new regions are added/removed.
func (r *databaseRegionChangeFinalizer) updateGlobalTablesZoneConfig(
	ctx context.Context, txn isql.Txn,
) error {
	regionConfig, err := SynthesizeRegionConfig(ctx, txn.KV(), r.dbID, r.localPlanner.Descriptors())
	if err != nil {
		return err
	}
	// If we're not in PLACEMENT RESTRICTED, GLOBAL tables will inherit the
	// database zone config. Therefore, their constraints do not have to be
	// refreshed.
	if !regionConfig.IsPlacementRestricted() {
		return nil
	}

	descsCol := r.localPlanner.Descriptors()

	dbDesc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, r.dbID)
	if err != nil {
		return err
	}

	err = r.localPlanner.refreshZoneConfigsForTables(ctx, dbDesc, WithOnlyGlobalTables)
	if err != nil {
		return err
	}

	return nil
}

// updateDatabaseZoneConfig updates the zone config of the database that
// encloses the multi-region enum such that there is an entry for all PUBLIC
// region values.
func (r *databaseRegionChangeFinalizer) updateDatabaseZoneConfig(
	ctx context.Context, txn descs.Txn,
) error {
	regionConfig, err := SynthesizeRegionConfig(ctx, txn.KV(), r.dbID, r.localPlanner.Descriptors())
	if err != nil {
		return err
	}
	return ApplyZoneConfigFromDatabaseRegionConfig(
		ctx,
		r.dbID,
		regionConfig,
		txn,
		r.localPlanner.ExecCfg(),
		true, /* validateLocalities */
		r.localPlanner.extendedEvalCtx.Tracing.KVTracingEnabled(),
	)
}

// repartitionRegionalByRowTables re-partitions all REGIONAL BY ROW tables
// contained in the database. repartitionRegionalByRowTables adds a partition
// and corresponding zone configuration for all PUBLIC enum members (regions)
// on the multi-region enum.
//
// Note that even if the caller does not write the returned descriptors, the
// mutable copies of the descriptor in the collection has been modified and is
// being returned. This allows callers to inject the descriptors into a
// collection in order to observe the side- effects of such a change. The caller
// is responsible for actually writing the repartitioned tables. To re-iterate,
// when a mutable descriptor is resolved from a collection subsequently, the
// exact same descriptor object is returned. All of the objects descriptors
// mutated here are from the underlying collection. However, these descriptors
// have not been added back to the collection using AddUncommittedDescriptor
// (or its friends WriteDesc.*), so immutable resolution of the descriptors
// will still yield the original, unmodified version. If users want these
// modified versions to be visible for immutable resolution, they must either
// write the descriptors through the collection or inject them as synthetic
// descriptors.
func (r *databaseRegionChangeFinalizer) repartitionRegionalByRowTables(
	ctx context.Context, txn descs.Txn,
) (repartitioned []*tabledesc.Mutable, zoneConfigUpdates []*zoneConfigUpdate, _ error) {
	var regionConfigOpts []multiregion.SynthesizeRegionConfigOption
	// For regional by row tables these will be forced as survive zone on
	// the system database, even if the system database is survive region
	if r.dbID == keys.SystemDatabaseID {
		regionConfigOpts = []multiregion.SynthesizeRegionConfigOption{
			multiregion.SynthesizeRegionConfigOptionForceSurvivalZone,
		}
	}

	regionConfig, err := SynthesizeRegionConfig(ctx, txn.KV(), r.dbID, r.localPlanner.Descriptors(), regionConfigOpts...)
	if err != nil {
		return nil, nil, err
	}

	for _, tableDesc := range r.regionalByRowTables {
		// Since we hydrated the columns with the old enum, and now that the enum
		// has transitioned the read-only members to public, we have to re-hydrate
		// the table descriptor with the new type metadata.
		for i := range tableDesc.Columns {
			col := &tableDesc.Columns[i]
			if col.Type.UserDefined() {
				tid := typedesc.UserDefinedTypeOIDToID(col.Type.Oid())
				if tid == r.typeID {
					col.Type.TypeMeta = types.UserDefinedTypeMetadata{}
				}
			}
		}
		if err := typedesc.HydrateTypesInDescriptor(
			ctx, tableDesc, r.localPlanner,
		); err != nil {
			return nil, nil, err
		}

		colName, err := tableDesc.GetRegionalByRowTableRegionColumnName()
		if err != nil {
			return nil, nil, err
		}
		partitionAllBy := multiregion.PartitionByForRegionalByRow(regionConfig, colName)

		// oldPartitionings saves the old partitionings for each
		// index that is repartitioned. This is later used to remove zone
		// configurations from any partitions that are removed.
		oldPartitionings := make(map[descpb.IndexID]catalog.Partitioning)

		// Update the partitioning on all indexes of the table that aren't being
		// dropped.
		for _, index := range tableDesc.NonDropIndexes() {
			oldPartitionings[index.GetID()] = index.GetPartitioning().DeepCopy()
			newImplicitCols, newPartitioning, err := CreatePartitioning(
				ctx,
				r.localPlanner.extendedEvalCtx.Settings,
				r.localPlanner.EvalContext(),
				tableDesc,
				*index.IndexDesc(),
				partitionAllBy,
				nil,  /* allowedNewColumnName*/
				true, /* allowImplicitPartitioning */
			)
			if err != nil {
				return nil, nil, err
			}
			tabledesc.UpdateIndexPartitioning(index.IndexDesc(), index.Primary(), newImplicitCols, newPartitioning)
		}

		// Update the zone configurations now that the partition's been added.
		update, err := prepareZoneConfigForMultiRegionTable(
			ctx,
			txn,
			r.localPlanner.ExecCfg(),
			regionConfig,
			tableDesc,
			ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
		)
		if err != nil {
			return nil, nil, err
		}
		if update != nil {
			zoneConfigUpdates = append(zoneConfigUpdates, update)
		}
		repartitioned = append(repartitioned, tableDesc)
	}

	return repartitioned, zoneConfigUpdates, nil
}
