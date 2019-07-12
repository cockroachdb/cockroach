// Copyright 2018 The Cockroach Authors.
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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// optCatalog implements the cat.Catalog interface over the SchemaResolver
// interface for the use of the new optimizer. The interfaces are simplified to
// only include what the optimizer needs, and certain common lookups are cached
// for faster performance.
type optCatalog struct {
	// planner needs to be set via a call to init before calling other methods.
	planner *planner

	// cfg is the gossiped and cached system config. It may be nil if the node
	// does not yet have it available.
	cfg *config.SystemConfig

	// dataSources is a cache of table and view objects that's used to satisfy
	// repeated calls for the same data source.
	// Note that the data source object might still need to be recreated if
	// something outside of the descriptor has changed (e.g. table stats).
	dataSources map[*sqlbase.ImmutableTableDescriptor]cat.DataSource

	// tn is a temporary name used during resolution to avoid heap allocation.
	tn tree.TableName
}

var _ cat.Catalog = &optCatalog{}

// init initializes an optCatalog instance (which the caller can pre-allocate).
// The instance can be used across multiple queries, but reset() should be
// called for each query.
func (oc *optCatalog) init(planner *planner) {
	oc.planner = planner
	oc.dataSources = make(map[*sqlbase.ImmutableTableDescriptor]cat.DataSource)
}

// reset prepares the optCatalog to be used for a new query.
func (oc *optCatalog) reset() {
	// If we have accumulated too many tables in our map, throw everything away.
	// This deals with possible edge cases where we do a lot of DDL in a
	// long-lived session.
	if len(oc.dataSources) > 100 {
		oc.dataSources = make(map[*sqlbase.ImmutableTableDescriptor]cat.DataSource)
	}

	// Gossip can be nil in testing scenarios.
	if oc.planner.execCfg.Gossip != nil {
		oc.cfg = oc.planner.execCfg.Gossip.GetSystemConfig()
	}
}

// optSchema is a wrapper around sqlbase.DatabaseDescriptor that implements the
// cat.Object and cat.Schema interfaces.
type optSchema struct {
	planner *planner
	desc    *sqlbase.DatabaseDescriptor

	name cat.SchemaName
}

// ID is part of the cat.Object interface.
func (os *optSchema) ID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// Equals is part of the cat.Object interface.
func (os *optSchema) Equals(other cat.Object) bool {
	otherSchema, ok := other.(*optSchema)
	return ok && os.desc.ID == otherSchema.desc.ID
}

// Name is part of the cat.Schema interface.
func (os *optSchema) Name() *cat.SchemaName {
	return &os.name
}

// GetDataSourceNames is part of the cat.Schema interface.
func (os *optSchema) GetDataSourceNames(ctx context.Context) ([]cat.DataSourceName, error) {
	return GetObjectNames(
		ctx, os.planner.Txn(), os.planner, os.desc,
		os.name.Schema(),
		true, /* explicitPrefix */
	)
}

// ResolveSchema is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveSchema(
	ctx context.Context, flags cat.Flags, name *cat.SchemaName,
) (cat.Schema, cat.SchemaName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}

	// ResolveTargetObject wraps ResolveTarget in order to raise "schema not
	// found" and "schema cannot be modified" errors. However, ResolveTargetObject
	// assumes that a data source object is being resolved, which is not the case
	// for ResolveSchema. Therefore, call ResolveTarget directly and produce a
	// more general error.
	oc.tn.TableName = ""
	oc.tn.TableNamePrefix = *name
	found, desc, err := oc.tn.ResolveTarget(
		ctx,
		oc.planner,
		oc.planner.CurrentDatabase(),
		oc.planner.CurrentSearchPath(),
	)
	if err != nil {
		return nil, cat.SchemaName{}, err
	}
	if !found {
		if !name.ExplicitSchema && !name.ExplicitCatalog {
			return nil, cat.SchemaName{}, pgerror.New(
				pgcode.InvalidName, "no database specified",
			)
		}
		return nil, cat.SchemaName{}, pgerror.Newf(
			pgcode.InvalidSchemaName, "target database or schema does not exist",
		)
	}
	return &optSchema{
		planner: oc.planner,
		desc:    desc.(*DatabaseDescriptor),
		name:    oc.tn.TableNamePrefix,
	}, oc.tn.TableNamePrefix, nil
}

// ResolveDataSource is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSource(
	ctx context.Context, flags cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}

	oc.tn = *name
	desc, err := ResolveExistingObject(ctx, oc.planner, &oc.tn, true /* required */, ResolveAnyDescType)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	ds, err := oc.dataSourceForDesc(ctx, flags, desc, &oc.tn)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	return ds, oc.tn, nil
}

// ResolveDataSourceByID is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSourceByID(
	ctx context.Context, dataSourceID cat.StableID,
) (cat.DataSource, error) {
	tableLookup, err := oc.planner.LookupTableByID(ctx, sqlbase.ID(dataSourceID))

	if err != nil || tableLookup.IsAdding {
		if err == sqlbase.ErrDescriptorNotFound || tableLookup.IsAdding {
			return nil, sqlbase.NewUndefinedRelationError(&tree.TableRef{TableID: int64(dataSourceID)})
		}
		return nil, err
	}
	desc := tableLookup.Desc

	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, oc.planner.Txn(), desc.ParentID)
	if err != nil {
		return nil, err
	}

	name := tree.MakeTableName(tree.Name(dbDesc.Name), tree.Name(desc.Name))
	return oc.dataSourceForDesc(ctx, cat.Flags{}, desc, &name)
}

// CheckPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
	switch t := o.(type) {
	case *optSchema:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	case *optTable:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	case *optView:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	case *optSequence:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	default:
		return errors.AssertionFailedf("invalid object type: %T", o)
	}
}

// CheckAnyPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckAnyPrivilege(ctx context.Context, o cat.Object) error {
	switch t := o.(type) {
	case *optSchema:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	case *optTable:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	case *optView:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	case *optSequence:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	default:
		return errors.AssertionFailedf("invalid object type: %T", o)
	}
}

// IsSuperUser is part of the cat.Catalog interface.
func (oc *optCatalog) IsSuperUser(ctx context.Context, action string) (bool, error) {
	return oc.planner.IsSuperUser(ctx, action)
}

// RequireSuperUser is part of the cat.Catalog interface.
func (oc *optCatalog) RequireSuperUser(ctx context.Context, action string) error {
	return oc.planner.RequireSuperUser(ctx, action)
}

// dataSourceForDesc returns a data source wrapper for the given descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) dataSourceForDesc(
	ctx context.Context,
	flags cat.Flags,
	desc *sqlbase.ImmutableTableDescriptor,
	name *cat.DataSourceName,
) (cat.DataSource, error) {
	if desc.IsTable() {
		// Tables require invalidation logic for cached wrappers.
		return oc.dataSourceForTable(ctx, flags, desc, name)
	}

	ds, ok := oc.dataSources[desc]
	if ok {
		return ds, nil
	}

	switch {
	case desc.IsView():
		ds = newOptView(desc, name)

	case desc.IsSequence():
		ds = newOptSequence(desc, name)

	default:
		return nil, errors.AssertionFailedf("unexpected table descriptor: %+v", desc)
	}

	oc.dataSources[desc] = ds
	return ds, nil
}

// dataSourceForTable returns a table data source wrapper for the given descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) dataSourceForTable(
	ctx context.Context,
	flags cat.Flags,
	desc *sqlbase.ImmutableTableDescriptor,
	name *cat.DataSourceName,
) (cat.DataSource, error) {
	// Even if we have a cached data source, we still have to cross-check that
	// statistics and the zone config haven't changed.
	var tableStats []*stats.TableStatistic
	if !flags.NoTableStats {
		var err error
		tableStats, err = oc.planner.execCfg.TableStatsCache.GetTableStats(context.TODO(), desc.ID)
		if err != nil {
			// Ignore any error. We still want to be able to run queries even if we lose
			// access to the statistics table.
			// TODO(radu): at least log the error.
			tableStats = nil
		}
	}

	zoneConfig, err := oc.getZoneConfig(desc)
	if err != nil {
		return nil, err
	}

	// Check to see if there's already a data source wrapper for this descriptor,
	// and it was created with the same stats and zone config.
	if ds, ok := oc.dataSources[desc]; ok && !ds.(*optTable).isStale(tableStats, zoneConfig) {
		return ds, nil
	}

	id := cat.StableID(desc.ID)
	if desc.IsVirtualTable() {
		// A virtual table can effectively have multiple instances, with different
		// contents. For example `db1.pg_catalog.pg_sequence` contains info about
		// sequences in db1, whereas `db2.pg_catalog.pg_sequence` contains info
		// about sequences in db2.
		//
		// These instances should have different stable IDs. To achieve this, we
		// prepend the database ID.
		//
		// Note that some virtual tables have a special instance with empty catalog,
		// for example "".information_schema.tables contains info about tables in
		// all databases. We treat the empty catalog as having database ID 0.
		if name.Catalog() != "" {
			// TODO(radu): it's unfortunate that we have to lookup the schema again.
			_, dbDesc, err := oc.planner.LookupSchema(ctx, name.Catalog(), name.Schema())
			if err != nil {
				return nil, err
			}
			if dbDesc == nil {
				// The database was not found. This can happen e.g. when
				// accessing a virtual schema over a non-existent
				// database. This is a common scenario when the current db
				// in the session points to a database that was not created
				// yet.
				//
				// In that case we use an invalid database ID. We
				// distinguish this from the empty database case because the
				// virtual tables do not "contain" the same information in
				// both cases.
				id |= cat.StableID(math.MaxUint32) << 32
			} else {
				id |= cat.StableID(dbDesc.(*DatabaseDescriptor).ID) << 32
			}
		}
	}

	ds, err := newOptTable(ctx, desc, id, name, tableStats, zoneConfig)
	if err != nil {
		return nil, err
	}
	if !desc.IsVirtualTable() {
		// Virtual tables can have multiple effective instances that utilize the
		// same descriptor (see above).
		oc.dataSources[desc] = ds
	}
	return ds, nil
}

var emptyZoneConfig = &config.ZoneConfig{}

// getZoneConfig returns the ZoneConfig data structure for the given table.
// ZoneConfigs are stored in protobuf binary format in the SystemConfig, which
// is gossiped around the cluster. Note that the returned ZoneConfig might be
// somewhat stale, since it's taken from the gossiped SystemConfig.
func (oc *optCatalog) getZoneConfig(
	desc *sqlbase.ImmutableTableDescriptor,
) (*config.ZoneConfig, error) {
	// Lookup table's zone if system config is available (it may not be as node
	// is starting up and before it's received the gossiped config). If it is
	// not available, use an empty config that has no zone constraints.
	if oc.cfg == nil || desc.IsVirtualTable() {
		return emptyZoneConfig, nil
	}
	zone, err := oc.cfg.GetZoneConfigForObject(uint32(desc.ID))
	if err != nil {
		return nil, err
	}
	if zone == nil {
		// This can happen with tests that override the hook.
		zone = emptyZoneConfig
	}
	return zone, err
}

// optView is a wrapper around sqlbase.ImmutableTableDescriptor that implements
// the cat.Object, cat.DataSource, and cat.View interfaces.
type optView struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of
	// the view.
	name cat.DataSourceName
}

var _ cat.View = &optView{}

func newOptView(desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName) *optView {
	ov := &optView{desc: desc, name: *name}

	// The cat.View interface requires that view names be fully qualified.
	ov.name.ExplicitSchema = true
	ov.name.ExplicitCatalog = true

	return ov
}

// ID is part of the cat.Object interface.
func (ov *optView) ID() cat.StableID {
	return cat.StableID(ov.desc.ID)
}

// Equals is part of the cat.Object interface.
func (ov *optView) Equals(other cat.Object) bool {
	otherView, ok := other.(*optView)
	if !ok {
		return false
	}
	return ov.desc.ID == otherView.desc.ID && ov.desc.Version == otherView.desc.Version
}

// Name is part of the cat.View interface.
func (ov *optView) Name() *cat.DataSourceName {
	return &ov.name
}

// Query is part of the cat.View interface.
func (ov *optView) Query() string {
	return ov.desc.ViewQuery
}

// ColumnNameCount is part of the cat.View interface.
func (ov *optView) ColumnNameCount() int {
	return len(ov.desc.Columns)
}

// ColumnName is part of the cat.View interface.
func (ov *optView) ColumnName(i int) tree.Name {
	return tree.Name(ov.desc.Columns[i].Name)
}

// optSequence is a wrapper around sqlbase.ImmutableTableDescriptor that
// implements the cat.Object and cat.DataSource interfaces.
type optSequence struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of the
	// sequence.
	name cat.DataSourceName
}

var _ cat.DataSource = &optSequence{}
var _ cat.Sequence = &optSequence{}

func newOptSequence(desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName) *optSequence {
	os := &optSequence{desc: desc, name: *name}

	// The cat.Sequence interface requires that table names be fully qualified.
	os.name.ExplicitSchema = true
	os.name.ExplicitCatalog = true

	return os
}

// ID is part of the cat.Object interface.
func (os *optSequence) ID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// Equals is part of the cat.Object interface.
func (os *optSequence) Equals(other cat.Object) bool {
	otherSeq, ok := other.(*optSequence)
	if !ok {
		return false
	}
	return os.desc.ID == otherSeq.desc.ID && os.desc.Version == otherSeq.desc.Version
}

// Name is part of the cat.DataSource interface.
func (os *optSequence) Name() *cat.DataSourceName {
	return &os.name
}

// SequenceName is part of the cat.Sequence interface.
func (os *optSequence) SequenceName() *tree.TableName {
	return os.Name()
}

// optTable is a wrapper around sqlbase.ImmutableTableDescriptor that caches
// index wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// This is the descriptor ID, except for virtual tables.
	id cat.StableID

	// name is the fully qualified, fully resolved, fully normalized name of the
	// table.
	name cat.DataSourceName

	// indexes are the inlined wrappers for the table's primary and secondary
	// indexes.
	indexes []optIndex

	// rawStats stores the original table statistics slice. Used for a fast-path
	// check that the statistics haven't changed.
	rawStats []*stats.TableStatistic

	// stats are the inlined wrappers for table statistics.
	stats []optTableStat

	zone *config.ZoneConfig

	// family is the inlined wrapper for the table's primary family. The primary
	// family is the first family explicitly specified by the user. If no families
	// were explicitly specified, then the primary family is synthesized.
	primaryFamily optFamily

	// families are the inlined wrappers for the table's non-primary families,
	// which are all the families specified by the user after the first. The
	// primary family is kept separate since the common case is that there's just
	// one family.
	families []optFamily

	outboundFKs []optForeignKeyConstraint
	inboundFKs  []optForeignKeyConstraint

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int
}

var _ cat.Table = &optTable{}

func newOptTable(
	ctx context.Context,
	desc *sqlbase.ImmutableTableDescriptor,
	id cat.StableID,
	name *cat.DataSourceName,
	stats []*stats.TableStatistic,
	tblZone *config.ZoneConfig,
) (*optTable, error) {
	ot := &optTable{
		desc:     desc,
		id:       id,
		name:     *name,
		rawStats: stats,
		zone:     tblZone,
	}

	// The cat.Table interface requires that table names be fully qualified.
	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	// Create the table's column mapping from sqlbase.ColumnID to column ordinal.
	ot.colMap = make(map[sqlbase.ColumnID]int, ot.DeletableColumnCount())
	for i, n := 0, ot.DeletableColumnCount(); i < n; i++ {
		ot.colMap[sqlbase.ColumnID(ot.Column(i).ColID())] = i
	}

	if !ot.desc.IsVirtualTable() {
		// Build the indexes (add 1 to account for lack of primary index in
		// DeletableIndexes slice).
		ot.indexes = make([]optIndex, 1+len(ot.desc.DeletableIndexes()))

		for i := range ot.indexes {
			var idxDesc *sqlbase.IndexDescriptor
			if i == 0 {
				idxDesc = &desc.PrimaryIndex
			} else {
				idxDesc = &ot.desc.DeletableIndexes()[i-1]
			}

			// If there is a subzone that applies to the entire index, use that,
			// else use the table zone. Skip subzones that apply to partitions,
			// since they apply only to a subset of the index.
			idxZone := tblZone
			for j := range tblZone.Subzones {
				subzone := &tblZone.Subzones[j]
				if subzone.IndexID == uint32(idxDesc.ID) && subzone.PartitionName == "" {
					copyZone := subzone.Config
					copyZone.InheritFromParent(tblZone)
					idxZone = &copyZone
				}
			}

			ot.indexes[i].init(ot, i, idxDesc, idxZone)
		}

		for _, fk := range ot.desc.OutboundFKs {
			originIdx, err := sqlbase.FindFKOriginIndex(ot.desc.TableDesc(), fk.OriginColumnIDs)
			if err != nil {
				return nil, err
			}
			referencedIdx, err := sqlbase.FindFKReferencedIndex(ot.desc.TableDesc(), fk.ReferencedColumnIDs)
			if err != nil {
				return nil, err
			}
			ot.outboundFKs = append(ot.outboundFKs, optForeignKeyConstraint{
				name:            fk.Name,
				originTable:     ot.id,
				originIndex:     originIdx.ID,
				referencedTable: cat.StableID(fk.ReferencedTableID),
				referencedIndex: referencedIdx.ID,
				numCols:         int(len(fk.OriginColumnIDs)),
				validity:        fk.Validity,
				match:           fk.Match,
			})
		}
	}

	for _, ref := range ot.desc.InboundFKs {
		originIdx, err := sqlbase.FindFKOriginIndex(ot.desc.TableDesc(), ref.OriginColumnIDs)
		if err != nil {
			return nil, err
		}
		referencedIdx, err := sqlbase.FindFKReferencedIndex(ot.desc.TableDesc(), ref.ReferencedColumnIDs)
		if err != nil {
			return nil, err
		}
		fk, err := ot.desc.FindFKForBackRef(ot.desc.ID, ref)
		if err != nil {
			return nil, err
		}
		ot.outboundFKs = append(ot.outboundFKs, optForeignKeyConstraint{
			name:            fk.Name,
			originTable:     cat.StableID(ref.OriginTableID),
			originIndex:     originIdx.ID,
			referencedTable: ot.id,
			referencedIndex: referencedIdx.ID,
			numCols:         int(len(ref.OriginColumnIDs)),
			validity:        fk.Validity,
			// TODO(jordan,radu): beware, this is weird: the "delete" direction for
			// fks always must use "match simple" at runtime. This is correct for
			// now, but when we implement the optimizer logic for match full, this
			// has to get attention.
			match: fk.Match,
		})
	}

	if len(desc.Families) == 0 {
		// This must be a virtual table, so synthesize a primary family. Only
		// column ids are needed by the family wrapper.
		family := &sqlbase.ColumnFamilyDescriptor{Name: "primary", ID: 0}
		family.ColumnIDs = make([]sqlbase.ColumnID, len(desc.Columns))
		for i := range family.ColumnIDs {
			family.ColumnIDs[i] = desc.Columns[i].ID
		}
		ot.primaryFamily.init(ot, family)
	} else {
		ot.primaryFamily.init(ot, &desc.Families[0])
		ot.families = make([]optFamily, len(desc.Families)-1)
		for i := range ot.families {
			ot.families[i].init(ot, &desc.Families[i+1])
		}
	}

	// Add stats last, now that other metadata is initialized.
	if stats != nil {
		ot.stats = make([]optTableStat, len(stats))
		n := 0
		for i := range stats {
			// We skip any stats that have columns that don't exist in the table anymore.
			if ok, err := ot.stats[n].init(ot, stats[i]); err != nil {
				return nil, err
			} else if ok {
				n++
			}
		}
		ot.stats = ot.stats[:n]
	}

	return ot, nil
}

// ID is part of the cat.Object interface.
func (ot *optTable) ID() cat.StableID {
	return ot.id
}

// isStale checks if the optTable object needs to be refreshed because the stats
// or zone config have changed. False positives are ok.
func (ot *optTable) isStale(tableStats []*stats.TableStatistic, zone *config.ZoneConfig) bool {
	// Fast check to verify that the statistics haven't changed: we check the
	// length and the address of the underlying array. This is not a perfect
	// check (in principle, the stats could have left the cache and then gotten
	// regenerated), but it works in the common case.
	if len(tableStats) != len(ot.rawStats) {
		return true
	}
	if len(tableStats) > 0 && &tableStats[0] != &ot.rawStats[0] {
		return true
	}
	if !zone.Equal(ot.zone) {
		return true
	}
	return false
}

// Equals is part of the cat.Object interface.
func (ot *optTable) Equals(other cat.Object) bool {
	otherTable, ok := other.(*optTable)
	if !ok {
		return false
	}
	if ot == otherTable {
		// Fast path when it is the same object.
		return true
	}
	if ot.id != otherTable.id || ot.desc.Version != otherTable.desc.Version {
		return false
	}

	// Verify the stats are identical.
	if len(ot.stats) != len(otherTable.stats) {
		return false
	}
	for i := range ot.stats {
		if !ot.stats[i].equals(&otherTable.stats[i]) {
			return false
		}
	}

	// Verify that indexes are in same zones. For performance, skip deep equality
	// check if it's the same as the previous index (common case).
	var prevLeftZone, prevRightZone *config.ZoneConfig
	for i := range ot.indexes {
		leftZone := ot.indexes[i].zone
		rightZone := otherTable.indexes[i].zone
		if leftZone == prevLeftZone && rightZone == prevRightZone {
			continue
		}
		if !leftZone.Equal(rightZone) {
			return false
		}
		prevLeftZone = leftZone
		prevRightZone = rightZone
	}

	return true
}

// Name is part of the cat.DataSource interface.
func (ot *optTable) Name() *cat.DataSourceName {
	return &ot.name
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return ot.desc.IsVirtualTable()
}

// IsInterleaved is part of the cat.Table interface.
func (ot *optTable) IsInterleaved() bool {
	return ot.desc.IsInterleaved()
}

// ColumnCount is part of the cat.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.desc.Columns)
}

// WritableColumnCount is part of the cat.Table interface.
func (ot *optTable) WritableColumnCount() int {
	return len(ot.desc.WritableColumns())
}

// DeletableColumnCount is part of the cat.Table interface.
func (ot *optTable) DeletableColumnCount() int {
	return len(ot.desc.DeletableColumns())
}

// Column is part of the cat.Table interface.
func (ot *optTable) Column(i int) cat.Column {
	return &ot.desc.DeletableColumns()[i]
}

// IndexCount is part of the cat.Table interface.
func (ot *optTable) IndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optTable) WritableIndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.WritableIndexes())
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optTable) DeletableIndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.DeletableIndexes())
}

// Index is part of the cat.Table interface.
func (ot *optTable) Index(i int) cat.Index {
	return &ot.indexes[i]
}

// StatisticCount is part of the cat.Table interface.
func (ot *optTable) StatisticCount() int {
	return len(ot.stats)
}

// Statistic is part of the cat.Table interface.
func (ot *optTable) Statistic(i int) cat.TableStatistic {
	return &ot.stats[i]
}

// CheckCount is part of the cat.Table interface.
func (ot *optTable) CheckCount() int {
	return len(ot.desc.ActiveChecks())
}

// Check is part of the cat.Table interface.
func (ot *optTable) Check(i int) cat.CheckConstraint {
	check := ot.desc.ActiveChecks()[i]
	return cat.CheckConstraint{
		Constraint: check.Expr,
		Validated:  check.Validity == sqlbase.ConstraintValidity_Validated,
	}
}

// FamilyCount is part of the cat.Table interface.
func (ot *optTable) FamilyCount() int {
	return 1 + len(ot.families)
}

// Family is part of the cat.Table interface.
func (ot *optTable) Family(i int) cat.Family {
	if i == 0 {
		return &ot.primaryFamily
	}
	return &ot.families[i-1]
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (ot *optTable) OutboundForeignKeyCount() int {
	return len(ot.outboundFKs)
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (ot *optTable) OutboundForeignKey(i int) cat.ForeignKeyConstraint {
	return &ot.outboundFKs[i]
}

// InboundForeignKeyCount is part of the cat.Table interface.
func (ot *optTable) InboundForeignKeyCount() int {
	return len(ot.inboundFKs)
}

// InboundForeignKey is part of the cat.Table interface.
func (ot *optTable) InboundForeignKey(i int) cat.ForeignKeyConstraint {
	return &ot.inboundFKs[i]
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optTable) lookupColumnOrdinal(colID sqlbase.ColumnID) (int, error) {
	col, ok := ot.colMap[colID]
	if ok {
		return col, nil
	}
	return col, pgerror.Newf(pgcode.UndefinedColumn,
		"column [%d] does not exist", colID)
}

// optIndex is a wrapper around sqlbase.IndexDescriptor that caches some
// commonly accessed information and keeps a reference to the table wrapper.
type optIndex struct {
	tab  *optTable
	desc *sqlbase.IndexDescriptor
	zone *config.ZoneConfig

	// storedCols is the set of non-PK columns if this is the primary index,
	// otherwise it is desc.StoreColumnIDs.
	storedCols []sqlbase.ColumnID

	indexOrdinal  int
	numCols       int
	numKeyCols    int
	numLaxKeyCols int
}

var _ cat.Index = &optIndex{}

// init can be used instead of newOptIndex when we have a pre-allocated instance
// (e.g. as part of a bigger struct).
func (oi *optIndex) init(
	tab *optTable, indexOrdinal int, desc *sqlbase.IndexDescriptor, zone *config.ZoneConfig,
) {
	oi.tab = tab
	oi.desc = desc
	oi.zone = zone
	oi.indexOrdinal = indexOrdinal
	if desc == &tab.desc.PrimaryIndex {
		// Although the primary index contains all columns in the table, the index
		// descriptor does not contain columns that are not explicitly part of the
		// primary key. Retrieve those columns from the table descriptor.
		oi.storedCols = make([]sqlbase.ColumnID, 0, tab.DeletableColumnCount()-len(desc.ColumnIDs))
		var pkCols util.FastIntSet
		for i := range desc.ColumnIDs {
			pkCols.Add(int(desc.ColumnIDs[i]))
		}
		for i, n := 0, tab.DeletableColumnCount(); i < n; i++ {
			id := tab.Column(i).ColID()
			if !pkCols.Contains(int(id)) {
				oi.storedCols = append(oi.storedCols, sqlbase.ColumnID(id))
			}
		}
		oi.numCols = tab.DeletableColumnCount()
	} else {
		oi.storedCols = desc.StoreColumnIDs
		oi.numCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs) + len(desc.StoreColumnIDs)
	}

	if desc.Unique {
		notNull := true
		for _, id := range desc.ColumnIDs {
			ord, _ := tab.lookupColumnOrdinal(id)
			if tab.desc.DeletableColumns()[ord].Nullable {
				notNull = false
				break
			}
		}

		if notNull {
			// Unique index with no null columns: columns from index are sufficient
			// to form a key without needing extra primary key columns. There is no
			// separate lax key.
			oi.numLaxKeyCols = len(desc.ColumnIDs)
			oi.numKeyCols = oi.numLaxKeyCols
		} else {
			// Unique index with at least one nullable column: extra primary key
			// columns will be added to the row key when one of the unique index
			// columns has a NULL value.
			oi.numLaxKeyCols = len(desc.ColumnIDs)
			oi.numKeyCols = oi.numLaxKeyCols + len(desc.ExtraColumnIDs)
		}
	} else {
		// Non-unique index: extra primary key columns are always added to the row
		// key. There is no separate lax key.
		oi.numLaxKeyCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs)
		oi.numKeyCols = oi.numLaxKeyCols
	}
}

// ID is part of the cat.Index interface.
func (oi *optIndex) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Index interface.
func (oi *optIndex) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// IsUnique is part of the cat.Index interface.
func (oi *optIndex) IsUnique() bool {
	return oi.desc.Unique
}

// IsInverted is part of the cat.Index interface.
func (oi *optIndex) IsInverted() bool {
	return oi.desc.Type == sqlbase.IndexDescriptor_INVERTED
}

// ColumnCount is part of the cat.Index interface.
func (oi *optIndex) ColumnCount() int {
	return oi.numCols
}

// KeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) KeyColumnCount() int {
	return oi.numKeyCols
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) LaxKeyColumnCount() int {
	return oi.numLaxKeyCols
}

// Column is part of the cat.Index interface.
func (oi *optIndex) Column(i int) cat.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return cat.IndexColumn{
			Column:     oi.tab.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		}
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
	}

	i -= length
	ord, _ := oi.tab.lookupColumnOrdinal(oi.storedCols[i])
	return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Zone is part of the cat.Index interface.
func (oi *optIndex) Zone() cat.Zone {
	return oi.zone
}

// Span is part of the cat.Index interface.
func (oi *optIndex) Span() roachpb.Span {
	desc := oi.tab.desc
	// Tables up to MaxSystemConfigDescID are grouped in a single system config
	// span.
	if desc.ID <= keys.MaxSystemConfigDescID {
		return keys.SystemConfigSpan
	}
	return desc.IndexSpan(oi.desc.ID)
}

// Table is part of the cat.Index interface.
func (oi *optIndex) Table() cat.Table {
	return oi.tab
}

// Ordinal is part of the cat.Index interface.
func (oi *optIndex) Ordinal() int {
	return oi.indexOrdinal
}

// PartitionByListPrefixes is part of the cat.Index interface.
func (oi *optIndex) PartitionByListPrefixes() []tree.Datums {
	list := oi.desc.Partitioning.List
	if len(list) == 0 {
		return nil
	}
	res := make([]tree.Datums, 0, len(list))
	var a sqlbase.DatumAlloc
	for i := range list {
		for _, valueEncBuf := range list[i].Values {
			t, _, err := sqlbase.DecodePartitionTuple(
				&a, &oi.tab.desc.TableDescriptor, oi.desc, &oi.desc.Partitioning,
				valueEncBuf, nil, /* prefixDatums */
			)
			if err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "while decoding partition tuple"))
			}
			// Ignore the DEFAULT case, where there is nothing to return.
			if len(t.Datums) > 0 {
				res = append(res, t.Datums)
			}
			// TODO(radu): split into multiple prefixes if Subpartition is also by list.
			// Note that this functionality should be kept in sync with the test catalog
			// implementation (test_catalog.go).
		}
	}
	return res
}

type optTableStat struct {
	createdAt      time.Time
	columnOrdinals []int
	rowCount       uint64
	distinctCount  uint64
	nullCount      uint64
	histogram      []cat.HistogramBucket
}

var _ cat.TableStatistic = &optTableStat{}

func (os *optTableStat) init(tab *optTable, stat *stats.TableStatistic) (ok bool, _ error) {
	os.createdAt = stat.CreatedAt
	os.rowCount = stat.RowCount
	os.distinctCount = stat.DistinctCount
	os.nullCount = stat.NullCount

	os.columnOrdinals = make([]int, len(stat.ColumnIDs))
	for i, c := range stat.ColumnIDs {
		var ok bool
		os.columnOrdinals[i], ok = tab.colMap[c]
		if !ok {
			// Column not in table (this is possible if the column was removed since
			// the statistic was calculated).
			return false, nil
		}
	}

	if stat.Histogram != nil {
		os.histogram = make([]cat.HistogramBucket, len(stat.Histogram.Buckets))
		typ := &stat.Histogram.ColumnType
		var a sqlbase.DatumAlloc
		for i := range os.histogram {
			bucket := &stat.Histogram.Buckets[i]
			datum, _, err := sqlbase.DecodeTableKey(&a, typ, bucket.UpperBound, encoding.Ascending)
			if err != nil {
				return false, err
			}
			os.histogram[i] = cat.HistogramBucket{
				NumEq:      uint64(bucket.NumEq),
				NumRange:   uint64(bucket.NumRange),
				UpperBound: datum,
			}
		}
	}
	return true, nil
}

func (os *optTableStat) equals(other *optTableStat) bool {
	// Two table statistics are considered equal if they have been created at the
	// same time, on the same set of columns.
	if os.createdAt != other.createdAt || len(os.columnOrdinals) != len(other.columnOrdinals) {
		return false
	}
	for i, c := range os.columnOrdinals {
		if c != other.columnOrdinals[i] {
			return false
		}
	}
	return true
}

// CreatedAt is part of the cat.TableStatistic interface.
func (os *optTableStat) CreatedAt() time.Time {
	return os.createdAt
}

// ColumnCount is part of the cat.TableStatistic interface.
func (os *optTableStat) ColumnCount() int {
	return len(os.columnOrdinals)
}

// ColumnOrdinal is part of the cat.TableStatistic interface.
func (os *optTableStat) ColumnOrdinal(i int) int {
	return os.columnOrdinals[i]
}

// RowCount is part of the cat.TableStatistic interface.
func (os *optTableStat) RowCount() uint64 {
	return os.rowCount
}

// DistinctCount is part of the cat.TableStatistic interface.
func (os *optTableStat) DistinctCount() uint64 {
	return os.distinctCount
}

// NullCount is part of the cat.TableStatistic interface.
func (os *optTableStat) NullCount() uint64 {
	return os.nullCount
}

// Histogram is part of the cat.TableStatistic interface.
func (os *optTableStat) Histogram() []cat.HistogramBucket {
	return os.histogram
}

// optFamily is a wrapper around sqlbase.ColumnFamilyDescriptor that keeps a
// reference to the table wrapper.
type optFamily struct {
	tab  *optTable
	desc *sqlbase.ColumnFamilyDescriptor
}

var _ cat.Family = &optFamily{}

// init can be used instead of newOptFamily when we have a pre-allocated
// instance (e.g. as part of a bigger struct).
func (oi *optFamily) init(tab *optTable, desc *sqlbase.ColumnFamilyDescriptor) {
	oi.tab = tab
	oi.desc = desc
}

// ID is part of the cat.Family interface.
func (oi *optFamily) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Family interface.
func (oi *optFamily) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// ColumnCount is part of the cat.Family interface.
func (oi *optFamily) ColumnCount() int {
	return len(oi.desc.ColumnIDs)
}

// Column is part of the cat.Family interface.
func (oi *optFamily) Column(i int) cat.FamilyColumn {
	ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
	return cat.FamilyColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Table is part of the cat.Family interface.
func (oi *optFamily) Table() cat.Table {
	return oi.tab
}

// optForeignKeyConstraint implements cat.ForeignKeyConstraint and represents a
// foreign key relationship. Both the origin and the referenced table store the
// same optForeignKeyConstraint (as an outbound and inbound reference,
// respectively).
type optForeignKeyConstraint struct {
	name string

	originTable cat.StableID
	originIndex sqlbase.IndexID

	referencedTable cat.StableID
	referencedIndex sqlbase.IndexID

	numCols  int
	validity sqlbase.ConstraintValidity
	match    sqlbase.ForeignKeyReference_Match
}

var _ cat.ForeignKeyConstraint = &optForeignKeyConstraint{}

// Name is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) Name() string {
	return fk.name
}

// OriginTableID is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) OriginTableID() cat.StableID {
	return fk.originTable
}

// ReferencedTableID is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) ReferencedTableID() cat.StableID {
	return fk.referencedTable
}

// ColumnCount is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) ColumnCount() int {
	return fk.numCols
}

// OriginColumnOrdinal is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) OriginColumnOrdinal(originTable cat.Table, i int) int {
	if originTable.ID() != fk.originTable {
		panic(errors.AssertionFailedf(
			"invalid table %d passed to OriginColumnOrdinal (expected %d)",
			originTable.ID(), fk.originTable,
		))
	}

	tab := originTable.(*optTable)
	index, err := tab.desc.FindIndexByID(fk.originIndex)
	if err != nil {
		panic(errors.AssertionFailedf("%v", err))
	}

	ord, _ := tab.lookupColumnOrdinal(index.ColumnIDs[i])
	return ord
}

// ReferencedColumnOrdinal is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) ReferencedColumnOrdinal(referencedTable cat.Table, i int) int {
	if referencedTable.ID() != fk.referencedTable {
		panic(errors.AssertionFailedf(
			"invalid table %d passed to ReferencedColumnOrdinal (expected %d)",
			referencedTable.ID(), fk.referencedTable,
		))
	}
	tab := referencedTable.(*optTable)
	index, err := tab.desc.FindIndexByID(fk.referencedIndex)
	if err != nil {
		panic(errors.AssertionFailedf("%v", err))
	}

	ord, _ := tab.lookupColumnOrdinal(index.ColumnIDs[i])
	return ord
}

// Validated is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) Validated() bool {
	return fk.validity == sqlbase.ConstraintValidity_Validated
}

// MatchMethod is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) MatchMethod() tree.CompositeKeyMatchMethod {
	return sqlbase.ForeignKeyReferenceMatchValue[fk.match]
}
