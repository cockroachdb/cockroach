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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
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

	oc.cfg = oc.planner.execCfg.Gossip.DeprecatedSystemConfig(47150)
}

// optSchema is a wrapper around sqlbase.ImmutableDatabaseDescriptor that
// implements the cat.Object and cat.Schema interfaces.
type optSchema struct {
	planner *planner
	desc    *sqlbase.ImmutableDatabaseDescriptor

	name cat.SchemaName
}

// ID is part of the cat.Object interface.
func (os *optSchema) ID() cat.StableID {
	return cat.StableID(os.desc.GetID())
}

// PostgresDescriptorID is part of the cat.Object interface.
func (os *optSchema) PostgresDescriptorID() cat.StableID {
	return cat.StableID(os.desc.GetID())
}

// Equals is part of the cat.Object interface.
func (os *optSchema) Equals(other cat.Object) bool {
	otherSchema, ok := other.(*optSchema)
	return ok && os.desc.GetID() == otherSchema.desc.GetID()
}

// Name is part of the cat.Schema interface.
func (os *optSchema) Name() *cat.SchemaName {
	return &os.name
}

// GetDataSourceNames is part of the cat.Schema interface.
func (os *optSchema) GetDataSourceNames(ctx context.Context) ([]cat.DataSourceName, error) {
	return resolver.GetObjectNames(
		ctx,
		os.planner.Txn(),
		os.planner,
		os.planner.ExecCfg().Codec,
		os.desc,
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

	oc.tn.ObjectNamePrefix = *name
	found, desc, err := oc.tn.ObjectNamePrefix.Resolve(
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
		desc:    desc.(*sqlbase.ImmutableDatabaseDescriptor),
		name:    oc.tn.ObjectNamePrefix,
	}, oc.tn.ObjectNamePrefix, nil
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
	desc, err := resolver.ResolveExistingTableObject(ctx, oc.planner, &oc.tn, tree.ObjectLookupFlagsWithRequired(), resolver.ResolveAnyDescType)
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
	ctx context.Context, flags cat.Flags, dataSourceID cat.StableID,
) (_ cat.DataSource, isAdding bool, _ error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}

	tableLookup, err := oc.planner.LookupTableByID(ctx, sqlbase.ID(dataSourceID))

	if err != nil || tableLookup.IsAdding {
		if errors.Is(err, sqlbase.ErrDescriptorNotFound) || tableLookup.IsAdding {
			return nil, tableLookup.IsAdding, sqlbase.NewUndefinedRelationError(&tree.TableRef{TableID: int64(dataSourceID)})
		}
		return nil, false, err
	}

	// The name is only used for virtual tables, which can't be looked up by ID.
	ds, err := oc.dataSourceForDesc(ctx, cat.Flags{}, tableLookup.Desc, &tree.TableName{})
	return ds, false, err
}

func getDescForCatalogObject(o cat.Object) (sqlbase.DescriptorInterface, error) {
	switch t := o.(type) {
	case *optSchema:
		return t.desc, nil
	case *optTable:
		return t.desc, nil
	case *optVirtualTable:
		return t.desc, nil
	case *optView:
		return t.desc, nil
	case *optSequence:
		return t.desc, nil
	default:
		return nil, errors.AssertionFailedf("invalid object type: %T", o)
	}
}

func getDescForDataSource(o cat.DataSource) (*sqlbase.ImmutableTableDescriptor, error) {
	switch t := o.(type) {
	case *optTable:
		return t.desc, nil
	case *optVirtualTable:
		return t.desc, nil
	case *optView:
		return t.desc, nil
	case *optSequence:
		return t.desc, nil
	default:
		return nil, errors.AssertionFailedf("invalid object type: %T", o)
	}
}

// CheckPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
	desc, err := getDescForCatalogObject(o)
	if err != nil {
		return err
	}
	return oc.planner.CheckPrivilege(ctx, desc, priv)
}

// CheckAnyPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckAnyPrivilege(ctx context.Context, o cat.Object) error {
	desc, err := getDescForCatalogObject(o)
	if err != nil {
		return err
	}
	return oc.planner.CheckAnyPrivilege(ctx, desc)
}

// HasAdminRole is part of the cat.Catalog interface.
func (oc *optCatalog) HasAdminRole(ctx context.Context) (bool, error) {
	return oc.planner.HasAdminRole(ctx)
}

// RequireAdminRole is part of the cat.Catalog interface.
func (oc *optCatalog) RequireAdminRole(ctx context.Context, action string) error {
	return oc.planner.RequireAdminRole(ctx, action)
}

// FullyQualifiedName is part of the cat.Catalog interface.
func (oc *optCatalog) FullyQualifiedName(
	ctx context.Context, ds cat.DataSource,
) (cat.DataSourceName, error) {
	return oc.fullyQualifiedNameWithTxn(ctx, ds, oc.planner.Txn())
}

func (oc *optCatalog) fullyQualifiedNameWithTxn(
	ctx context.Context, ds cat.DataSource, txn *kv.Txn,
) (cat.DataSourceName, error) {
	if vt, ok := ds.(*optVirtualTable); ok {
		// Virtual tables require special handling, because they can have multiple
		// effective instances that utilize the same descriptor.
		return vt.name, nil
	}

	desc, err := getDescForDataSource(ds)
	if err != nil {
		return cat.DataSourceName{}, err
	}

	dbID := desc.ParentID
	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, txn, oc.codec(), dbID)
	if err != nil {
		return cat.DataSourceName{}, err
	}
	return tree.MakeTableName(tree.Name(dbDesc.GetName()), tree.Name(desc.Name)), nil
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
		ds = newOptView(desc)

	case desc.IsSequence():
		ds = newOptSequence(desc)

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
	if desc.IsVirtualTable() {
		// Virtual tables can have multiple effective instances that utilize the
		// same descriptor, so we can't cache them (see the comment for
		// optVirtualTable.id for more information).
		return newOptVirtualTable(ctx, oc, desc, name)
	}

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

	ds, err := newOptTable(desc, oc.codec(), tableStats, zoneConfig)
	if err != nil {
		return nil, err
	}
	oc.dataSources[desc] = ds
	return ds, nil
}

var emptyZoneConfig = &zonepb.ZoneConfig{}

// getZoneConfig returns the ZoneConfig data structure for the given table.
// ZoneConfigs are stored in protobuf binary format in the SystemConfig, which
// is gossiped around the cluster. Note that the returned ZoneConfig might be
// somewhat stale, since it's taken from the gossiped SystemConfig.
func (oc *optCatalog) getZoneConfig(
	desc *sqlbase.ImmutableTableDescriptor,
) (*zonepb.ZoneConfig, error) {
	// Lookup table's zone if system config is available (it may not be as node
	// is starting up and before it's received the gossiped config). If it is
	// not available, use an empty config that has no zone constraints.
	if oc.cfg == nil || desc.IsVirtualTable() {
		return emptyZoneConfig, nil
	}
	zone, err := oc.cfg.GetZoneConfigForObject(oc.codec(), uint32(desc.ID))
	if err != nil {
		return nil, err
	}
	if zone == nil {
		// This can happen with tests that override the hook.
		zone = emptyZoneConfig
	}
	return zone, err
}

func (oc *optCatalog) codec() keys.SQLCodec {
	return oc.planner.ExecCfg().Codec
}

// optView is a wrapper around sqlbase.ImmutableTableDescriptor that implements
// the cat.Object, cat.DataSource, and cat.View interfaces.
type optView struct {
	desc *sqlbase.ImmutableTableDescriptor
}

var _ cat.View = &optView{}

func newOptView(desc *sqlbase.ImmutableTableDescriptor) *optView {
	return &optView{desc: desc}
}

// ID is part of the cat.Object interface.
func (ov *optView) ID() cat.StableID {
	return cat.StableID(ov.desc.ID)
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ov *optView) PostgresDescriptorID() cat.StableID {
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
func (ov *optView) Name() tree.Name {
	return tree.Name(ov.desc.Name)
}

// IsSystemView is part of the cat.View interface.
func (ov *optView) IsSystemView() bool {
	return ov.desc.IsVirtualTable()
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
}

var _ cat.DataSource = &optSequence{}
var _ cat.Sequence = &optSequence{}

func newOptSequence(desc *sqlbase.ImmutableTableDescriptor) *optSequence {
	return &optSequence{desc: desc}
}

// ID is part of the cat.Object interface.
func (os *optSequence) ID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// PostgresDescriptorID is part of the cat.Object interface.
func (os *optSequence) PostgresDescriptorID() cat.StableID {
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

// Name is part of the cat.Sequence interface.
func (os *optSequence) Name() tree.Name {
	return tree.Name(os.desc.Name)
}

// SequenceMarker is part of the cat.Sequence interface.
func (os *optSequence) SequenceMarker() {}

// optTable is a wrapper around sqlbase.ImmutableTableDescriptor that caches
// index wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// indexes are the inlined wrappers for the table's primary and secondary
	// indexes.
	indexes []optIndex

	// codec is capable of encoding sql table keys.
	codec keys.SQLCodec

	// rawStats stores the original table statistics slice. Used for a fast-path
	// check that the statistics haven't changed.
	rawStats []*stats.TableStatistic

	// stats are the inlined wrappers for table statistics.
	stats []optTableStat

	zone *zonepb.ZoneConfig

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

	// checkConstraints is the set of check constraints for this table. It
	// can be different from desc's constraints because of synthesized
	// constraints for user defined types.
	checkConstraints []cat.CheckConstraint

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int
}

var _ cat.Table = &optTable{}

func newOptTable(
	desc *sqlbase.ImmutableTableDescriptor,
	codec keys.SQLCodec,
	stats []*stats.TableStatistic,
	tblZone *zonepb.ZoneConfig,
) (*optTable, error) {
	ot := &optTable{
		desc:     desc,
		codec:    codec,
		rawStats: stats,
		zone:     tblZone,
	}

	// Create the table's column mapping from sqlbase.ColumnID to column ordinal.
	ot.colMap = make(map[sqlbase.ColumnID]int, ot.DeletableColumnCount())
	for i, n := 0, ot.DeletableColumnCount(); i < n; i++ {
		ot.colMap[sqlbase.ColumnID(ot.Column(i).ColID())] = i
	}

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

	for i := range ot.desc.OutboundFKs {
		fk := &ot.desc.OutboundFKs[i]
		ot.outboundFKs = append(ot.outboundFKs, optForeignKeyConstraint{
			name:              fk.Name,
			originTable:       ot.ID(),
			originColumns:     fk.OriginColumnIDs,
			referencedTable:   cat.StableID(fk.ReferencedTableID),
			referencedColumns: fk.ReferencedColumnIDs,
			validity:          fk.Validity,
			match:             fk.Match,
			deleteAction:      fk.OnDelete,
			updateAction:      fk.OnUpdate,
		})
	}
	for i := range ot.desc.InboundFKs {
		fk := &ot.desc.InboundFKs[i]
		ot.inboundFKs = append(ot.inboundFKs, optForeignKeyConstraint{
			name:              fk.Name,
			originTable:       cat.StableID(fk.OriginTableID),
			originColumns:     fk.OriginColumnIDs,
			referencedTable:   ot.ID(),
			referencedColumns: fk.ReferencedColumnIDs,
			validity:          fk.Validity,
			match:             fk.Match,
			deleteAction:      fk.OnDelete,
			updateAction:      fk.OnUpdate,
		})
	}

	ot.primaryFamily.init(ot, &desc.Families[0])
	ot.families = make([]optFamily, len(desc.Families)-1)
	for i := range ot.families {
		ot.families[i].init(ot, &desc.Families[i+1])
	}

	// Synthesize any check constraints for user defined types.
	var synthesizedChecks []cat.CheckConstraint
	// TODO (rohany): We don't allow referencing columns in mutations in these
	//  expressions. However, it seems like we will need to have these checks
	//  operate on columns in mutations. Consider the following case:
	//  * a user adds a column with an enum type.
	//  * the column has a default expression of an enum that is not in the
	//    writeable state.
	//  * We will need a check constraint here to ensure that writes to the
	//    column are not successful, but we wouldn't be able to add that now.
	for i := 0; i < ot.ColumnCount(); i++ {
		col := ot.Column(i)
		colType := col.DatumType()
		if colType.UserDefined() {
			switch colType.Family() {
			case types.EnumFamily:
				// TODO (rohany): When we can alter types, this logic will change.
				//  In particular, we will want to generate two check constraints if the
				//  enum contains values that are read only. The first constraint will
				//  be validated, and contain all of the members of the enum. The second
				//  will be unvalidated, and will contain only the writeable members of
				//  the enum. The unvalidated constraint ensures that only writeable
				//  members of the enum are written. The validated constraint ensures
				//  that all potentially written values of the enum are considered when
				//  planning read operations.
				// We synthesize an (x IN (v1, v2, v3...)) check for enum types.
				expr := &tree.ComparisonExpr{
					Operator: tree.In,
					Left:     &tree.ColumnItem{ColumnName: col.ColName()},
					Right:    tree.NewDTuple(colType, tree.MakeAllDEnumsInType(colType)...),
				}
				synthesizedChecks = append(synthesizedChecks, cat.CheckConstraint{
					Constraint: tree.Serialize(expr),
					Validated:  true,
				})
			}
		}
	}
	// Move all existing and synthesized checks into the opt table.
	activeChecks := desc.ActiveChecks()
	ot.checkConstraints = make([]cat.CheckConstraint, 0, len(activeChecks)+len(synthesizedChecks))
	for i := range activeChecks {
		ot.checkConstraints = append(ot.checkConstraints, cat.CheckConstraint{
			Constraint: activeChecks[i].Expr,
			Validated:  activeChecks[i].Validity == sqlbase.ConstraintValidity_Validated,
		})
	}
	ot.checkConstraints = append(ot.checkConstraints, synthesizedChecks...)

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
	return cat.StableID(ot.desc.ID)
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ot *optTable) PostgresDescriptorID() cat.StableID {
	return cat.StableID(ot.desc.ID)
}

// isStale checks if the optTable object needs to be refreshed because the stats
// or zone config have changed. False positives are ok.
func (ot *optTable) isStale(tableStats []*stats.TableStatistic, zone *zonepb.ZoneConfig) bool {
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
	if ot.desc.ID != otherTable.desc.ID || ot.desc.Version != otherTable.desc.Version {
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
	var prevLeftZone, prevRightZone *zonepb.ZoneConfig
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

// Name is part of the cat.Table interface.
func (ot *optTable) Name() tree.Name {
	return tree.Name(ot.desc.Name)
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return false
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
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optTable) WritableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.WritableIndexes())
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optTable) DeletableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.DeletableIndexes())
}

// Index is part of the cat.Table interface.
func (ot *optTable) Index(i cat.IndexOrdinal) cat.Index {
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
	return len(ot.checkConstraints)
}

// Check is part of the cat.Table interface.
func (ot *optTable) Check(i int) cat.CheckConstraint {
	return ot.checkConstraints[i]
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
	zone *zonepb.ZoneConfig

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
	tab *optTable, indexOrdinal int, desc *sqlbase.IndexDescriptor, zone *zonepb.ZoneConfig,
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

// Predicate is part of the cat.Index interface. It returns the predicate
// expression and true if the index is a partial index. If the index is not
// partial, the empty string and false is returned.
func (oi *optIndex) Predicate() (string, bool) {
	return oi.desc.Predicate, oi.desc.Predicate != ""
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
	return desc.IndexSpan(oi.tab.codec, oi.desc.ID)
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
				&a, oi.tab.codec, &oi.tab.desc.TableDescriptor, oi.desc, &oi.desc.Partitioning,
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

// InterleaveAncestorCount is part of the cat.Index interface.
func (oi *optIndex) InterleaveAncestorCount() int {
	return len(oi.desc.Interleave.Ancestors)
}

// InterleaveAncestor is part of the cat.Index interface.
func (oi *optIndex) InterleaveAncestor(i int) (table, index cat.StableID, numKeyCols int) {
	a := &oi.desc.Interleave.Ancestors[i]
	return cat.StableID(a.TableID), cat.StableID(a.IndexID), int(a.SharedPrefixLen)
}

// InterleavedByCount is part of the cat.Index interface.
func (oi *optIndex) InterleavedByCount() int {
	return len(oi.desc.InterleavedBy)
}

// InterleavedBy is part of the cat.Index interface.
func (oi *optIndex) InterleavedBy(i int) (table, index cat.StableID) {
	ref := &oi.desc.InterleavedBy[i]
	return cat.StableID(ref.Table), cat.StableID(ref.Index)
}

// GeoConfig is part of the cat.Index interface.
func (oi *optIndex) GeoConfig() *geoindex.Config {
	return &oi.desc.GeoConfig
}

type optTableStat struct {
	stat           *stats.TableStatistic
	columnOrdinals []int
}

var _ cat.TableStatistic = &optTableStat{}

func (os *optTableStat) init(tab *optTable, stat *stats.TableStatistic) (ok bool, _ error) {
	os.stat = stat
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

	return true, nil
}

func (os *optTableStat) equals(other *optTableStat) bool {
	// Two table statistics are considered equal if they have been created at the
	// same time, on the same set of columns.
	if os.CreatedAt() != other.CreatedAt() || len(os.columnOrdinals) != len(other.columnOrdinals) {
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
	return os.stat.CreatedAt
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
	return os.stat.RowCount
}

// DistinctCount is part of the cat.TableStatistic interface.
func (os *optTableStat) DistinctCount() uint64 {
	return os.stat.DistinctCount
}

// NullCount is part of the cat.TableStatistic interface.
func (os *optTableStat) NullCount() uint64 {
	return os.stat.NullCount
}

// Histogram is part of the cat.TableStatistic interface.
func (os *optTableStat) Histogram() []cat.HistogramBucket {
	return os.stat.Histogram
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

	originTable   cat.StableID
	originColumns []sqlbase.ColumnID

	referencedTable   cat.StableID
	referencedColumns []sqlbase.ColumnID

	validity     sqlbase.ConstraintValidity
	match        sqlbase.ForeignKeyReference_Match
	deleteAction sqlbase.ForeignKeyReference_Action
	updateAction sqlbase.ForeignKeyReference_Action
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
	return len(fk.originColumns)
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
	ord, _ := tab.lookupColumnOrdinal(fk.originColumns[i])
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
	ord, _ := tab.lookupColumnOrdinal(fk.referencedColumns[i])
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

// DeleteReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) DeleteReferenceAction() tree.ReferenceAction {
	return sqlbase.ForeignKeyReferenceActionType[fk.deleteAction]
}

// UpdateReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) UpdateReferenceAction() tree.ReferenceAction {
	return sqlbase.ForeignKeyReferenceActionType[fk.updateAction]
}

// optVirtualTable is similar to optTable but is used with virtual tables.
type optVirtualTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// A virtual table can effectively have multiple instances, with different
	// contents. For example `db1.pg_catalog.pg_sequence` contains info about
	// sequences in db1, whereas `db2.pg_catalog.pg_sequence` contains info about
	// sequences in db2.
	//
	// These instances should have different stable IDs. To achieve this, the
	// stable ID is the database ID concatenated with the descriptor ID.
	//
	// Note that some virtual tables have a special instance with empty catalog,
	// for example "".information_schema.tables contains info about tables in
	// all databases. We treat the empty catalog as having database ID 0.
	id cat.StableID

	// name is the fully qualified, fully resolved, fully normalized name of the
	// virtual table.
	name cat.DataSourceName

	// indexes contains "virtual indexes", which are used to produce virtual table
	// data given constraints using generator functions. The 0th index is a
	// synthesized primary index.
	indexes []optVirtualIndex

	// family is a synthesized primary family.
	family optVirtualFamily

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int
}

var _ cat.Table = &optVirtualTable{}

func newOptVirtualTable(
	ctx context.Context,
	oc *optCatalog,
	desc *sqlbase.ImmutableTableDescriptor,
	name *cat.DataSourceName,
) (*optVirtualTable, error) {
	// Calculate the stable ID (see the comment for optVirtualTable.id).
	id := cat.StableID(desc.ID)
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
			id |= cat.StableID(dbDesc.(*sqlbase.ImmutableDatabaseDescriptor).GetID()) << 32
		}
	}

	ot := &optVirtualTable{
		desc: desc,
		id:   id,
		name: *name,
	}

	// Create the table's column mapping from sqlbase.ColumnID to column ordinal.
	ot.colMap = make(map[sqlbase.ColumnID]int, ot.DeletableColumnCount())
	for i, n := 0, ot.DeletableColumnCount(); i < n; i++ {
		ot.colMap[sqlbase.ColumnID(ot.Column(i).ColID())] = i
	}

	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	ot.family.init(ot)

	// Build the indexes (add 1 to account for lack of primary index in
	// indexes slice).
	ot.indexes = make([]optVirtualIndex, 1+len(ot.desc.Indexes))
	// Set up the primary index.
	ot.indexes[0] = optVirtualIndex{
		tab:          ot,
		indexOrdinal: 0,
		numCols:      ot.ColumnCount(),
		isPrimary:    true,
		desc: &sqlbase.IndexDescriptor{
			ID:   0,
			Name: "primary",
		},
	}

	for i := range ot.desc.Indexes {
		idxDesc := &ot.desc.Indexes[i]
		if len(idxDesc.ColumnIDs) > 1 {
			panic("virtual indexes with more than 1 col not supported")
		}

		// Add 1, since the 0th index will the the primary that we added above.
		ot.indexes[i+1] = optVirtualIndex{
			tab:          ot,
			desc:         idxDesc,
			indexOrdinal: i + 1,
			// The virtual indexes don't return the bogus PK key?
			numCols: ot.ColumnCount(),
		}
	}

	return ot, nil
}

// ID is part of the cat.Object interface.
func (ot *optVirtualTable) ID() cat.StableID {
	return ot.id
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ot *optVirtualTable) PostgresDescriptorID() cat.StableID {
	return cat.StableID(ot.desc.ID)
}

// Equals is part of the cat.Object interface.
func (ot *optVirtualTable) Equals(other cat.Object) bool {
	otherTable, ok := other.(*optVirtualTable)
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

	return true
}

// Name is part of the cat.Table interface.
func (ot *optVirtualTable) Name() tree.Name {
	return ot.name.ObjectName
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optVirtualTable) IsVirtualTable() bool {
	return true
}

// ColumnCount is part of the cat.Table interface.
func (ot *optVirtualTable) ColumnCount() int {
	// Virtual tables expose an extra (bogus) PK column.
	return len(ot.desc.Columns) + 1
}

// WritableColumnCount is part of the cat.Table interface.
func (ot *optVirtualTable) WritableColumnCount() int {
	return len(ot.desc.WritableColumns()) + 1
}

// DeletableColumnCount is part of the cat.Table interface.
func (ot *optVirtualTable) DeletableColumnCount() int {
	return len(ot.desc.DeletableColumns()) + 1
}

// Column is part of the cat.Table interface.
func (ot *optVirtualTable) Column(i int) cat.Column {
	if i == 0 {
		// Column 0 is a dummy PK column.
		return optDummyVirtualPKColumn{}
	}
	return &ot.desc.DeletableColumns()[i-1]
}

// IndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) IndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) WritableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.WritableIndexes())
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) DeletableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.DeletableIndexes())
}

// Index is part of the cat.Table interface.
func (ot *optVirtualTable) Index(i cat.IndexOrdinal) cat.Index {
	return &ot.indexes[i]
}

// StatisticCount is part of the cat.Table interface.
func (ot *optVirtualTable) StatisticCount() int {
	return 0
}

// Statistic is part of the cat.Table interface.
func (ot *optVirtualTable) Statistic(i int) cat.TableStatistic {
	panic("no stats")
}

// CheckCount is part of the cat.Table interface.
func (ot *optVirtualTable) CheckCount() int {
	return len(ot.desc.ActiveChecks())
}

// Check is part of the cat.Table interface.
func (ot *optVirtualTable) Check(i int) cat.CheckConstraint {
	check := ot.desc.ActiveChecks()[i]
	return cat.CheckConstraint{
		Constraint: check.Expr,
		Validated:  check.Validity == sqlbase.ConstraintValidity_Validated,
	}
}

// FamilyCount is part of the cat.Table interface.
func (ot *optVirtualTable) FamilyCount() int {
	return 1
}

// Family is part of the cat.Table interface.
func (ot *optVirtualTable) Family(i int) cat.Family {
	return &ot.family
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (ot *optVirtualTable) OutboundForeignKeyCount() int {
	return 0
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (ot *optVirtualTable) OutboundForeignKey(i int) cat.ForeignKeyConstraint {
	panic("no FKs")
}

// InboundForeignKeyCount is part of the cat.Table interface.
func (ot *optVirtualTable) InboundForeignKeyCount() int {
	return 0
}

// InboundForeignKey is part of the cat.Table interface.
func (ot *optVirtualTable) InboundForeignKey(i int) cat.ForeignKeyConstraint {
	panic("no FKs")
}

type optDummyVirtualPKColumn struct{}

var _ cat.Column = optDummyVirtualPKColumn{}

// ColID is part of the cat.Column interface.
func (optDummyVirtualPKColumn) ColID() cat.StableID {
	return math.MaxInt64
}

// ColName is part of the cat.Column interface.
func (optDummyVirtualPKColumn) ColName() tree.Name {
	return "crdb_internal_vtable_pk"
}

// DatumType is part of the cat.Column interface.
func (optDummyVirtualPKColumn) DatumType() *types.T {
	return types.Int
}

// ColTypePrecision is part of the cat.Column interface.
func (optDummyVirtualPKColumn) ColTypePrecision() int {
	return int(types.Int.Precision())
}

// ColTypeWidth is part of the cat.Column interface.
func (optDummyVirtualPKColumn) ColTypeWidth() int {
	return int(types.Int.Width())
}

// ColTypeStr is part of the cat.Column interface.
func (optDummyVirtualPKColumn) ColTypeStr() string {
	return types.Int.SQLString()
}

// IsNullable is part of the cat.Column interface.
func (optDummyVirtualPKColumn) IsNullable() bool {
	return false
}

// IsHidden is part of the cat.Column interface.
func (optDummyVirtualPKColumn) IsHidden() bool {
	return true
}

// HasDefault is part of the cat.Column interface.
func (optDummyVirtualPKColumn) HasDefault() bool {
	return false
}

// DefaultExprStr is part of the cat.Column interface.
func (optDummyVirtualPKColumn) DefaultExprStr() string {
	return ""
}

// IsComputed is part of the cat.Column interface.
func (optDummyVirtualPKColumn) IsComputed() bool {
	return false
}

// ComputedExprStr is part of the cat.Column interface.
func (optDummyVirtualPKColumn) ComputedExprStr() string {
	return ""
}

// optVirtualIndex is a dummy implementation of cat.Index for the indexes
// reported by a virtual table. The index assumes that table column 0 is a dummy
// PK column.
type optVirtualIndex struct {
	tab *optVirtualTable

	// isPrimary is set to true if this is the dummy PK index for virtual tables.
	isPrimary bool

	desc *sqlbase.IndexDescriptor

	numCols int

	indexOrdinal int
}

// ID is part of the cat.Index interface.
func (oi *optVirtualIndex) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Index interface.
func (oi *optVirtualIndex) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// IsUnique is part of the cat.Index interface.
func (oi *optVirtualIndex) IsUnique() bool {
	return oi.desc.Unique
}

// IsInverted is part of the cat.Index interface.
func (oi *optVirtualIndex) IsInverted() bool {
	return false
}

// ColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) ColumnCount() int {
	return oi.numCols
}

// Predicate is part of the cat.Index interface.
func (oi *optVirtualIndex) Predicate() (string, bool) {
	return "", false
}

// KeyColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) KeyColumnCount() int {
	return 1
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) LaxKeyColumnCount() int {
	return 1
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optVirtualTable) lookupColumnOrdinal(colID sqlbase.ColumnID) (int, error) {
	col, ok := ot.colMap[colID]
	if ok {
		return col, nil
	}
	return col, pgerror.Newf(pgcode.UndefinedColumn,
		"column [%d] does not exist", colID)
}

// Column is part of the cat.Index interface.
func (oi *optVirtualIndex) Column(i int) cat.IndexColumn {
	if oi.isPrimary {
		return cat.IndexColumn{Column: oi.tab.Column(i), Ordinal: i}
	}
	if i == oi.ColumnCount()-1 {
		// The special bogus PK column goes at the end. It has ID 0.
		return cat.IndexColumn{Column: oi.tab.Column(0), Ordinal: 0}
	}
	length := len(oi.desc.ColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return cat.IndexColumn{
			Column:  oi.tab.Column(ord),
			Ordinal: ord,
		}
	}

	i -= length
	ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.StoreColumnIDs[i])
	return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Zone is part of the cat.Index interface.
func (oi *optVirtualIndex) Zone() cat.Zone {
	panic("no zone")
}

// Span is part of the cat.Index interface.
func (oi *optVirtualIndex) Span() roachpb.Span {
	panic("no span")
}

// Table is part of the cat.Index interface.
func (oi *optVirtualIndex) Table() cat.Table {
	return oi.tab
}

// Ordinal is part of the cat.Index interface.
func (oi *optVirtualIndex) Ordinal() int {
	return oi.indexOrdinal
}

// PartitionByListPrefixes is part of the cat.Index interface.
func (oi *optVirtualIndex) PartitionByListPrefixes() []tree.Datums {
	panic("no partition")
}

// InterleaveAncestorCount is part of the cat.Index interface.
func (oi *optVirtualIndex) InterleaveAncestorCount() int {
	return 0
}

// InterleaveAncestor is part of the cat.Index interface.
func (oi *optVirtualIndex) InterleaveAncestor(i int) (table, index cat.StableID, numKeyCols int) {
	panic("no interleavings")
}

// InterleavedByCount is part of the cat.Index interface.
func (oi *optVirtualIndex) InterleavedByCount() int {
	return 0
}

// InterleavedBy is part of the cat.Index interface.
func (oi *optVirtualIndex) InterleavedBy(i int) (table, index cat.StableID) {
	panic("no interleavings")
}

// GeoConfig is part of the cat.Index interface.
func (oi *optVirtualIndex) GeoConfig() *geoindex.Config {
	return nil
}

// optVirtualFamily is a dummy implementation of cat.Family for the only family
// reported by a virtual table.
type optVirtualFamily struct {
	tab *optVirtualTable
}

var _ cat.Family = &optVirtualFamily{}

func (oi *optVirtualFamily) init(tab *optVirtualTable) {
	oi.tab = tab
}

// ID is part of the cat.Family interface.
func (oi *optVirtualFamily) ID() cat.StableID {
	return 0
}

// Name is part of the cat.Family interface.
func (oi *optVirtualFamily) Name() tree.Name {
	return "primary"
}

// ColumnCount is part of the cat.Family interface.
func (oi *optVirtualFamily) ColumnCount() int {
	return oi.tab.ColumnCount()
}

// Column is part of the cat.Family interface.
func (oi *optVirtualFamily) Column(i int) cat.FamilyColumn {
	return cat.FamilyColumn{Column: oi.tab.Column(i), Ordinal: i}
}

// Table is part of the cat.Family interface.
func (oi *optVirtualFamily) Table() cat.Table {
	return oi.tab
}
