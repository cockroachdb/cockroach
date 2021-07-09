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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
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
	dataSources map[catalog.TableDescriptor]cat.DataSource

	// tn is a temporary name used during resolution to avoid heap allocation.
	tn tree.TableName
}

var _ cat.Catalog = &optCatalog{}

// init initializes an optCatalog instance (which the caller can pre-allocate).
// The instance can be used across multiple queries, but reset() should be
// called for each query.
func (oc *optCatalog) init(planner *planner) {
	oc.planner = planner
	oc.dataSources = make(map[catalog.TableDescriptor]cat.DataSource)
}

// reset prepares the optCatalog to be used for a new query.
func (oc *optCatalog) reset() {
	// If we have accumulated too many tables in our map, throw everything away.
	// This deals with possible edge cases where we do a lot of DDL in a
	// long-lived session.
	if len(oc.dataSources) > 100 {
		oc.dataSources = make(map[catalog.TableDescriptor]cat.DataSource)
	}

	oc.cfg = oc.planner.execCfg.SystemConfig.GetSystemConfig()
}

// optSchema represents the parent database and schema for an object. It
// implements the cat.Object and cat.Schema interfaces.
type optSchema struct {
	planner *planner

	database catalog.DatabaseDescriptor
	schema   catalog.SchemaDescriptor

	name cat.SchemaName
}

// ID is part of the cat.Object interface.
func (os *optSchema) ID() cat.StableID {
	switch os.schema.SchemaKind() {
	case catalog.SchemaUserDefined, catalog.SchemaTemporary:
		// User defined schemas and the temporary schema have real ID's, so use
		// them here.
		return cat.StableID(os.schema.GetID())
	default:
		// Virtual schemas and the public schema don't, so just fall back to the
		// parent database's ID.
		return cat.StableID(os.database.GetID())
	}
}

// PostgresDescriptorID is part of the cat.Object interface.
func (os *optSchema) PostgresDescriptorID() cat.StableID {
	return os.ID()
}

// Equals is part of the cat.Object interface.
func (os *optSchema) Equals(other cat.Object) bool {
	otherSchema, ok := other.(*optSchema)
	return ok && os.ID() == otherSchema.ID()
}

// Name is part of the cat.Schema interface.
func (os *optSchema) Name() *cat.SchemaName {
	return &os.name
}

// GetDataSourceNames is part of the cat.Schema interface.
func (os *optSchema) GetDataSourceNames(
	ctx context.Context,
) ([]cat.DataSourceName, descpb.IDs, error) {
	return resolver.GetObjectNamesAndIDs(
		ctx,
		os.planner.Txn(),
		os.planner,
		os.planner.ExecCfg().Codec,
		os.database,
		os.name.Schema(),
		true, /* explicitPrefix */
	)
}

func (os *optSchema) getDescriptorForPermissionsCheck() catalog.Descriptor {
	// If the schema is backed by a descriptor, then return it.
	if os.schema.SchemaKind() == catalog.SchemaUserDefined {
		return os.schema
	}
	// Otherwise, just return the database descriptor.
	return os.database
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
	found, prefix, err := resolver.ResolveObjectNamePrefix(
		ctx, oc.planner, oc.planner.CurrentDatabase(),
		oc.planner.CurrentSearchPath(), &oc.tn.ObjectNamePrefix,
	)
	if err != nil {
		return nil, cat.SchemaName{}, err
	}
	if !found {
		if !name.ExplicitSchema && !name.ExplicitCatalog {
			return nil, cat.SchemaName{}, pgerror.New(
				pgcode.InvalidName, "no database or schema specified",
			)
		}
		return nil, cat.SchemaName{}, pgerror.Newf(
			pgcode.InvalidSchemaName, "target database or schema does not exist",
		)
	}

	return &optSchema{
		planner:  oc.planner,
		database: prefix.Database,
		schema:   prefix.Schema,
		name:     oc.tn.ObjectNamePrefix,
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
	lflags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveAnyTableKind)
	prefix, desc, err := resolver.ResolveExistingTableObject(ctx, oc.planner, &oc.tn, lflags)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}

	// Ensure that the current user can access the target schema.
	if err := oc.planner.canResolveDescUnderSchema(ctx, prefix.Schema, desc); err != nil {
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

	tableLookup, err := oc.planner.LookupTableByID(ctx, descpb.ID(dataSourceID))

	if err != nil {
		isAdding := catalog.HasAddingTableError(err)
		if errors.Is(err, catalog.ErrDescriptorNotFound) || isAdding {
			return nil, isAdding, sqlerrors.NewUndefinedRelationError(&tree.TableRef{TableID: int64(dataSourceID)})
		}
		return nil, false, err
	}

	// The name is only used for virtual tables, which can't be looked up by ID.
	ds, err := oc.dataSourceForDesc(ctx, cat.Flags{}, tableLookup, &tree.TableName{})
	return ds, false, err
}

// ResolveTypeByOID is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return oc.planner.ResolveTypeByOID(ctx, oid)
}

// ResolveType is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	return oc.planner.ResolveType(ctx, name)
}

func getDescFromCatalogObjectForPermissions(o cat.Object) (catalog.Descriptor, error) {
	switch t := o.(type) {
	case *optSchema:
		return t.getDescriptorForPermissionsCheck(), nil
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

func getDescForDataSource(o cat.DataSource) (catalog.TableDescriptor, error) {
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
	desc, err := getDescFromCatalogObjectForPermissions(o)
	if err != nil {
		return err
	}
	return oc.planner.CheckPrivilege(ctx, desc, priv)
}

// CheckAnyPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckAnyPrivilege(ctx context.Context, o cat.Object) error {
	desc, err := getDescFromCatalogObjectForPermissions(o)
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

// HasRoleOption is part of the cat.Catalog interface.
func (oc *optCatalog) HasRoleOption(
	ctx context.Context, roleOption roleoption.Option,
) (bool, error) {
	return oc.planner.HasRoleOption(ctx, roleOption)
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

	dbID := desc.GetParentID()
	dbDesc, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, oc.codec(), dbID)
	if err != nil {
		return cat.DataSourceName{}, err
	}
	scID := desc.GetParentSchemaID()
	var scName tree.Name
	if scID == keys.PublicSchemaID {
		scName = tree.PublicSchemaName
	} else {
		scDesc, err := catalogkv.MustGetSchemaDescByID(ctx, txn, oc.codec(), scID)
		if err != nil {
			return cat.DataSourceName{}, err
		}
		scName = tree.Name(scDesc.GetName())
	}

	return tree.MakeTableNameWithSchema(
			tree.Name(dbDesc.GetName()),
			scName,
			tree.Name(desc.GetName())),
		nil
}

// dataSourceForDesc returns a data source wrapper for the given descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) dataSourceForDesc(
	ctx context.Context, flags cat.Flags, desc catalog.TableDescriptor, name *cat.DataSourceName,
) (cat.DataSource, error) {
	// Because they are backed by physical data, we treat materialized views
	// as tables for the purposes of planning.
	if desc.IsTable() || desc.MaterializedView() {
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
	ctx context.Context, flags cat.Flags, desc catalog.TableDescriptor, name *cat.DataSourceName,
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
		tableStats, err = oc.planner.execCfg.TableStatsCache.GetTableStats(context.TODO(), desc.GetID())
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
	if ds, ok := oc.dataSources[desc]; ok && !ds.(*optTable).isStale(desc, tableStats, zoneConfig) {
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
func (oc *optCatalog) getZoneConfig(desc catalog.TableDescriptor) (*zonepb.ZoneConfig, error) {
	// Lookup table's zone if system config is available (it may not be as node
	// is starting up and before it's received the gossiped config). If it is
	// not available, use an empty config that has no zone constraints.
	if oc.cfg == nil || desc.IsVirtualTable() {
		return emptyZoneConfig, nil
	}
	zone, err := oc.cfg.GetZoneConfigForObject(oc.codec(), uint32(desc.GetID()))
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

// optView is a wrapper around catalog.TableDescriptor that implements
// the cat.Object, cat.DataSource, and cat.View interfaces.
type optView struct {
	desc catalog.TableDescriptor
}

var _ cat.View = &optView{}

func newOptView(desc catalog.TableDescriptor) *optView {
	return &optView{desc: desc}
}

// ID is part of the cat.Object interface.
func (ov *optView) ID() cat.StableID {
	return cat.StableID(ov.desc.GetID())
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ov *optView) PostgresDescriptorID() cat.StableID {
	return cat.StableID(ov.desc.GetID())
}

// Equals is part of the cat.Object interface.
func (ov *optView) Equals(other cat.Object) bool {
	otherView, ok := other.(*optView)
	if !ok {
		return false
	}
	return ov.desc.GetID() == otherView.desc.GetID() && ov.desc.GetVersion() == otherView.desc.GetVersion()
}

// Name is part of the cat.View interface.
func (ov *optView) Name() tree.Name {
	return tree.Name(ov.desc.GetName())
}

// IsSystemView is part of the cat.View interface.
func (ov *optView) IsSystemView() bool {
	return ov.desc.IsVirtualTable()
}

// Query is part of the cat.View interface.
func (ov *optView) Query() string {
	return ov.desc.GetViewQuery()
}

// ColumnNameCount is part of the cat.View interface.
func (ov *optView) ColumnNameCount() int {
	return len(ov.desc.PublicColumns())
}

// ColumnName is part of the cat.View interface.
func (ov *optView) ColumnName(i int) tree.Name {
	return ov.desc.PublicColumns()[i].ColName()
}

// CollectTypes is part of the cat.DataSource interface.
func (ov *optView) CollectTypes(ord int) (descpb.IDs, error) {
	col := ov.desc.AllColumns()[ord]
	return collectTypes(col)
}

// optSequence is a wrapper around catalog.TableDescriptor that
// implements the cat.Object and cat.DataSource interfaces.
type optSequence struct {
	desc catalog.TableDescriptor
}

var _ cat.DataSource = &optSequence{}
var _ cat.Sequence = &optSequence{}

func newOptSequence(desc catalog.TableDescriptor) *optSequence {
	return &optSequence{desc: desc}
}

// ID is part of the cat.Object interface.
func (os *optSequence) ID() cat.StableID {
	return cat.StableID(os.desc.GetID())
}

// PostgresDescriptorID is part of the cat.Object interface.
func (os *optSequence) PostgresDescriptorID() cat.StableID {
	return cat.StableID(os.desc.GetID())
}

// Equals is part of the cat.Object interface.
func (os *optSequence) Equals(other cat.Object) bool {
	otherSeq, ok := other.(*optSequence)
	if !ok {
		return false
	}
	return os.desc.GetID() == otherSeq.desc.GetID() && os.desc.GetVersion() == otherSeq.desc.GetVersion()
}

// Name is part of the cat.Sequence interface.
func (os *optSequence) Name() tree.Name {
	return tree.Name(os.desc.GetName())
}

// SequenceMarker is part of the cat.Sequence interface.
func (os *optSequence) SequenceMarker() {}

// CollectTypes is part of the cat.DataSource interface.
func (os *optSequence) CollectTypes(ord int) (descpb.IDs, error) {
	col := os.desc.AllColumns()[ord]
	return collectTypes(col)
}

// optTable is a wrapper around catalog.TableDescriptor that caches
// index wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc catalog.TableDescriptor

	// columns contains all the columns presented to the catalog. This includes:
	//  - ordinary table columns (those in the table descriptor)
	//  - MVCC timestamp system column
	//  - virtual columns (for inverted indexes).
	// They are stored in this order, though we shouldn't rely on that anywhere.
	columns []cat.Column

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

	uniqueConstraints []optUniqueConstraint

	outboundFKs []optForeignKeyConstraint
	inboundFKs  []optForeignKeyConstraint

	// checkConstraints is the set of check constraints for this table. It
	// can be different from desc's constraints because of synthesized
	// constraints for user defined types.
	checkConstraints []cat.CheckConstraint

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap catalog.TableColMap
}

var _ cat.Table = &optTable{}

func newOptTable(
	desc catalog.TableDescriptor,
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

	// First, determine how many columns we will potentially need.
	cols := ot.desc.DeletableColumns()
	numCols := len(ot.desc.AllColumns())
	// One for each inverted index virtual column.
	secondaryIndexes := ot.desc.DeletableNonPrimaryIndexes()
	for _, index := range secondaryIndexes {
		if index.GetType() == descpb.IndexDescriptor_INVERTED {
			numCols++
		}
	}

	ot.columns = make([]cat.Column, len(cols), numCols)
	for _, col := range cols {
		var kind cat.ColumnKind
		visibility := cat.Visible
		switch {
		case col.Public():
			kind = cat.Ordinary
			if col.IsInaccessible() {
				visibility = cat.Inaccessible
			} else if col.IsHidden() {
				visibility = cat.Hidden
			}
		case col.WriteAndDeleteOnly():
			kind = cat.WriteOnly
			visibility = cat.Inaccessible
		default:
			kind = cat.DeleteOnly
			visibility = cat.Inaccessible
		}
		if !col.IsVirtual() {
			ot.columns[col.Ordinal()].InitNonVirtual(
				col.Ordinal(),
				cat.StableID(col.GetID()),
				col.ColName(),
				kind,
				col.GetType(),
				col.IsNullable(),
				visibility,
				col.ColumnDesc().DefaultExpr,
				col.ColumnDesc().ComputeExpr,
			)
		} else {
			// Note: a WriteOnly or DeleteOnly mutation column doesn't require any
			// special treatment inside the optimizer, other than having the correct
			// visibility.
			ot.columns[col.Ordinal()].InitVirtualComputed(
				col.Ordinal(),
				cat.StableID(col.GetID()),
				col.ColName(),
				col.GetType(),
				col.IsNullable(),
				visibility,
				col.GetComputeExpr(),
			)
		}
	}

	newColumn := func() (col *cat.Column, ordinal int) {
		ordinal = len(ot.columns)
		ot.columns = ot.columns[:ordinal+1]
		return &ot.columns[ordinal], ordinal
	}

	// Set up any registered system columns. However, we won't add the column
	// in case a non-system column with the same name already exists in the table.
	// This check is done for migration purposes. We need to avoid adding the
	// system column if the table has a column with this name for some reason.
	for _, sysCol := range ot.desc.SystemColumns() {
		found, _ := desc.FindColumnWithName(sysCol.ColName())
		if found == nil || found.IsSystemColumn() {
			col, ord := newColumn()
			col.InitNonVirtual(
				ord,
				cat.StableID(sysCol.GetID()),
				sysCol.ColName(),
				cat.System,
				sysCol.GetType(),
				sysCol.IsNullable(),
				cat.MaybeHidden(sysCol.IsHidden()),
				sysCol.ColumnDesc().DefaultExpr,
				sysCol.ColumnDesc().ComputeExpr,
			)
		}
	}

	// Create the table's column mapping from descpb.ColumnID to column ordinal.
	for i := range ot.columns {
		ot.colMap.Set(descpb.ColumnID(ot.columns[i].ColID()), i)
	}

	// Add unique without index constraints. Constraints for implicitly
	// partitioned unique indexes will be added below.
	ot.uniqueConstraints = make([]optUniqueConstraint, 0, len(ot.desc.GetUniqueWithoutIndexConstraints()))
	for i := range ot.desc.GetUniqueWithoutIndexConstraints() {
		u := &ot.desc.GetUniqueWithoutIndexConstraints()[i]
		ot.uniqueConstraints = append(ot.uniqueConstraints, optUniqueConstraint{
			name:         u.Name,
			table:        ot.ID(),
			columns:      u.ColumnIDs,
			predicate:    u.Predicate,
			withoutIndex: true,
			validity:     u.Validity,
		})
	}

	// Build the indexes.
	ot.indexes = make([]optIndex, 1+len(secondaryIndexes))

	for i := range ot.indexes {
		var idx catalog.Index
		if i == 0 {
			idx = desc.GetPrimaryIndex()
		} else {
			idx = secondaryIndexes[i-1]
		}

		// If there is a subzone that applies to the entire index, use that, else
		// use the table zone. Save subzones that apply to partitions, since we will
		// use those later when initializing partitions in the index.
		idxZone := tblZone
		partZones := make(map[string]*zonepb.ZoneConfig)
		for j := range tblZone.Subzones {
			subzone := &tblZone.Subzones[j]
			if subzone.IndexID == uint32(idx.GetID()) {
				if subzone.PartitionName == "" {
					// Subzone applies to the whole index.
					copyZone := subzone.Config
					copyZone.InheritFromParent(tblZone)
					idxZone = &copyZone
				} else {
					// Subzone applies to a partition.
					copyZone := subzone.Config
					copyZone.InheritFromParent(tblZone)
					partZones[subzone.PartitionName] = &copyZone
				}
			}
		}
		if idx.GetType() == descpb.IndexDescriptor_INVERTED {
			// The last column of an inverted index is special: in the
			// descriptors, it looks as if the table column is part of the
			// index; in fact the key contains values *derived* from that
			// column. In the catalog, we refer to this key as a separate,
			// virtual column.
			invertedSourceColOrdinal, _ := ot.lookupColumnOrdinal(idx.GetKeyColumnID(idx.NumKeyColumns() - 1))

			// Add a virtual column that refers to the inverted index key.
			virtualCol, virtualColOrd := newColumn()

			// All virtual inverted columns have type bytes.
			typ := types.Bytes
			virtualCol.InitVirtualInverted(
				virtualColOrd,
				tree.Name(string(ot.Column(invertedSourceColOrdinal).ColName())+"_inverted_key"),
				typ,
				false, /* nullable */
				invertedSourceColOrdinal,
			)
			ot.indexes[i].init(ot, i, idx, idxZone, partZones, virtualColOrd)
		} else {
			ot.indexes[i].init(ot, i, idx, idxZone, partZones, -1 /* virtualColOrd */)
		}

		// Add unique constraints for implicitly partitioned unique indexes.
		if idx.IsUnique() && idx.GetPartitioning().NumImplicitColumns() > 0 {
			ot.uniqueConstraints = append(ot.uniqueConstraints, optUniqueConstraint{
				name:         idx.GetName(),
				table:        ot.ID(),
				columns:      idx.IndexDesc().KeyColumnIDs[idx.GetPartitioning().NumImplicitColumns():],
				withoutIndex: true,
				predicate:    idx.GetPredicate(),
				// TODO(rytaft): will we ever support an unvalidated unique constraint
				// here?
				validity: descpb.ConstraintValidity_Validated,
			})
		}
	}

	for i := range ot.desc.GetOutboundFKs() {
		fk := &ot.desc.GetOutboundFKs()[i]
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
	for i := range ot.desc.GetInboundFKs() {
		fk := &ot.desc.GetInboundFKs()[i]
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

	ot.primaryFamily.init(ot, &desc.GetFamilies()[0])
	ot.families = make([]optFamily, len(desc.GetFamilies())-1)
	for i := range ot.families {
		ot.families[i].init(ot, &desc.GetFamilies()[i+1])
	}

	// Synthesize any check constraints for user defined types.
	var synthesizedChecks []cat.CheckConstraint
	for i := 0; i < ot.ColumnCount(); i++ {
		col := ot.Column(i)
		if col.IsMutation() {
			// We do not synthesize check constraints for mutation columns.
			continue
		}
		colType := col.DatumType()
		if colType.UserDefined() {
			switch colType.Family() {
			case types.EnumFamily:
				// We synthesize an (x IN (v1, v2, v3...)) check for enum types.
				expr := &tree.ComparisonExpr{
					Operator: tree.MakeComparisonOperator(tree.In),
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
			Validated:  activeChecks[i].Validity == descpb.ConstraintValidity_Validated,
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
	return cat.StableID(ot.desc.GetID())
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ot *optTable) PostgresDescriptorID() cat.StableID {
	return cat.StableID(ot.desc.GetID())
}

// isStale checks if the optTable object needs to be refreshed because the stats,
// zone config, or used types have changed. False positives are ok.
func (ot *optTable) isStale(
	rawDesc catalog.TableDescriptor, tableStats []*stats.TableStatistic, zone *zonepb.ZoneConfig,
) bool {
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
	// Check if any of the version of column types have changed.
	if !catalog.UserDefinedTypeColsHaveSameVersion(ot.desc, rawDesc) {
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
	if ot.desc.GetID() != otherTable.desc.GetID() || ot.desc.GetVersion() != otherTable.desc.GetVersion() {
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

	// Verify that all of the user defined types in the table are the same.
	if !catalog.UserDefinedTypeColsHaveSameVersion(ot.desc, otherTable.desc) {
		return false
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
	return tree.Name(ot.desc.GetName())
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return false
}

// IsMaterializedView implements the cat.Table interface.
func (ot *optTable) IsMaterializedView() bool {
	return ot.desc.MaterializedView()
}

// ColumnCount is part of the cat.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.columns)
}

// Column is part of the cat.Table interface.
func (ot *optTable) Column(i int) *cat.Column {
	return &ot.columns[i]
}

// getCol is part of optCatalogTableInterface.
func (ot *optTable) getCol(i int) catalog.Column {
	if i < len(ot.desc.AllColumns()) {
		return ot.desc.AllColumns()[i]
	}
	return nil
}

// IndexCount is part of the cat.Table interface.
func (ot *optTable) IndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return len(ot.desc.ActiveIndexes())
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optTable) WritableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.WritableNonPrimaryIndexes())
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optTable) DeletableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return len(ot.desc.AllIndexes())
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

// UniqueCount is part of the cat.Table interface.
func (ot *optTable) UniqueCount() int {
	return len(ot.uniqueConstraints)
}

// Unique is part of the cat.Table interface.
func (ot *optTable) Unique(i cat.UniqueOrdinal) cat.UniqueConstraint {
	return &ot.uniqueConstraints[i]
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optTable) lookupColumnOrdinal(colID descpb.ColumnID) (int, error) {
	col, ok := ot.colMap.Get(colID)
	if ok {
		return col, nil
	}
	return col, pgerror.Newf(pgcode.UndefinedColumn,
		"column [%d] does not exist", colID)
}

// CollectTypes is part of the cat.DataSource interface.
func (ot *optTable) CollectTypes(ord int) (descpb.IDs, error) {
	col := ot.desc.AllColumns()[ord]
	return collectTypes(col)
}

// optIndex is a wrapper around catalog.Index that caches some
// commonly accessed information and keeps a reference to the table wrapper.
type optIndex struct {
	tab  *optTable
	idx  catalog.Index
	zone *zonepb.ZoneConfig

	// columnOrds maps the index columns to table column ordinals.
	columnOrds []int

	// storedCols is the set of non-PK columns if this is the primary index,
	// otherwise it is desc.StoreColumnIDs.
	storedCols []descpb.ColumnID

	indexOrdinal  int
	numCols       int
	numKeyCols    int
	numLaxKeyCols int

	// partitions stores zone information and datums for PARTITION BY LIST
	// partitions.
	partitions []optPartition

	// invertedVirtualColOrd is used if this is an inverted index; it stores the
	// ordinal of the virtual column created to refer to the key of this index.
	// It is -1 if this is not an inverted index.
	invertedVirtualColOrd int
}

var _ cat.Index = &optIndex{}

// init can be used instead of newOptIndex when we have a pre-allocated instance
// (e.g. as part of a bigger struct).
func (oi *optIndex) init(
	tab *optTable,
	indexOrdinal int,
	idx catalog.Index,
	zone *zonepb.ZoneConfig,
	partZones map[string]*zonepb.ZoneConfig,
	invertedVirtualColOrd int,
) {
	oi.tab = tab
	oi.idx = idx
	oi.zone = zone
	oi.indexOrdinal = indexOrdinal
	oi.invertedVirtualColOrd = invertedVirtualColOrd
	if idx.Primary() {
		// Although the primary index contains all columns in the table, the index
		// descriptor does not contain columns that are not explicitly part of the
		// primary key. Retrieve those columns from the table descriptor.
		oi.storedCols = make([]descpb.ColumnID, 0, tab.ColumnCount()-idx.NumKeyColumns())
		var pkCols util.FastIntSet
		for i := 0; i < idx.NumKeyColumns(); i++ {
			id := idx.GetKeyColumnID(i)
			pkCols.Add(int(id))
		}
		for i, n := 0, tab.ColumnCount(); i < n; i++ {
			if col := tab.Column(i); col.Kind() != cat.VirtualInverted && !col.IsVirtualComputed() {
				if id := col.ColID(); !pkCols.Contains(int(id)) {
					oi.storedCols = append(oi.storedCols, descpb.ColumnID(id))
				}
			}
		}
		oi.numCols = idx.NumKeyColumns() + len(oi.storedCols)
	} else {
		oi.storedCols = idx.IndexDesc().StoreColumnIDs
		oi.numCols = idx.NumKeyColumns() + idx.NumKeySuffixColumns() + idx.NumSecondaryStoredColumns()
	}

	// Collect information about the partitions.
	idxPartitioning := idx.GetPartitioning()
	oi.partitions = make([]optPartition, 0, idxPartitioning.NumLists())
	_ = idxPartitioning.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
		op := optPartition{
			name:   name,
			zone:   &zonepb.ZoneConfig{},
			datums: make([]tree.Datums, 0, len(values)),
		}

		// Get the zone.
		if zone, ok := partZones[name]; ok {
			op.zone = zone
		}

		// Get the partition values.
		var a rowenc.DatumAlloc
		for _, valueEncBuf := range values {
			t, _, err := rowenc.DecodePartitionTuple(
				&a, oi.tab.codec, oi.tab.desc, oi.idx, oi.idx.GetPartitioning(),
				valueEncBuf, nil, /* prefixDatums */
			)
			if err != nil {
				log.Fatalf(context.TODO(), "error while decoding partition tuple: %+v %+v",
					oi.tab.desc, oi.tab.desc.GetDependsOnTypes())
			}
			op.datums = append(op.datums, t.Datums)
			// TODO(radu): split into multiple prefixes if Subpartition is also by list.
			// Note that this functionality should be kept in sync with the test catalog
			// implementation (test_catalog.go).
		}

		oi.partitions = append(oi.partitions, op)
		return nil
	})

	if idx.IsUnique() {
		notNull := true
		for i := 0; i < idx.NumKeyColumns(); i++ {
			id := idx.GetKeyColumnID(i)
			ord, _ := tab.lookupColumnOrdinal(id)
			if tab.Column(ord).IsNullable() {
				notNull = false
				break
			}
		}

		if notNull {
			// Unique index with no null columns: columns from index are sufficient
			// to form a key without needing extra primary key columns. There is no
			// separate lax key.
			oi.numLaxKeyCols = idx.NumKeyColumns()
			oi.numKeyCols = oi.numLaxKeyCols
		} else {
			// Unique index with at least one nullable column: extra primary key
			// columns will be added to the row key when one of the unique index
			// columns has a NULL value.
			oi.numLaxKeyCols = idx.NumKeyColumns()
			oi.numKeyCols = oi.numLaxKeyCols + idx.NumKeySuffixColumns()
		}
	} else {
		// Non-unique index: extra primary key columns are always added to the row
		// key. There is no separate lax key.
		oi.numLaxKeyCols = idx.NumKeyColumns() + idx.NumKeySuffixColumns()
		oi.numKeyCols = oi.numLaxKeyCols
	}

	// Populate columnOrds.
	inverted := oi.IsInverted()
	numKeyCols := idx.NumKeyColumns()
	numKeySuffixCols := idx.NumKeySuffixColumns()
	oi.columnOrds = make([]int, oi.numCols)
	for i := 0; i < oi.numCols; i++ {
		var ord int
		switch {
		case inverted && i == numKeyCols-1:
			ord = oi.invertedVirtualColOrd
		case i < numKeyCols:
			ord, _ = oi.tab.lookupColumnOrdinal(oi.idx.GetKeyColumnID(i))
		case i < numKeyCols+numKeySuffixCols:
			ord, _ = oi.tab.lookupColumnOrdinal(oi.idx.GetKeySuffixColumnID(i - numKeyCols))
		default:
			ord, _ = oi.tab.lookupColumnOrdinal(oi.storedCols[i-numKeyCols-numKeySuffixCols])
		}
		oi.columnOrds[i] = ord
	}
}

// ID is part of the cat.Index interface.
func (oi *optIndex) ID() cat.StableID {
	return cat.StableID(oi.idx.GetID())
}

// Name is part of the cat.Index interface.
func (oi *optIndex) Name() tree.Name {
	return tree.Name(oi.idx.GetName())
}

// IsUnique is part of the cat.Index interface.
func (oi *optIndex) IsUnique() bool {
	return oi.idx.IsUnique()
}

// IsInverted is part of the cat.Index interface.
func (oi *optIndex) IsInverted() bool {
	return oi.idx.GetType() == descpb.IndexDescriptor_INVERTED
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

// NonInvertedPrefixColumnCount is part of the cat.Index interface.
func (oi *optIndex) NonInvertedPrefixColumnCount() int {
	if !oi.IsInverted() {
		panic("non-inverted indexes do not have inverted prefix columns")
	}
	return oi.idx.NumKeyColumns() - 1
}

// Column is part of the cat.Index interface.
func (oi *optIndex) Column(i int) cat.IndexColumn {
	ord := oi.columnOrds[i]
	// Only key columns have a direction.
	descending := i < oi.idx.NumKeyColumns() && oi.idx.GetKeyColumnDirection(i) == descpb.IndexDescriptor_DESC
	return cat.IndexColumn{
		Column:     oi.tab.Column(ord),
		Descending: descending,
	}
}

// VirtualInvertedColumn is part of the cat.Index interface.
func (oi *optIndex) VirtualInvertedColumn() cat.IndexColumn {
	if !oi.IsInverted() {
		panic(errors.AssertionFailedf("non-inverted indexes do not have inverted virtual columns"))
	}
	ord := oi.idx.NumKeyColumns() - 1
	return oi.Column(ord)
}

// Predicate is part of the cat.Index interface. It returns the predicate
// expression and true if the index is a partial index. If the index is not
// partial, the empty string and false is returned.
func (oi *optIndex) Predicate() (string, bool) {
	return oi.idx.GetPredicate(), oi.idx.GetPredicate() != ""
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
	if desc.GetID() <= keys.MaxSystemConfigDescID {
		return keys.SystemConfigSpan
	}
	return desc.IndexSpan(oi.tab.codec, oi.idx.GetID())
}

// Table is part of the cat.Index interface.
func (oi *optIndex) Table() cat.Table {
	return oi.tab
}

// Ordinal is part of the cat.Index interface.
func (oi *optIndex) Ordinal() int {
	return oi.indexOrdinal
}

// ImplicitPartitioningColumnCount is part of the cat.Index interface.
func (oi *optIndex) ImplicitPartitioningColumnCount() int {
	return oi.idx.GetPartitioning().NumImplicitColumns()
}

// InterleaveAncestorCount is part of the cat.Index interface.
func (oi *optIndex) InterleaveAncestorCount() int {
	return oi.idx.NumInterleaveAncestors()
}

// InterleaveAncestor is part of the cat.Index interface.
func (oi *optIndex) InterleaveAncestor(i int) (table, index cat.StableID, numKeyCols int) {
	a := oi.idx.GetInterleaveAncestor(i)
	return cat.StableID(a.TableID), cat.StableID(a.IndexID), int(a.SharedPrefixLen)
}

// InterleavedByCount is part of the cat.Index interface.
func (oi *optIndex) InterleavedByCount() int {
	return oi.idx.NumInterleavedBy()
}

// InterleavedBy is part of the cat.Index interface.
func (oi *optIndex) InterleavedBy(i int) (table, index cat.StableID) {
	ref := oi.idx.GetInterleavedBy(i)
	return cat.StableID(ref.Table), cat.StableID(ref.Index)
}

// GeoConfig is part of the cat.Index interface.
func (oi *optIndex) GeoConfig() *geoindex.Config {
	return &oi.idx.IndexDesc().GeoConfig
}

// Version is part of the cat.Index interface.
func (oi *optIndex) Version() descpb.IndexDescriptorVersion {
	return oi.idx.GetVersion()
}

// PartitionCount is part of the cat.Index interface.
func (oi *optIndex) PartitionCount() int {
	return len(oi.partitions)
}

// Partition is part of the cat.Index interface.
func (oi *optIndex) Partition(i int) cat.Partition {
	return &oi.partitions[i]
}

// optPartition implements cat.Partition and represents a PARTITION BY LIST
// partition of an index.
type optPartition struct {
	name   string
	zone   *zonepb.ZoneConfig
	datums []tree.Datums
}

var _ cat.Partition = &optPartition{}

// Name is part of the cat.Partition interface.
func (op *optPartition) Name() string {
	return op.name
}

// Zone is part of the cat.Partition interface.
func (op *optPartition) Zone() cat.Zone {
	return op.zone
}

// PartitionByListPrefixes is part of the cat.Partition interface.
func (op *optPartition) PartitionByListPrefixes() []tree.Datums {
	return op.datums
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
		os.columnOrdinals[i], ok = tab.colMap.Get(c)
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

// optFamily is a wrapper around descpb.ColumnFamilyDescriptor that keeps a
// reference to the table wrapper.
type optFamily struct {
	tab  *optTable
	desc *descpb.ColumnFamilyDescriptor
}

var _ cat.Family = &optFamily{}

// init can be used instead of newOptFamily when we have a pre-allocated
// instance (e.g. as part of a bigger struct).
func (oi *optFamily) init(tab *optTable, desc *descpb.ColumnFamilyDescriptor) {
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

// optUniqueConstraint implements cat.UniqueConstraint and represents a
// unique constraint.
type optUniqueConstraint struct {
	name string

	table     cat.StableID
	columns   []descpb.ColumnID
	predicate string

	withoutIndex bool
	validity     descpb.ConstraintValidity
}

var _ cat.UniqueConstraint = &optUniqueConstraint{}

// Name is part of the cat.UniqueConstraint interface.
func (u *optUniqueConstraint) Name() string {
	return u.name
}

// TableID is part of the cat.UniqueConstraint interface.
func (u *optUniqueConstraint) TableID() cat.StableID {
	return u.table
}

// ColumnCount is part of the cat.UniqueConstraint interface.
func (u *optUniqueConstraint) ColumnCount() int {
	return len(u.columns)
}

// ColumnOrdinal is part of the cat.UniqueConstraint interface.
func (u *optUniqueConstraint) ColumnOrdinal(tab cat.Table, i int) int {
	if tab.ID() != u.table {
		panic(errors.AssertionFailedf(
			"invalid table %d passed to ColumnOrdinal (expected %d)",
			tab.ID(), u.table,
		))
	}
	optTab := tab.(*optTable)
	ord, _ := optTab.lookupColumnOrdinal(u.columns[i])
	return ord
}

// Predicate is part of the cat.UniqueConstraint interface.
func (u *optUniqueConstraint) Predicate() (string, bool) {
	return u.predicate, u.predicate != ""
}

// WithoutIndex is part of the cat.UniqueConstraint interface.
func (u *optUniqueConstraint) WithoutIndex() bool {
	return u.withoutIndex
}

// Validated is part of the cat.UniqueConstraint interface.
func (u *optUniqueConstraint) Validated() bool {
	return u.validity == descpb.ConstraintValidity_Validated
}

// optForeignKeyConstraint implements cat.ForeignKeyConstraint and represents a
// foreign key relationship. Both the origin and the referenced table store the
// same optForeignKeyConstraint (as an outbound and inbound reference,
// respectively).
type optForeignKeyConstraint struct {
	name string

	originTable   cat.StableID
	originColumns []descpb.ColumnID

	referencedTable   cat.StableID
	referencedColumns []descpb.ColumnID

	validity     descpb.ConstraintValidity
	match        descpb.ForeignKeyReference_Match
	deleteAction descpb.ForeignKeyReference_Action
	updateAction descpb.ForeignKeyReference_Action
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
	return fk.validity == descpb.ConstraintValidity_Validated
}

// MatchMethod is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) MatchMethod() tree.CompositeKeyMatchMethod {
	return descpb.ForeignKeyReferenceMatchValue[fk.match]
}

// DeleteReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) DeleteReferenceAction() tree.ReferenceAction {
	return descpb.ForeignKeyReferenceActionType[fk.deleteAction]
}

// UpdateReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) UpdateReferenceAction() tree.ReferenceAction {
	return descpb.ForeignKeyReferenceActionType[fk.updateAction]
}

// optVirtualTable is similar to optTable but is used with virtual tables.
type optVirtualTable struct {
	desc catalog.TableDescriptor

	// columns contains all the columns presented to the catalog. This includes
	// the dummy PK column and the columns in the table descriptor.
	columns []cat.Column

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
	colMap catalog.TableColMap
}

var _ cat.Table = &optVirtualTable{}

func newOptVirtualTable(
	ctx context.Context, oc *optCatalog, desc catalog.TableDescriptor, name *cat.DataSourceName,
) (*optVirtualTable, error) {
	// Calculate the stable ID (see the comment for optVirtualTable.id).
	id := cat.StableID(desc.GetID())
	if name.Catalog() != "" {
		// TODO(radu): it's unfortunate that we have to lookup the schema again.
		found, prefix, err := oc.planner.LookupSchema(ctx, name.Catalog(), name.Schema())
		if err != nil {
			return nil, err
		}
		if !found {
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
			id |= cat.StableID(prefix.Database.GetID()) << 32
		}
	}

	ot := &optVirtualTable{
		desc: desc,
		id:   id,
		name: *name,
	}

	ot.columns = make([]cat.Column, len(desc.PublicColumns())+1)
	// Init dummy PK column.
	ot.columns[0].InitNonVirtual(
		0,
		math.MaxInt64, /* stableID */
		"crdb_internal_vtable_pk",
		cat.Ordinary,
		types.Int,
		false,      /* nullable */
		cat.Hidden, /* hidden */
		nil,        /* defaultExpr */
		nil,        /* computedExpr */
	)
	for i, d := range desc.PublicColumns() {
		ot.columns[i+1].InitNonVirtual(
			i+1,
			cat.StableID(d.GetID()),
			tree.Name(d.GetName()),
			cat.Ordinary,
			d.GetType(),
			d.IsNullable(),
			cat.MaybeHidden(d.IsHidden()),
			d.ColumnDesc().DefaultExpr,
			d.ColumnDesc().ComputeExpr,
		)
	}

	// Create the table's column mapping from descpb.ColumnID to column ordinal.
	for i := range ot.columns {
		ot.colMap.Set(descpb.ColumnID(ot.columns[i].ColID()), i)
	}

	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	ot.family.init(ot)

	// Build the indexes (add 1 to account for lack of primary index in
	// indexes slice).
	ot.indexes = make([]optVirtualIndex, len(ot.desc.ActiveIndexes()))
	// Set up the primary index.
	ot.indexes[0] = optVirtualIndex{
		tab:          ot,
		indexOrdinal: 0,
		numCols:      ot.ColumnCount(),
	}

	for _, idx := range ot.desc.PublicNonPrimaryIndexes() {
		if idx.NumKeyColumns() > 1 {
			panic(errors.AssertionFailedf("virtual indexes with more than 1 col not supported"))
		}

		// Add 1, since the 0th index will the primary that we added above.
		ot.indexes[idx.Ordinal()] = optVirtualIndex{
			tab:          ot,
			idx:          idx,
			indexOrdinal: idx.Ordinal(),
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
	return cat.StableID(ot.desc.GetID())
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
	if ot.id != otherTable.id || ot.desc.GetVersion() != otherTable.desc.GetVersion() {
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

// IsMaterializedView implements the cat.Table interface.
func (ot *optVirtualTable) IsMaterializedView() bool {
	return false
}

// ColumnCount is part of the cat.Table interface.
func (ot *optVirtualTable) ColumnCount() int {
	return len(ot.columns)
}

// Column is part of the cat.Table interface.
func (ot *optVirtualTable) Column(i int) *cat.Column {
	return &ot.columns[i]
}

// getCol is part of optCatalogTableInterface.
func (ot *optVirtualTable) getCol(i int) catalog.Column {
	if i > 0 && i <= len(ot.desc.PublicColumns()) {
		return ot.desc.PublicColumns()[i-1]
	}
	return nil
}

// IndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) IndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return len(ot.desc.ActiveIndexes())
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) WritableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.WritableNonPrimaryIndexes())
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) DeletableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return len(ot.desc.AllIndexes())
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
	panic(errors.AssertionFailedf("no stats"))
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
		Validated:  check.Validity == descpb.ConstraintValidity_Validated,
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
	panic(errors.AssertionFailedf("no FKs"))
}

// InboundForeignKeyCount is part of the cat.Table interface.
func (ot *optVirtualTable) InboundForeignKeyCount() int {
	return 0
}

// InboundForeignKey is part of the cat.Table interface.
func (ot *optVirtualTable) InboundForeignKey(i int) cat.ForeignKeyConstraint {
	panic(errors.AssertionFailedf("no FKs"))
}

// UniqueCount is part of the cat.Table interface.
func (ot *optVirtualTable) UniqueCount() int {
	return 0
}

// Unique is part of the cat.Table interface.
func (ot *optVirtualTable) Unique(i cat.UniqueOrdinal) cat.UniqueConstraint {
	panic(errors.AssertionFailedf("no unique constraints"))
}

// CollectTypes is part of the cat.DataSource interface.
func (ot *optVirtualTable) CollectTypes(ord int) (descpb.IDs, error) {
	col := ot.desc.AllColumns()[ord]
	return collectTypes(col)
}

// optVirtualIndex is a dummy implementation of cat.Index for the indexes
// reported by a virtual table. The index assumes that table column 0 is a dummy
// PK column.
type optVirtualIndex struct {
	tab *optVirtualTable

	// idx is set to nil if this is the dummy PK index for virtual tables.
	idx catalog.Index

	numCols int

	indexOrdinal int
}

// ID is part of the cat.Index interface.
func (oi *optVirtualIndex) ID() cat.StableID {
	if oi.idx == nil {
		// Dummy PK index ID.
		return cat.StableID(0)
	}
	return cat.StableID(oi.idx.GetID())
}

// Name is part of the cat.Index interface.
func (oi *optVirtualIndex) Name() tree.Name {
	if oi.idx == nil {
		// Dummy PK index name.
		return "primary"
	}
	return tree.Name(oi.idx.GetName())
}

// IsUnique is part of the cat.Index interface.
func (oi *optVirtualIndex) IsUnique() bool {
	if oi.idx == nil {
		// Dummy PK index is not explicitly UNIQUE.
		return false
	}
	return oi.idx.IsUnique()
}

// IsInverted is part of the cat.Index interface.
func (oi *optVirtualIndex) IsInverted() bool {
	return false
}

// ColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) ColumnCount() int {
	return oi.numCols
}

// KeyColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) KeyColumnCount() int {
	// Virtual indexes for the time being always have exactly 2 key columns,
	// because they're only constructable on a single column, and we don't support
	// the concept of a unique virtual index. So, we always export 2 key columns:
	// the first is the column to be indexed, and the second is the fake virtual
	// index column that we pretend exists to guarantee uniqueness. See the
	// implementation of optVirtualIndex.Column().
	return 2
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) LaxKeyColumnCount() int {
	// Virtual indexes are never unique, so their lax key is the same as their
	// key.
	return 2
}

// NonInvertedPrefixColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) NonInvertedPrefixColumnCount() int {
	panic("virtual indexes are not inverted")
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optVirtualTable) lookupColumnOrdinal(colID descpb.ColumnID) (int, error) {
	col, ok := ot.colMap.Get(colID)
	if ok {
		return col, nil
	}
	return col, pgerror.Newf(pgcode.UndefinedColumn,
		"column [%d] does not exist", colID)
}

// Column is part of the cat.Index interface.
func (oi *optVirtualIndex) Column(i int) cat.IndexColumn {
	if oi.idx == nil {
		// Dummy PK index columns.
		return cat.IndexColumn{Column: oi.tab.Column(i)}
	}
	length := oi.idx.NumKeyColumns()
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.idx.GetKeyColumnID(i))
		return cat.IndexColumn{
			Column: oi.tab.Column(ord),
		}
	}
	if i == length {
		// The special bogus PK column goes at the end of the index columns. It
		// has ID 0.
		return cat.IndexColumn{Column: oi.tab.Column(0)}
	}

	i -= length + 1
	ord, _ := oi.tab.lookupColumnOrdinal(oi.idx.GetStoredColumnID(i))
	return cat.IndexColumn{Column: oi.tab.Column(ord)}
}

// VirtualInvertedColumn is part of the cat.Index interface.
func (oi *optVirtualIndex) VirtualInvertedColumn() cat.IndexColumn {
	panic(errors.AssertionFailedf("virtual indexes are not inverted"))
}

// Predicate is part of the cat.Index interface.
func (oi *optVirtualIndex) Predicate() (string, bool) {
	return "", false
}

// Zone is part of the cat.Index interface.
func (oi *optVirtualIndex) Zone() cat.Zone {
	panic(errors.AssertionFailedf("no zone"))
}

// Span is part of the cat.Index interface.
func (oi *optVirtualIndex) Span() roachpb.Span {
	panic(errors.AssertionFailedf("no span"))
}

// Table is part of the cat.Index interface.
func (oi *optVirtualIndex) Table() cat.Table {
	return oi.tab
}

// Ordinal is part of the cat.Index interface.
func (oi *optVirtualIndex) Ordinal() int {
	return oi.indexOrdinal
}

// ImplicitPartitioningColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) ImplicitPartitioningColumnCount() int {
	return 0
}

// InterleaveAncestorCount is part of the cat.Index interface.
func (oi *optVirtualIndex) InterleaveAncestorCount() int {
	return 0
}

// InterleaveAncestor is part of the cat.Index interface.
func (oi *optVirtualIndex) InterleaveAncestor(i int) (table, index cat.StableID, numKeyCols int) {
	panic(errors.AssertionFailedf("no interleavings"))
}

// InterleavedByCount is part of the cat.Index interface.
func (oi *optVirtualIndex) InterleavedByCount() int {
	return 0
}

// InterleavedBy is part of the cat.Index interface.
func (oi *optVirtualIndex) InterleavedBy(i int) (table, index cat.StableID) {
	panic(errors.AssertionFailedf("no interleavings"))
}

// GeoConfig is part of the cat.Index interface.
func (oi *optVirtualIndex) GeoConfig() *geoindex.Config {
	return nil
}

// Version is part of the cat.Index interface.
func (oi *optVirtualIndex) Version() descpb.IndexDescriptorVersion {
	return 0
}

// PartitionCount is part of the cat.Index interface.
func (oi *optVirtualIndex) PartitionCount() int {
	return 0
}

// Partition is part of the cat.Index interface.
func (oi *optVirtualIndex) Partition(i int) cat.Partition {
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

type optCatalogTableInterface interface {
	// getCol returns the catalog.Column interface backing a given column,
	// (or nil if it is a virtual column).
	getCol(i int) catalog.Column
}

var _ optCatalogTableInterface = &optTable{}
var _ optCatalogTableInterface = &optVirtualTable{}

// collectTypes walks the given column's default and computed expression,
// and collects any user defined types it finds. If the column itself is of
// a user defined type, it will also be added to the set of user defined types.
func collectTypes(col catalog.Column) (descpb.IDs, error) {
	visitor := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}
	addOIDsInExpr := func(exprStr string) error {
		expr, err := parser.ParseExpr(exprStr)
		if err != nil {
			return err
		}
		tree.WalkExpr(visitor, expr)
		return nil
	}

	// Collect UDTs in default expression, computed column and the column type itself.
	if col.HasDefault() {
		if err := addOIDsInExpr(col.GetDefaultExpr()); err != nil {
			return nil, err
		}
	}
	if col.IsComputed() {
		if err := addOIDsInExpr(col.GetComputeExpr()); err != nil {
			return nil, err
		}
	}
	if typ := col.GetType(); typ != nil && typ.UserDefined() {
		visitor.OIDs[typ.Oid()] = struct{}{}
	}

	ids := make(descpb.IDs, 0, len(visitor.OIDs))
	for collectedOid := range visitor.OIDs {
		id, err := typedesc.UserDefinedTypeOIDToID(collectedOid)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
