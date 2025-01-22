// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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

var _ cat.Catalog = (*optCatalog)(nil)

// optPlanningCatalog is a thin wrapper over cat.Catalog
// with few additional planner specific methods.
type optPlanningCatalog interface {
	cat.Catalog
	init(planner *planner)
	reset()
	fullyQualifiedNameWithTxn(
		ctx context.Context, ds cat.DataSource, txn *kv.Txn,
	) (cat.DataSourceName, error)
}

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
	return cat.StableID(os.PostgresDescriptorID())
}

// PostgresDescriptorID is part of the cat.Object interface.
func (os *optSchema) PostgresDescriptorID() catid.DescID {
	switch os.schema.SchemaKind() {
	case catalog.SchemaUserDefined, catalog.SchemaTemporary:
		// User defined schemas and the temporary schema have real ID's, so use
		// them here.
		return os.schema.GetID()
	default:
		// Virtual schemas and the public schema don't, so just fall back to the
		// parent database's ID.
		return os.database.GetID()
	}
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
	return os.planner.GetObjectNamesAndIDs(ctx, os.database, os.schema)
}

func (os *optSchema) getDescriptorForPermissionsCheck() catalog.Descriptor {
	// If the schema is backed by a descriptor, then return it.
	if os.schema.SchemaKind() == catalog.SchemaUserDefined {
		return os.schema
	}
	// Otherwise, just return the database descriptor.
	return os.database
}

// LookupDatabaseName implements the cat.Catalog interface.
func (oc *optCatalog) LookupDatabaseName(
	ctx context.Context, flags cat.Flags, name string,
) (tree.Name, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.skipDescriptorCache = prev
		}(oc.planner.skipDescriptorCache)
		oc.planner.skipDescriptorCache = true
	}
	if name == "" {
		name = oc.planner.CurrentDatabase()
	}
	if err := oc.planner.LookupDatabase(ctx, name); err != nil {
		return "", err
	}
	return tree.Name(name), nil
}

// ResolveSchema is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveSchema(
	ctx context.Context, flags cat.Flags, name *cat.SchemaName,
) (cat.Schema, cat.SchemaName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.skipDescriptorCache = prev
		}(oc.planner.skipDescriptorCache)
		oc.planner.skipDescriptorCache = true
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

// GetAllSchemaNamesForDB is part of the cat.Catalog interface.
func (oc *optCatalog) GetAllSchemaNamesForDB(
	ctx context.Context, dbName string,
) ([]cat.SchemaName, error) {
	schemas, err := oc.planner.GetSchemasForDB(ctx, dbName)
	if err != nil {
		return nil, err
	}

	var schemaNames []cat.SchemaName
	for _, name := range schemas {
		var scName cat.SchemaName
		scName.SchemaName = tree.Name(name)
		scName.ExplicitSchema = true
		scName.CatalogName = tree.Name(dbName)
		scName.ExplicitCatalog = true
		schemaNames = append(schemaNames, scName)
	}

	return schemaNames, nil
}

// ResolveDataSource is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSource(
	ctx context.Context, flags cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.skipDescriptorCache = prev
		}(oc.planner.skipDescriptorCache)
		oc.planner.skipDescriptorCache = true
	}

	oc.tn = *name
	lflags := tree.ObjectLookupFlags{
		Required:          true,
		DesiredObjectKind: tree.TableObject,
	}
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

// ResolveIndex is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveIndex(
	ctx context.Context, flags cat.Flags, name *tree.TableIndexName,
) (cat.Index, cat.DataSourceName, error) {
	var prefix catalog.ResolvedObjectPrefix
	var tbl catalog.TableDescriptor
	var idx catalog.Index
	var err error
	oc.planner.runWithOptions(resolveFlags{skipCache: flags.AvoidDescriptorCaches}, func() {
		_, prefix, tbl, idx, err = resolver.ResolveIndex(
			ctx,
			oc.planner,
			name,
			tree.IndexLookupFlags{
				Required:              true,
				IncludeNonActiveIndex: flags.IncludeNonActiveIndexes,
				IncludeOfflineTable:   flags.IncludeOfflineTables,
			},
		)
	})
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	if err := oc.planner.canResolveDescUnderSchema(ctx, prefix.Schema, tbl); err != nil {
		return nil, cat.DataSourceName{}, err
	}

	namePrefix := prefix.NamePrefix()
	oc.tn = tree.MakeTableNameWithSchema(namePrefix.CatalogName, namePrefix.SchemaName, tree.Name(tbl.GetName()))

	ds, err := oc.dataSourceForDesc(ctx, flags, tbl, &oc.tn)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}

	table, ok := ds.(cat.Table)
	if !ok {
		return nil, cat.DataSourceName{}, pgerror.Newf(
			pgcode.InvalidParameterValue, "%q is not a table or materialized view", ds.Name(),
		)
	}

	return table.Index(idx.Ordinal()), oc.tn, nil
}

// ResolveDataSourceByID is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSourceByID(
	ctx context.Context, flags cat.Flags, dataSourceID cat.StableID,
) (_ cat.DataSource, isAdding bool, _ error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.skipDescriptorCache = prev
		}(oc.planner.skipDescriptorCache)
		oc.planner.skipDescriptorCache = true
	}

	tableLookup, err := oc.planner.LookupTableByID(ctx, descpb.ID(dataSourceID))

	if err != nil {
		isAdding := catalog.HasAddingDescriptorError(err)
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

// ResolveFunction is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveFunction(
	ctx context.Context, name tree.UnresolvedRoutineName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	return oc.planner.ResolveFunction(ctx, name, path)
}

func (oc *optCatalog) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (*tree.RoutineName, *tree.Overload, error) {
	return oc.planner.ResolveFunctionByOID(ctx, oid)
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
func (oc *optCatalog) CheckPrivilege(
	ctx context.Context, o cat.Object, user username.SQLUsername, priv privilege.Kind,
) error {
	if o.ID() == cat.DefaultStableID {
		return oc.planner.CheckPrivilegeForUser(ctx, syntheticprivilege.GlobalPrivilegeObject, priv, user)
	}
	desc, err := getDescFromCatalogObjectForPermissions(o)
	if err != nil {
		return err
	}
	return oc.planner.CheckPrivilegeForUser(ctx, desc, priv, user)
}

// CheckAnyPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckAnyPrivilege(ctx context.Context, o cat.Object) error {
	desc, err := getDescFromCatalogObjectForPermissions(o)
	if err != nil {
		return err
	}
	return oc.planner.CheckAnyPrivilege(ctx, desc)
}

// CheckExecutionPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckExecutionPrivilege(
	ctx context.Context, oid oid.Oid, user username.SQLUsername,
) error {
	desc, err := oc.planner.FunctionDesc(ctx, oid)
	if err != nil {
		return errors.WithAssertionFailure(err)
	}
	return oc.planner.CheckPrivilegeForUser(ctx, desc, privilege.EXECUTE, user)
}

// HasAdminRole is part of the cat.Catalog interface.
func (oc *optCatalog) HasAdminRole(ctx context.Context) (bool, error) {
	return oc.planner.HasAdminRole(ctx)
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
	dbDesc, err := oc.planner.Descriptors().ByIDWithoutLeased(txn).WithoutNonPublic().Get().Database(ctx, dbID)
	if err != nil {
		return cat.DataSourceName{}, err
	}
	scID := desc.GetParentSchemaID()
	var scName tree.Name
	// TODO(richardjcai): Remove this in 22.2.
	if scID == keys.PublicSchemaID {
		scName = catconstants.PublicSchemaName
	} else {
		scDesc, err := oc.planner.Descriptors().ByIDWithoutLeased(txn).WithoutNonPublic().Get().Schema(ctx, scID)
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

// CheckRoleExists is part of the cat.Catalog interface.
func (oc *optCatalog) CheckRoleExists(ctx context.Context, role username.SQLUsername) error {
	return oc.planner.CheckRoleExists(ctx, role)
}

// Optimizer is part of the cat.Catalog interface.
func (oc *optCatalog) Optimizer() interface{} {
	if oc.planner == nil {
		return nil
	}
	plannerInterface := eval.Planner(oc.planner)
	return plannerInterface.Optimizer()
}

// GetCurrentUser is part of the cat.Catalog interface.
func (oc *optCatalog) GetCurrentUser() username.SQLUsername {
	return oc.planner.User()
}

// GetDependencyDigest is part of the cat.Catalog interface.
func (oc *optCatalog) GetDependencyDigest() cat.DependencyDigest {
	// The stats cache may not be setup in some tests like
	// TestPortalsDestroyedOnTxnFinish. In which case always
	// return the empty digest.
	if oc.planner.ExecCfg().TableStatsCache == nil {
		return cat.DependencyDigest{}
	}
	return cat.DependencyDigest{
		LeaseGeneration: oc.planner.Descriptors().GetLeaseGeneration(),
		StatsGeneration: oc.planner.execCfg.TableStatsCache.GetGeneration(),
		SystemConfig:    oc.planner.execCfg.SystemConfig.GetSystemConfig(),
		CurrentDatabase: oc.planner.CurrentDatabase(),
		SearchPath:      oc.planner.SessionData().SearchPath,
		CurrentUser:     oc.planner.User(),
	}
}

// GetRoutineOwner is part of the cat.Catalog interface.
func (oc *optCatalog) GetRoutineOwner(
	ctx context.Context, routineOid oid.Oid,
) (username.SQLUsername, error) {
	fnDesc, err := oc.planner.FunctionDesc(ctx, routineOid)
	if err != nil {
		return username.EmptyRoleName(), err
	}
	return fnDesc.FuncDesc().Privileges.Owner(), nil
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
		var typeResolver *descs.DistSQLTypeResolver
		if p := oc.planner; p != nil {
			r := descs.NewDistSQLTypeResolver(p.Descriptors(), p.Txn())
			typeResolver = &r
		}
		var err error
		tableStats, err = oc.planner.execCfg.TableStatsCache.GetTableStats(ctx, desc, typeResolver)
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

	ds, err := newOptTable(ctx, desc, oc.codec(), tableStats, zoneConfig)
	if err != nil {
		return nil, err
	}
	oc.dataSources[desc] = ds
	return ds, nil
}

var emptyZoneConfig = cat.EmptyZone()

// getZoneConfig returns the ZoneConfig data structure for the given table.
// ZoneConfigs are stored in protobuf binary format in the SystemConfig, which
// is gossiped around the cluster. Note that the returned ZoneConfig might be
// somewhat stale, since it's taken from the gossiped SystemConfig.
func (oc *optCatalog) getZoneConfig(desc catalog.TableDescriptor) (cat.Zone, error) {
	// Lookup table's zone if system config is available (it may not be as node
	// is starting up and before it's received the gossiped config). If it is
	// not available, use an empty config that has no zone constraints.
	if oc.cfg == nil || desc.IsVirtualTable() {
		return emptyZoneConfig, nil
	}
	zone, err := oc.cfg.GetZoneConfigForObject(
		oc.codec(), config.ObjectID(desc.GetID()),
	)
	if err != nil {
		return nil, err
	}
	if zone == nil {
		// This can happen with tests that override the hook.
		return emptyZoneConfig, nil
	}
	return cat.AsZone(zone), nil
}

func (oc *optCatalog) codec() keys.SQLCodec {
	return oc.planner.ExecCfg().Codec
}

// optView is a wrapper around catalog.TableDescriptor that implements
// the cat.Object, cat.DataSource, and cat.View interfaces.
type optView struct {
	desc     catalog.TableDescriptor
	triggers []optTrigger
}

var _ cat.View = &optView{}

func newOptView(desc catalog.TableDescriptor) *optView {
	return &optView{
		desc:     desc,
		triggers: getOptTriggers(desc.GetTriggers()),
	}
}

// ID is part of the cat.Object interface.
func (ov *optView) ID() cat.StableID {
	return cat.StableID(ov.desc.GetID())
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ov *optView) PostgresDescriptorID() catid.DescID {
	return ov.desc.GetID()
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

// TriggerCount is part of the cat.View interface.
func (ov *optView) TriggerCount() int {
	return len(ov.triggers)
}

// Trigger is part of the cat.View interface.
func (ov *optView) Trigger(i int) cat.Trigger {
	return &ov.triggers[i]
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
func (os *optSequence) PostgresDescriptorID() catid.DescID {
	return os.desc.GetID()
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
	//  - inverted columns
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

	zone cat.Zone

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
	checkConstraints []optCheckConstraint

	triggers []optTrigger

	// Row-level security (RLS) fields
	rlsEnabled bool
	policies   map[tree.PolicyType][]optPolicy

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap catalog.TableColMap
}

var _ cat.Table = &optTable{}

func newOptTable(
	ctx context.Context,
	desc catalog.TableDescriptor,
	codec keys.SQLCodec,
	stats []*stats.TableStatistic,
	tblZone cat.Zone,
) (*optTable, error) {
	ot := &optTable{
		desc:     desc,
		codec:    codec,
		rawStats: stats,
		zone:     tblZone,
	}

	// Determine the primary key columns.
	pkCols := desc.GetPrimaryIndex().CollectKeyColumnIDs()

	// Determine how many columns we will potentially need.
	cols := ot.desc.DeletableColumns()
	numCols := len(ot.desc.AllColumns())
	// Add one for each inverted index column.
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
		// Primary key columns that are virtual in the descriptor are considered
		// "stored" from the perspective of the optimizer because they are
		// written to the primary index and all secondary indexes.
		if !col.IsVirtual() || pkCols.Contains(col.GetID()) {
			cd := col.ColumnDesc()
			ot.columns[col.Ordinal()].Init(
				col.Ordinal(),
				cat.StableID(col.GetID()),
				col.ColName(),
				kind,
				col.GetType(),
				col.IsNullable(),
				visibility,
				cd.DefaultExpr,
				cd.ComputeExpr,
				cd.OnUpdateExpr,
				mapGeneratedAsIdentityType(col.GetGeneratedAsIdentityType()),
				cd.GeneratedAsIdentitySequenceOption,
			)
		} else {
			// We need to propagate the mutation state for computed columns, so that
			// the optimizer can correctly determine if these columns should be
			// accessible. We need this to be propagated since virtual columns may
			// depend on other columns, and in cascaded drop operations their accessibility
			// will be impacted.
			ot.columns[col.Ordinal()].InitVirtualComputed(
				col.Ordinal(),
				cat.StableID(col.GetID()),
				col.ColName(),
				kind,
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
	// This check is done for upgrade purposes. We need to avoid adding the
	// system column if the table has a column with this name for some reason.
	for _, sysCol := range ot.desc.SystemColumns() {
		found := catalog.FindColumnByTreeName(desc, sysCol.ColName())
		if found == nil || found.IsSystemColumn() {
			col, ord := newColumn()
			cd := sysCol.ColumnDesc()
			col.Init(
				ord,
				cat.StableID(sysCol.GetID()),
				sysCol.ColName(),
				cat.System,
				sysCol.GetType(),
				sysCol.IsNullable(),
				cat.MaybeHidden(sysCol.IsHidden()),
				cd.DefaultExpr,
				cd.ComputeExpr,
				cd.OnUpdateExpr,
				mapGeneratedAsIdentityType(sysCol.GetGeneratedAsIdentityType()),
				cd.GeneratedAsIdentitySequenceOption,
			)
		}
	}

	// Create the table's column mapping from descpb.ColumnID to column ordinal.
	for i := range ot.columns {
		ot.colMap.Set(descpb.ColumnID(ot.columns[i].ColID()), i)
	}

	// Add unique without index constraints. Constraints for implicitly
	// partitioned unique indexes will be added below.
	ot.uniqueConstraints = make([]optUniqueConstraint, len(ot.desc.EnforcedUniqueConstraintsWithoutIndex()))
	for i, u := range ot.desc.EnforcedUniqueConstraintsWithoutIndex() {
		ot.uniqueConstraints[i] = optUniqueConstraint{
			name:         u.GetName(),
			table:        ot.ID(),
			columns:      u.CollectKeyColumnIDs().Ordered(),
			predicate:    u.GetPredicate(),
			withoutIndex: true,
			validity:     u.GetConstraintValidity(),
		}
	}

	// Build the indexes.
	ot.indexes = make([]optIndex, 1+len(secondaryIndexes))
	// partZones is allocated lazily and is reused for all indexes.
	var partZones map[string]cat.Zone

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
		for k := range partZones {
			delete(partZones, k)
		}
		for j := 0; j < tblZone.SubzoneCount(); j++ {
			subzone := tblZone.Subzone(j)
			if subzone.Index() == cat.StableID(idx.GetID()) {
				copyZone := subzone.Zone().InheritFromParent(tblZone)
				if subzone.Partition() == "" {
					// Subzone applies to the whole index.
					idxZone = copyZone
				} else {
					// Subzone applies to a partition.
					if partZones == nil {
						partZones = make(map[string]cat.Zone)
					}
					partZones[subzone.Partition()] = copyZone
				}
			}
		}
		if idx.GetType() == descpb.IndexDescriptor_INVERTED {
			// The inverted column of an inverted index is special: in the
			// descriptors, it looks as if the table column is part of the
			// index; in fact the key contains values *derived* from that
			// column. In the catalog, we refer to this key as a separate,
			// inverted column.
			invertedColumnID := idx.InvertedColumnID()
			invertedColumnName := idx.InvertedColumnName()
			invertedColumnType := idx.InvertedColumnKeyType()

			invertedSourceColOrdinal, _ := ot.lookupColumnOrdinal(invertedColumnID)

			// Add an inverted column that refers to the inverted index key.
			invertedCol, invertedColOrd := newColumn()
			invertedCol.InitInverted(
				invertedColOrd,
				tree.Name(invertedColumnName+"_inverted_key"),
				invertedColumnType,
				false, /* nullable */
				invertedSourceColOrdinal,
			)
			ot.indexes[i].init(ot, i, idx, idxZone, partZones, invertedColOrd)
		} else {
			ot.indexes[i].init(ot, i, idx, idxZone, partZones, -1 /* invertedColOrd */)
		}

		if idx.IsUnique() {
			if idx.ImplicitPartitioningColumnCount() > 0 {
				// Add unique constraints for implicitly partitioned unique indexes. If
				// there is a single implicit column of ENUM type (e.g. an implicit RBR
				// column), then we can ensure uniqueness under non-Serializable
				// isolation levels by writing tombstones. We assume that the partition
				// column is the first column of the index.
				partitionColumn := catalog.FindColumnByID(desc, idx.GetKeyColumnID(0 /* columnOrdinal */))
				canUseTombstones := idx.ImplicitPartitioningColumnCount() == 1 &&
					partitionColumn.GetType().Family() == types.EnumFamily
				ot.uniqueConstraints = append(ot.uniqueConstraints, optUniqueConstraint{
					name:                  idx.GetName(),
					table:                 ot.ID(),
					columns:               idx.IndexDesc().KeyColumnIDs[idx.IndexDesc().ExplicitColumnStartIdx():],
					withoutIndex:          true,
					canUseTombstones:      canUseTombstones,
					tombstoneIndexOrdinal: idx.Ordinal(),
					predicate:             idx.GetPredicate(),
					// TODO(rytaft): will we ever support an unvalidated unique constraint
					// here?
					validity: descpb.ConstraintValidity_Validated,
				})
			} else if idx.IsSharded() {
				// Add unique constraint for hash sharded indexes.
				ot.uniqueConstraints = append(ot.uniqueConstraints, optUniqueConstraint{
					name:                               idx.GetName(),
					table:                              ot.ID(),
					columns:                            idx.IndexDesc().KeyColumnIDs[idx.IndexDesc().ExplicitColumnStartIdx():],
					withoutIndex:                       true,
					predicate:                          idx.GetPredicate(),
					validity:                           descpb.ConstraintValidity_Validated,
					uniquenessGuaranteedByAnotherIndex: true,
				})
			}
		}
	}

	for _, fk := range ot.desc.OutboundForeignKeys() {
		ot.outboundFKs = append(ot.outboundFKs, optForeignKeyConstraint{
			name:              fk.GetName(),
			originTable:       ot.ID(),
			originColumns:     fk.ForeignKeyDesc().OriginColumnIDs,
			referencedTable:   cat.StableID(fk.GetReferencedTableID()),
			referencedColumns: fk.ForeignKeyDesc().ReferencedColumnIDs,
			validity:          fk.GetConstraintValidity(),
			match:             tree.CompositeKeyMatchMethodType[fk.Match()],
			deleteAction:      tree.ForeignKeyReferenceActionType[fk.OnDelete()],
			updateAction:      tree.ForeignKeyReferenceActionType[fk.OnUpdate()],
		})
	}
	for _, fk := range ot.desc.InboundForeignKeys() {
		ot.inboundFKs = append(ot.inboundFKs, optForeignKeyConstraint{
			name:              fk.GetName(),
			originTable:       cat.StableID(fk.GetOriginTableID()),
			originColumns:     fk.ForeignKeyDesc().OriginColumnIDs,
			referencedTable:   ot.ID(),
			referencedColumns: fk.ForeignKeyDesc().ReferencedColumnIDs,
			validity:          fk.GetConstraintValidity(),
			match:             tree.CompositeKeyMatchMethodType[fk.Match()],
			deleteAction:      tree.ForeignKeyReferenceActionType[fk.OnDelete()],
			updateAction:      tree.ForeignKeyReferenceActionType[fk.OnUpdate()],
		})
	}

	ot.primaryFamily.init(ot, &desc.GetFamilies()[0])
	ot.families = make([]optFamily, len(desc.GetFamilies())-1)
	for i := range ot.families {
		ot.families[i].init(ot, &desc.GetFamilies()[i+1])
	}

	// Synthesize any check constraints for user defined types.
	var synthesizedChecks []optCheckConstraint
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
					Operator: treecmp.MakeComparisonOperator(treecmp.In),
					Left:     &tree.ColumnItem{ColumnName: col.ColName()},
					Right:    tree.NewDTuple(colType, tree.MakeAllDEnumsInType(colType)...),
				}
				synthesizedChecks = append(synthesizedChecks, optCheckConstraint{
					constraint:  tree.Serialize(expr),
					validated:   true,
					columnCount: 1,
					lookupColumnOrdinal: func(i int) (int, error) {
						return col.Ordinal(), nil
					},
				})
			}
		}
	}
	// Move all existing and synthesized checks into the opt table.
	activeChecks := desc.EnforcedCheckConstraints()
	ot.checkConstraints = make([]optCheckConstraint, 0, len(activeChecks)+len(synthesizedChecks))
	for i := range activeChecks {
		check := activeChecks[i]
		ot.checkConstraints = append(ot.checkConstraints, optCheckConstraint{
			constraint:  check.GetExpr(),
			validated:   check.GetConstraintValidity() == descpb.ConstraintValidity_Validated,
			columnCount: len(check.CheckDesc().ColumnIDs),
			lookupColumnOrdinal: func(j int) (int, error) {
				return ot.lookupColumnOrdinal(check.CheckDesc().ColumnIDs[j])
			},
		})
	}
	ot.checkConstraints = append(ot.checkConstraints, synthesizedChecks...)

	// Move all triggers into the opt table.
	ot.triggers = getOptTriggers(desc.GetTriggers())

	// Store row-level security information
	// TODO(136717): Update this to utilize the tableDescriptor's field for
	// indicating if RLS is enabled once the field is added.
	ot.rlsEnabled = false
	ot.policies = getOptPolicies(desc.GetPolicies())

	// Add stats last, now that other metadata is initialized.
	if stats != nil {
		ot.stats = make([]optTableStat, len(stats))
		n := 0
		for i := range stats {
			// We skip any stats that have columns that don't exist in the table anymore.
			if ok, err := ot.stats[n].init(ctx, ot, stats[i]); err != nil {
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
func (ot *optTable) PostgresDescriptorID() catid.DescID {
	return ot.desc.GetID()
}

// isStale checks if the optTable object needs to be refreshed because the stats,
// zone config, or used types have changed. False positives are ok.
func (ot *optTable) isStale(
	rawDesc catalog.TableDescriptor, tableStats []*stats.TableStatistic, zone cat.Zone,
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
	var prevLeftZone, prevRightZone cat.Zone
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

// IsSystemTable is part of the cat.Table interface.
func (ot *optTable) IsSystemTable() bool {
	return catalog.IsSystemDescriptor(ot.desc)
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
	return len(ot.desc.DeletableNonPrimaryIndexes()) + 1
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
	return &ot.checkConstraints[i]
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

// OutboundForeignKey is part of the cat.Table interface.
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

// Zone is part of the cat.Table interface.
func (ot *optTable) Zone() cat.Zone {
	return ot.zone
}

// IsPartitionAllBy is part of the cat.Table interface.
func (ot *optTable) IsPartitionAllBy() bool {
	return ot.desc.IsPartitionAllBy()
}

// HomeRegion is part of the cat.Table interface.
func (ot *optTable) HomeRegion() (region string, ok bool) {
	localityConfig := ot.desc.GetLocalityConfig()
	if localityConfig == nil {
		return "", false
	}
	regionalByTable := localityConfig.GetRegionalByTable()
	if regionalByTable == nil {
		return "", false
	}
	if regionalByTable.Region != nil {
		return regionalByTable.Region.String(), true
	}
	if ot.zone.LeasePreferenceCount() != 1 {
		return "", false
	}
	if ot.zone.LeasePreference(0).ConstraintCount() != 1 {
		return "", false
	}
	if ot.zone.LeasePreference(0).Constraint(0).GetKey() != "region" {
		return "", false
	}
	return ot.zone.LeasePreference(0).Constraint(0).GetValue(), true
}

// IsGlobalTable is part of the cat.Table interface.
func (ot *optTable) IsGlobalTable() bool {
	localityConfig := ot.desc.GetLocalityConfig()
	if localityConfig == nil {
		return false
	}
	return localityConfig.GetGlobal() != nil
}

// IsRegionalByRow is part of the cat.Table interface.
func (ot *optTable) IsRegionalByRow() bool {
	localityConfig := ot.desc.GetLocalityConfig()
	if localityConfig == nil {
		return false
	}
	return localityConfig.GetRegionalByRow() != nil
}

// IsMultiregion is part of the cat.Table interface.
func (ot *optTable) IsMultiregion() bool {
	localityConfig := ot.desc.GetLocalityConfig()
	return localityConfig != nil
}

// HomeRegionColName is part of the cat.Table interface.
func (ot *optTable) HomeRegionColName() (colName string, ok bool) {
	localityConfig := ot.desc.GetLocalityConfig()
	if localityConfig == nil {
		return "", false
	}
	regionalByRowConfig := localityConfig.GetRegionalByRow()
	if regionalByRowConfig == nil {
		return "", false
	}
	if regionalByRowConfig.As == nil {
		return "crdb_region", true
	}
	return *regionalByRowConfig.As, true
}

// GetDatabaseID is part of the cat.Table interface.
func (ot *optTable) GetDatabaseID() descpb.ID {
	return ot.desc.GetParentID()
}

// IsHypothetical is part of the cat.Table interface.
func (ot *optTable) IsHypothetical() bool {
	return false
}

// TriggerCount is part of the cat.Table interface.
func (ot *optTable) TriggerCount() int {
	return len(ot.triggers)
}

// Trigger is part of the cat.Table interface.
func (ot *optTable) Trigger(i int) cat.Trigger {
	return &ot.triggers[i]
}

// IsRowLevelSecurityEnabled is part of the cat.Table interface.
func (ot *optTable) IsRowLevelSecurityEnabled() bool { return ot.rlsEnabled }

// PolicyCount is part of the cat.Table interface
func (ot *optTable) PolicyCount(polType tree.PolicyType) int { return len(ot.policies[polType]) }

// Policy is part of the cat.Table interface
func (ot *optTable) Policy(polType tree.PolicyType, i int) cat.Policy {
	return &ot.policies[polType][i]
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

// convertTableToOptTable converts a table to an *optTable. This is either an
// *optTable or a *indexrec.HypotheticalTable (which has an embedded *optTable).
func convertTableToOptTable(tab cat.Table) *optTable {
	var optTab *optTable
	switch t := tab.(type) {
	case *optTable:
		optTab = t
	case *indexrec.HypotheticalTable:
		optTab = t.Table.(*optTable)
	}
	return optTab
}

// CollectTypes is part of the cat.DataSource interface.
func (ot *optTable) CollectTypes(ord int) (descpb.IDs, error) {
	col := ot.desc.AllColumns()[ord]
	return collectTypes(col)
}

// IsRefreshViewRequired is part of the cat.Table interface.
func (ot *optTable) IsRefreshViewRequired() bool {
	return ot.desc.IsRefreshViewRequired()
}

// optIndex is a wrapper around catalog.Index that caches some
// commonly accessed information and keeps a reference to the table wrapper.
type optIndex struct {
	tab  *optTable
	idx  catalog.Index
	zone cat.Zone

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

	// invertedColOrd is used if this is an inverted index; it stores
	// the ordinal of the inverted column created to refer to the key of this
	// index. It is -1 if this is not an inverted index.
	invertedColOrd int
}

var _ cat.Index = &optIndex{}

// init can be used instead of newOptIndex when we have a pre-allocated instance
// (e.g. as part of a bigger struct).
// partZones can be nil when no partition zone information is present.
func (oi *optIndex) init(
	tab *optTable,
	indexOrdinal int,
	idx catalog.Index,
	zone cat.Zone,
	partZones map[string]cat.Zone,
	invertedColOrd int,
) {
	oi.tab = tab
	oi.idx = idx
	oi.zone = zone
	oi.indexOrdinal = indexOrdinal
	oi.invertedColOrd = invertedColOrd
	if idx.Primary() {
		// Although the primary index contains all columns in the table, the index
		// descriptor does not contain columns that are not explicitly part of the
		// primary key. Retrieve those columns from the table descriptor.
		oi.storedCols = make([]descpb.ColumnID, 0, tab.ColumnCount()-idx.NumKeyColumns())
		pkCols := idx.CollectKeyColumnIDs()
		for i, n := 0, tab.ColumnCount(); i < n; i++ {
			if col := tab.Column(i); col.Kind() != cat.Inverted && !col.IsVirtualComputed() {
				if id := descpb.ColumnID(col.ColID()); !pkCols.Contains(id) {
					oi.storedCols = append(oi.storedCols, id)
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
			zone:   cat.EmptyZone(),
			datums: make([]tree.Datums, 0, len(values)),
		}

		// Get the zone.
		if partZones != nil {
			if zone, ok := partZones[name]; ok {
				op.zone = zone
			}
		}

		// Get the partition values.
		var a tree.DatumAlloc
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
			ord = oi.invertedColOrd
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

// IsVector is part of the cat.Index interface.
func (oi *optIndex) IsVector() bool {
	// TODO(#137370): check the index type.
	return false
}

// GetInvisibility is part of the cat.Index interface.
func (oi *optIndex) GetInvisibility() float64 {
	return oi.idx.GetInvisibility()
}

// ColumnCount is part of the cat.Index interface.
func (oi *optIndex) ColumnCount() int {
	return oi.numCols
}

// ExplicitColumnCount is part of the cat.Index interface.
func (oi *optIndex) ExplicitColumnCount() int {
	return oi.idx.NumKeyColumns()
}

// KeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) KeyColumnCount() int {
	return oi.numKeyCols
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) LaxKeyColumnCount() int {
	return oi.numLaxKeyCols
}

// PrefixColumnCount is part of the cat.Index interface.
func (oi *optIndex) PrefixColumnCount() int {
	if !oi.IsInverted() && !oi.IsVector() {
		panic(errors.AssertionFailedf("only inverted and vector indexes have prefix columns"))
	}
	return oi.idx.NumKeyColumns() - 1
}

// Column is part of the cat.Index interface.
func (oi *optIndex) Column(i int) cat.IndexColumn {
	ord := oi.columnOrds[i]
	// Only key columns have a direction.
	descending := i < oi.idx.NumKeyColumns() && oi.idx.GetKeyColumnDirection(i) == catenumpb.IndexColumn_DESC
	return cat.IndexColumn{
		Column:     oi.tab.Column(ord),
		Descending: descending,
	}
}

// InvertedColumn is part of the cat.Index interface.
func (oi *optIndex) InvertedColumn() cat.IndexColumn {
	if !oi.IsInverted() {
		panic(errors.AssertionFailedf("non-inverted indexes do not have inverted columns"))
	}
	ord := oi.idx.NumKeyColumns() - 1
	return oi.Column(ord)
}

// VectorColumn is part of the cat.Index interface.
func (oi *optIndex) VectorColumn() cat.IndexColumn {
	if !oi.IsVector() {
		panic(errors.AssertionFailedf("non-vector indexes do not have inverted columns"))
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
	return desc.IndexSpan(oi.tab.codec, oi.idx.GetID())
}

// Table is part of the cat.Index interface.
func (oi *optIndex) Table() cat.Table {
	return oi.tab
}

// Ordinal is part of the cat.Index interface.
func (oi *optIndex) Ordinal() cat.IndexOrdinal {
	return oi.indexOrdinal
}

// ImplicitColumnCount is part of the cat.Index interface.
func (oi *optIndex) ImplicitColumnCount() int {
	implicitColCnt := oi.idx.ImplicitPartitioningColumnCount()
	if oi.idx.IsSharded() {
		implicitColCnt++
	}
	return implicitColCnt
}

// ImplicitPartitioningColumnCount is part of the cat.Index interface.
func (oi *optIndex) ImplicitPartitioningColumnCount() int {
	return oi.idx.ImplicitPartitioningColumnCount()
}

// GeoConfig is part of the cat.Index interface.
func (oi *optIndex) GeoConfig() geopb.Config {
	return oi.idx.IndexDesc().GeoConfig
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
	zone   cat.Zone
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

// optCheckConstraint implements cat.CheckConstraint. See that interface
// for more information on the fields.
type optCheckConstraint struct {
	constraint  string
	validated   bool
	columnCount int

	// lookupColumnOrdinal returns the table column ordinal of the ith column in
	// this constraint.
	lookupColumnOrdinal func(i int) (int, error)
}

var _ cat.CheckConstraint = &optCheckConstraint{}

// Constraint is part of the cat.CheckConstraint interface.
func (oc *optCheckConstraint) Constraint() string {
	return oc.constraint
}

// Validated is part of the cat.CheckConstraint interface.
func (oc *optCheckConstraint) Validated() bool {
	return oc.validated
}

// ColumnCount is part of the cat.CheckConstraint interface.
func (oc *optCheckConstraint) ColumnCount() int {
	return oc.columnCount
}

// ColumnOrdinal is part of the cat.CheckConstraint interface.
func (oc *optCheckConstraint) ColumnOrdinal(i int) int {
	ord, err := oc.lookupColumnOrdinal(i)
	if err != nil {
		panic(err)
	}
	return ord
}

type optTableStat struct {
	stat           *stats.TableStatistic
	columnOrdinals []int
}

var _ cat.TableStatistic = &optTableStat{}

func (os *optTableStat) init(
	ctx context.Context, tab *optTable, stat *stats.TableStatistic,
) (ok bool, _ error) {
	os.stat = stat
	os.columnOrdinals = make([]int, len(stat.ColumnIDs))
	for i, c := range stat.ColumnIDs {
		var ok bool
		os.columnOrdinals[i], ok = tab.colMap.Get(c)
		if !ok {
			// Column not in table (this is expected if the column was removed since
			// the statistic was calculated).
			return false, nil
		}
	}

	// Verify that histogram column type matches table column type.
	// TODO(49698): When we support multi-column histograms this check will need
	// adjustment.
	if len(os.columnOrdinals) == 1 {
		col := tab.getCol(os.columnOrdinals[0])
		if err := stat.HistogramData.TypeCheck(
			col.GetType(), string(tab.Name()), col.GetName(), stats.TSFromTime(stat.CreatedAt),
		); err != nil {
			// Column type in the histogram differs from column type in the
			// table. This is only possible if we somehow re-used the same column ID
			// during an ALTER TABLE statement, which we shouldn't.
			if buildutil.CrdbTestBuild {
				return false, errors.NewAssertionErrorWithWrappedErrf(
					err, "type check failed while initializing stat %d", stat.StatisticID,
				)
			}
			// For release builds, skip over the stat and log a warning.
			log.Warningf(ctx, "skipping stat %d due to failed type check: %v", stat.StatisticID, err)
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

// AvgSize is part of the cat.TableStatistic interface.
func (os *optTableStat) AvgSize() uint64 {
	return os.stat.AvgSize
}

// Histogram is part of the cat.TableStatistic interface.
func (os *optTableStat) Histogram() []cat.HistogramBucket {
	return os.stat.Histogram
}

// HistogramType is part of the cat.TableStatistic interface.
func (os *optTableStat) HistogramType() *types.T {
	return os.stat.HistogramData.ColumnType
}

// IsPartial is part of the cat.TableStatistic interface.
func (os *optTableStat) IsPartial() bool {
	return os.stat.IsPartial()
}

// IsMerged is part of the cat.TableStatistic interface.
func (os *optTableStat) IsMerged() bool {
	return os.stat.IsMerged()
}

// IsForecast is part of the cat.TableStatistic interface.
func (os *optTableStat) IsForecast() bool {
	return os.stat.IsForecast()
}

// IsAuto is part of the cat.TableStatistic interface.
func (os *optTableStat) IsAuto() bool {
	return os.stat.IsAuto()
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

	withoutIndex          bool
	canUseTombstones      bool
	tombstoneIndexOrdinal cat.IndexOrdinal
	validity              descpb.ConstraintValidity

	uniquenessGuaranteedByAnotherIndex bool
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
	optTab := convertTableToOptTable(tab)
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

func (u *optUniqueConstraint) TombstoneIndexOrdinal() (ordinal cat.IndexOrdinal, ok bool) {
	ok = u.canUseTombstones
	if ok {
		ordinal = u.tombstoneIndexOrdinal
	} else {
		ordinal = -1
	}
	return ordinal, ok
}

// Validated is part of the cat.UniqueConstraint interface.
func (u *optUniqueConstraint) Validated() bool {
	return u.validity == descpb.ConstraintValidity_Validated
}

// UniquenessGuaranteedByAnotherIndex is part of the cat.UniqueConstraint
// interface. It is a hack to make unique hash sharded index work before issue
// #75070 is resolved. Be sure to remove `ignoreUniquenessCheck` field from
// `optUniqueConstraint` struct when dropping this hack.
func (u *optUniqueConstraint) UniquenessGuaranteedByAnotherIndex() bool {
	return u.uniquenessGuaranteedByAnotherIndex
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
	match        tree.CompositeKeyMatchMethod
	deleteAction tree.ReferenceAction
	updateAction tree.ReferenceAction
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

	tab := convertTableToOptTable(originTable)
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
	tab := convertTableToOptTable(referencedTable)
	ord, _ := tab.lookupColumnOrdinal(fk.referencedColumns[i])
	return ord
}

// Validated is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) Validated() bool {
	return fk.validity == descpb.ConstraintValidity_Validated
}

// MatchMethod is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) MatchMethod() tree.CompositeKeyMatchMethod {
	return fk.match
}

// DeleteReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) DeleteReferenceAction() tree.ReferenceAction {
	return fk.deleteAction
}

// UpdateReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) UpdateReferenceAction() tree.ReferenceAction {
	return fk.updateAction
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
	ot.columns[0].Init(
		0,
		math.MaxInt64, /* stableID */
		"crdb_internal_vtable_pk",
		cat.Ordinary,
		types.Int,
		false,      /* nullable */
		cat.Hidden, /* hidden */
		nil,        /* defaultExpr */
		nil,        /* computedExpr */
		nil,        /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil, /* generatedAsIdentitySequenceOption */
	)
	for i, d := range desc.PublicColumns() {
		cd := d.ColumnDesc()
		ot.columns[i+1].Init(
			i+1,
			cat.StableID(d.GetID()),
			tree.Name(d.GetName()),
			cat.Ordinary,
			d.GetType(),
			d.IsNullable(),
			cat.MaybeHidden(d.IsHidden()),
			cd.DefaultExpr,
			cd.ComputeExpr,
			cd.OnUpdateExpr,
			mapGeneratedAsIdentityType(d.GetGeneratedAsIdentityType()),
			cd.GeneratedAsIdentitySequenceOption,
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
func (ot *optVirtualTable) PostgresDescriptorID() catid.DescID {
	return ot.desc.GetID()
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

// IsSystemTable is part of the cat.Table interface.
func (ot *optVirtualTable) IsSystemTable() bool {
	return false
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
	return len(ot.desc.EnforcedCheckConstraints())
}

// Check is part of the cat.Table interface.
func (ot *optVirtualTable) Check(i int) cat.CheckConstraint {
	check := ot.desc.EnforcedCheckConstraints()[i]
	return &optCheckConstraint{
		constraint:  check.GetExpr(),
		validated:   check.GetConstraintValidity() == descpb.ConstraintValidity_Validated,
		columnCount: len(check.CheckDesc().ColumnIDs),
		lookupColumnOrdinal: func(j int) (int, error) {
			return ot.lookupColumnOrdinal(check.CheckDesc().ColumnIDs[j])
		},
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

// OutboundForeignKey is part of the cat.Table interface.
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

// Zone is part of the cat.Table interface.
func (ot *optVirtualTable) Zone() cat.Zone {
	panic(errors.AssertionFailedf("no zone"))
}

// IsPartitionAllBy is part of the cat.Table interface.
func (ot *optVirtualTable) IsPartitionAllBy() bool {
	return false
}

// HomeRegion is part of the cat.Table interface.
func (ot *optVirtualTable) HomeRegion() (region string, ok bool) {
	return "", false
}

// IsGlobalTable is part of the cat.Table interface.
func (ot *optVirtualTable) IsGlobalTable() bool {
	return false
}

// IsRegionalByRow is part of the cat.Table interface.
func (ot *optVirtualTable) IsRegionalByRow() bool {
	return false
}

// IsMultiregion is part of the cat.Table interface.
func (ot *optVirtualTable) IsMultiregion() bool {
	return false
}

// HomeRegionColName is part of the cat.Table interface.
func (ot *optVirtualTable) HomeRegionColName() (colName string, ok bool) {
	return "", false
}

// GetDatabaseID is part of the cat.Table interface.
func (ot *optVirtualTable) GetDatabaseID() descpb.ID {
	return 0
}

// IsHypothetical is part of the cat.Table interface.
func (ot *optVirtualTable) IsHypothetical() bool {
	return false
}

// CollectTypes is part of the cat.DataSource interface.
func (ot *optVirtualTable) CollectTypes(ord int) (descpb.IDs, error) {
	col := ot.desc.AllColumns()[ord]
	return collectTypes(col)
}

// IsRefreshViewRequired is part of the cat.Table interface.
func (ot *optVirtualTable) IsRefreshViewRequired() bool {
	return false
}

// TriggerCount is part of the cat.Table interface.
func (ot *optVirtualTable) TriggerCount() int {
	return 0
}

// Trigger is part of the cat.Table interface.
func (ot *optVirtualTable) Trigger(i int) cat.Trigger {
	panic(errors.AssertionFailedf("no triggers"))
}

// IsRowLevelSecurityEnabled is part of the cat.Table interface.
func (ot *optVirtualTable) IsRowLevelSecurityEnabled() bool { return false }

// PolicyCount is part of the cat.Table interface
func (ot *optVirtualTable) PolicyCount(polType tree.PolicyType) int { return 0 }

// Policy is part of the cat.Table interface
func (ot *optVirtualTable) Policy(polType tree.PolicyType, i int) cat.Policy {
	panic(errors.AssertionFailedf("no policies"))
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

// IsVector is part of the cat.Index interface.
func (oi *optVirtualIndex) IsVector() bool {
	return false
}

// GetInvisibility is part of the cat.Index interface.
func (oi *optVirtualIndex) GetInvisibility() float64 {
	return 0.0
}

// ExplicitColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) ExplicitColumnCount() int {
	return oi.idx.NumKeyColumns()
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

// PrefixColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) PrefixColumnCount() int {
	panic(errors.AssertionFailedf("virtual indexes cannot be inverted or vector indexes"))
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

// InvertedColumn is part of the cat.Index interface.
func (oi *optVirtualIndex) InvertedColumn() cat.IndexColumn {
	panic(errors.AssertionFailedf("virtual indexes are not inverted"))
}

// VectorColumn is part of the cat.Index interface.
func (oi *optVirtualIndex) VectorColumn() cat.IndexColumn {
	panic(errors.AssertionFailedf("virtual indexes cannot be vector indexes"))
}

// Predicate is part of the cat.Index interface.
func (oi *optVirtualIndex) Predicate() (string, bool) {
	if oi.idx == nil {
		return "", false
	}
	pred := oi.idx.GetPredicate()
	return pred, pred != ""
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
func (oi *optVirtualIndex) Ordinal() cat.IndexOrdinal {
	return oi.indexOrdinal
}

// ImplicitColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) ImplicitColumnCount() int {
	return 0
}

// ImplicitPartitioningColumnCount is part of the cat.Index interface.
func (oi *optVirtualIndex) ImplicitPartitioningColumnCount() int {
	return 0
}

// GeoConfig is part of the cat.Index interface.
func (oi *optVirtualIndex) GeoConfig() geopb.Config {
	return geopb.Config{}
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

// optTrigger is a wrapper around descpb.TriggerDescriptor that implements the
// cat.Trigger interface.
type optTrigger struct {
	name               tree.Name
	actionTime         tree.TriggerActionTime
	events             []tree.TriggerEvent
	newTransitionAlias tree.Name
	oldTransitionAlias tree.Name
	forEachRow         bool
	whenExpr           string
	funcID             cat.StableID
	funcArgs           tree.Datums
	funcBody           string
	enabled            bool
}

var _ cat.Trigger = &optTrigger{}

// Name is part of the cat.Trigger interface.
func (o *optTrigger) Name() tree.Name {
	return o.name
}

// ActionTime is part of the cat.Trigger interface.
func (o *optTrigger) ActionTime() tree.TriggerActionTime {
	return o.actionTime
}

// EventCount is part of the cat.Trigger interface.
func (o *optTrigger) EventCount() int {
	return len(o.events)
}

// Event is part of the cat.Trigger interface.
func (o *optTrigger) Event(i int) tree.TriggerEvent {
	return o.events[i]
}

// NewTransitionAlias is part of the cat.Trigger interface.
func (o *optTrigger) NewTransitionAlias() tree.Name {
	return o.newTransitionAlias
}

// OldTransitionAlias is part of the cat.Trigger interface.
func (o *optTrigger) OldTransitionAlias() tree.Name {
	return o.oldTransitionAlias
}

// ForEachRow is part of the cat.Trigger interface.
func (o *optTrigger) ForEachRow() bool {
	return o.forEachRow
}

// WhenExpr is part of the cat.Trigger interface.
func (o *optTrigger) WhenExpr() string {
	return o.whenExpr
}

// FuncID is part of the cat.Trigger interface.
func (o *optTrigger) FuncID() cat.StableID {
	return o.funcID
}

// FuncArgs is part of the cat.Trigger interface.
func (o *optTrigger) FuncArgs() tree.Datums {
	return o.funcArgs
}

func (o *optTrigger) FuncBody() string {
	return o.funcBody
}

// Enabled is part of the cat.Trigger interface.
func (o *optTrigger) Enabled() bool {
	return o.enabled
}

// getOptTriggers maps from descpb.TriggerDescriptor to optTrigger.
func getOptTriggers(descTriggers []descpb.TriggerDescriptor) []optTrigger {
	triggers := make([]optTrigger, len(descTriggers))
	for i := range triggers {
		descTrigger := &descTriggers[i]
		optEvents := make([]tree.TriggerEvent, len(descTrigger.Events))
		for j := range optEvents {
			descEvent := descTrigger.Events[j]
			optEvents[j].EventType = tree.TriggerEventTypeToTree[descEvent.Type]
			optEvents[j].Columns = make(tree.NameList, 0, len(descEvent.ColumnNames))
			for _, colName := range descEvent.ColumnNames {
				optEvents[j].Columns = append(optEvents[j].Columns, tree.Name(colName))
			}
		}
		funcArgs := make(tree.Datums, len(descTrigger.FuncArgs))
		for j := range funcArgs {
			funcArgs[j] = tree.NewDString(descTrigger.FuncArgs[j])
		}
		triggers[i] = optTrigger{
			name:               tree.Name(descTrigger.Name),
			actionTime:         tree.TriggerActionTimeToTree[descTrigger.ActionTime],
			events:             optEvents,
			newTransitionAlias: tree.Name(descTrigger.NewTransitionAlias),
			oldTransitionAlias: tree.Name(descTrigger.OldTransitionAlias),
			forEachRow:         descTrigger.ForEachRow,
			whenExpr:           descTrigger.WhenExpr,
			funcID:             cat.StableID(descTrigger.FuncID),
			funcArgs:           funcArgs,
			funcBody:           descTrigger.FuncBody,
			enabled:            descTrigger.Enabled,
		}
	}
	return triggers
}

// optPolicy is a wrapper around descpb.PolicyDescriptor that implements the
// cat.Policy interface.
type optPolicy struct {
	name          tree.Name
	usingExpr     string
	withCheckExpr string
	roles         map[string]struct{} // If roles is nil, then the policy applies to all (aka public)
	command       catpb.PolicyCommand
}

var _ cat.Policy = &optPolicy{}

// Name implements the cat.Policy interface.
func (o *optPolicy) Name() tree.Name {
	return o.name
}

// GetUsingExpr implements the cat.Policy interface.
func (o *optPolicy) GetUsingExpr() string {
	return o.usingExpr
}

// GetWithCheckExpr implements the cat.Policy interface.
func (o *optPolicy) GetWithCheckExpr() string {
	return o.withCheckExpr
}

// GetPolicyCommand implements the cat.Policy interface.
func (o *optPolicy) GetPolicyCommand() catpb.PolicyCommand { return o.command }

// AppliesToRole implements the cat.Policy interface.
func (o *optPolicy) AppliesToRole(user username.SQLUsername) bool {
	// If no roles are specified, assume the policy applies to all users (public role).
	if o.roles == nil {
		return true
	}
	_, found := o.roles[user.Normalized()]
	return found
}

// getOptPolicies maps from descpb.PolicyDescriptor to optPolicy
func getOptPolicies(descPolicies []descpb.PolicyDescriptor) map[tree.PolicyType][]optPolicy {
	policies := make(map[tree.PolicyType][]optPolicy, 2)
	policies[tree.PolicyTypePermissive] = make([]optPolicy, 0, len(descPolicies))
	policies[tree.PolicyTypeRestrictive] = make([]optPolicy, 0, len(descPolicies))
	for i := range descPolicies {
		descPolicy := &descPolicies[i]
		roles := make(map[string]struct{})
		for _, r := range descPolicy.RoleNames {
			if r == username.PublicRole {
				// If the public role is defined, there is no need to check the
				// remaining roles since the policy applies to everyone. We will clear
				// out the roles map to signal that all roles apply.
				roles = nil
				break
			}
			roles[r] = struct{}{}
		}
		targetPolicyType := tree.PolicyTypePermissive
		if descPolicy.Type == catpb.PolicyType_RESTRICTIVE {
			targetPolicyType = tree.PolicyTypeRestrictive
		}
		policies[targetPolicyType] = append(policies[targetPolicyType], optPolicy{
			name:          tree.Name(descPolicy.Name),
			usingExpr:     descPolicy.UsingExpr,
			withCheckExpr: descPolicy.WithCheckExpr,
			command:       descPolicy.Command,
			roles:         roles,
		})
	}
	return policies
}

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
		id := typedesc.UserDefinedTypeOIDToID(collectedOid)
		ids = append(ids, id)
	}
	return ids, nil
}

// mapGeneratedAsIdentityType maps a descpb.GeneratedAsIdentityType into corresponding
// cat.GeneratedAsIdentityType. This is a helper function for the read access to
// the GeneratedAsIdentityType attribute for descpb.ColumnDescriptor.
func mapGeneratedAsIdentityType(inType catpb.GeneratedAsIdentityType) cat.GeneratedAsIdentityType {
	mapGeneratedAsIdentityType := map[catpb.GeneratedAsIdentityType]cat.GeneratedAsIdentityType{
		catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN:  cat.NotGeneratedAsIdentity,
		catpb.GeneratedAsIdentityType_GENERATED_ALWAYS:     cat.GeneratedAlwaysAsIdentity,
		catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT: cat.GeneratedByDefaultAsIdentity,
	}
	return mapGeneratedAsIdentityType[inType]
}
