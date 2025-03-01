// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

import (
	"context"
	"fmt"
	"math/bits"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// SchemaID uniquely identifies the usage of a schema within the scope of a
// query. SchemaID 0 is reserved to mean "unknown schema". Internally, the
// SchemaID consists of an index into the Metadata.schemas slice.
//
// See the comment for Metadata for more details on identifiers.
type SchemaID int32

// privilegeBitmap stores a union of zero or more privileges. Each privilege
// that is present in the bitmap is represented by a bit that is shifted by
// 1 << privilege.Kind, so that multiple privileges can be stored.
type privilegeBitmap uint64

type routineDep struct {
	overload        *tree.Overload
	invocationTypes []*types.T
}

// Metadata assigns unique ids to the columns, tables, and other metadata used
// for global identification within the scope of a particular query. These ids
// tend to be small integers that can be efficiently stored and manipulated.
//
// Within a query, every unique column and every projection should be assigned a
// unique column id. Additionally, every separate reference to a table in the
// query should get a new set of output column ids.
//
// For example, consider the query:
//
//	SELECT x FROM a WHERE y > 0
//
// There are 2 columns in the above query: x and y. During name resolution, the
// above query becomes:
//
//	SELECT [0] FROM a WHERE [1] > 0
//	-- [0] -> x
//	-- [1] -> y
//
// An operator is allowed to reuse some or all of the column ids of an input if:
//
//  1. For every output row, there exists at least one input row having identical
//     values for those columns.
//  2. OR if no such input row exists, there is at least one output row having
//     NULL values for all those columns (e.g. when outer join NULL-extends).
//
// For example, is it safe for a Select to use its input's column ids because it
// only filters rows. Likewise, pass-through column ids of a Project can be
// reused.
//
// For an example where columns cannot be reused, consider the query:
//
//	SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
//
// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not equivalent
// to `r.y`. Therefore, we need to give these columns different ids.
type Metadata struct {
	// schemas stores each schema used by the query if it is a CREATE statement,
	// indexed by SchemaID.
	schemas []cat.Schema

	// cols stores information about each metadata column, indexed by
	// ColumnID.index().
	cols []ColumnMeta

	// tables stores information about each metadata table, indexed by
	// TableID.index().
	tables []TableMeta

	// sequences stores information about each metadata sequence, indexed by SequenceID.
	sequences []cat.Sequence

	// userDefinedTypes contains all user defined types present in expressions
	// in this query.
	// TODO (rohany): This only contains user defined types present in the query
	//  because the installation of type metadata in tables doesn't go through
	//  the type resolver that the optimizer hijacks. However, we could update
	//  this map when adding a table via metadata.AddTable.
	userDefinedTypes      map[oid.Oid]struct{}
	userDefinedTypesSlice []*types.T

	// views stores the list of referenced views. This information is only
	// needed for EXPLAIN (opt, env).
	views []cat.View

	// currUniqueID is the highest UniqueID that has been assigned.
	currUniqueID UniqueID

	// withBindings store bindings for relational expressions inside With or
	// mutation operators, used to determine the logical properties of WithScan.
	withBindings map[WithID]Expr

	// hoistedUncorrelatedSubqueries is used to track uncorrelated subqueries
	// that have been hoisted.
	hoistedUncorrelatedSubqueries map[Expr]struct{}

	// dataSourceDeps stores each data source object that the query depends on.
	dataSourceDeps map[cat.StableID]cat.DataSource

	// routineDeps stores each user-defined function and stored procedure overload
	// (as well as the invocation signature) that the query depends on.
	routineDeps map[cat.StableID]routineDep

	// objectRefsByName stores each unique name that the query uses to reference
	// each object. It is needed because changes to the search path may change
	// which object a given name refers to; for example, switching the database.
	objectRefsByName map[cat.StableID][]*tree.UnresolvedObjectName

	// privileges stores the privileges needed to access each object that the
	// query depends on.
	privileges map[cat.StableID]privilegeBitmap

	// builtinRefsByName stores the names used to reference builtin functions in
	// the query. This is necessary to handle the case where changes to the search
	// path cause a function call to be resolved to a UDF with the same signature
	// as a builtin function.
	builtinRefsByName map[tree.UnresolvedName]struct{}

	// rlsMeta stores row-level security policy metadata enforced during query
	// execution.
	rlsMeta RowLevelSecurityMeta

	digest struct {
		syncutil.Mutex
		depDigest cat.DependencyDigest
	}

	// NOTE! When adding fields here, update Init (if reusing allocated
	// data structures is desired), CopyFrom and TestMetadata.
}

// Init prepares the metadata for use (or reuse).
func (md *Metadata) Init() {
	// Clear the metadata objects to release memory (this clearing pattern is
	// optimized by Go).
	schemas := md.schemas
	for i := range schemas {
		schemas[i] = nil
	}

	cols := md.cols
	for i := range cols {
		cols[i] = ColumnMeta{}
	}

	tables := md.tables
	for i := range tables {
		tables[i] = TableMeta{}
	}

	sequences := md.sequences
	for i := range sequences {
		sequences[i] = nil
	}

	views := md.views
	for i := range views {
		views[i] = nil
	}

	dataSourceDeps := md.dataSourceDeps
	if dataSourceDeps == nil {
		dataSourceDeps = make(map[cat.StableID]cat.DataSource)
	}
	for id := range md.dataSourceDeps {
		delete(md.dataSourceDeps, id)
	}

	routineDeps := md.routineDeps
	if routineDeps == nil {
		routineDeps = make(map[cat.StableID]routineDep)
	}
	for id := range md.routineDeps {
		delete(md.routineDeps, id)
	}

	objectRefsByName := md.objectRefsByName
	if objectRefsByName == nil {
		objectRefsByName = make(map[cat.StableID][]*tree.UnresolvedObjectName)
	}
	for id := range md.objectRefsByName {
		delete(md.objectRefsByName, id)
	}

	privileges := md.privileges
	if privileges == nil {
		privileges = make(map[cat.StableID]privilegeBitmap)
	}
	for id := range md.privileges {
		delete(md.privileges, id)
	}

	builtinRefsByName := md.builtinRefsByName
	if builtinRefsByName == nil {
		builtinRefsByName = make(map[tree.UnresolvedName]struct{})
	}
	for name := range md.builtinRefsByName {
		delete(md.builtinRefsByName, name)
	}

	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*md = Metadata{}
	md.schemas = schemas[:0]
	md.cols = cols[:0]
	md.tables = tables[:0]
	md.sequences = sequences[:0]
	md.views = views[:0]
	md.dataSourceDeps = dataSourceDeps
	md.routineDeps = routineDeps
	md.objectRefsByName = objectRefsByName
	md.privileges = privileges
	md.builtinRefsByName = builtinRefsByName
}

// CopyFrom initializes the metadata with a copy of the provided metadata.
// This metadata can then be modified independent of the copied metadata.
//
// Table annotations are not transferred over; all annotations are unset on
// the copy, except for regionConfig, which is read-only, and can be shared.
//
// copyScalarFn must be a function that returns a copy of the given scalar
// expression.
func (md *Metadata) CopyFrom(from *Metadata, copyScalarFn func(Expr) Expr) {
	if len(md.schemas) != 0 || len(md.cols) != 0 || len(md.tables) != 0 ||
		len(md.sequences) != 0 || len(md.views) != 0 || len(md.userDefinedTypes) != 0 ||
		len(md.userDefinedTypesSlice) != 0 || len(md.dataSourceDeps) != 0 ||
		len(md.routineDeps) != 0 || len(md.objectRefsByName) != 0 || len(md.privileges) != 0 ||
		len(md.builtinRefsByName) != 0 || md.rlsMeta.IsInitialized {
		panic(errors.AssertionFailedf("CopyFrom requires empty destination"))
	}
	md.schemas = append(md.schemas, from.schemas...)
	md.cols = append(md.cols, from.cols...)

	if len(from.userDefinedTypesSlice) > 0 {
		if md.userDefinedTypes == nil {
			md.userDefinedTypes = make(map[oid.Oid]struct{}, len(from.userDefinedTypesSlice))
		}
		for i := range from.userDefinedTypesSlice {
			typ := from.userDefinedTypesSlice[i]
			md.userDefinedTypes[typ.Oid()] = struct{}{}
			md.userDefinedTypesSlice = append(md.userDefinedTypesSlice, typ)
		}
	}

	if cap(md.tables) >= len(from.tables) {
		md.tables = md.tables[:len(from.tables)]
	} else {
		md.tables = make([]TableMeta, len(from.tables))
	}
	for i := range from.tables {
		// Note: annotations inside TableMeta are not retained...
		md.tables[i].copyFrom(&from.tables[i], copyScalarFn)

		// ...except for the regionConfig annotation.
		tabID := from.tables[i].MetaID
		regionConfig, ok := md.TableAnnotation(tabID, regionConfigAnnID).(*multiregion.RegionConfig)
		if ok {
			// Don't waste time looking up a database descriptor and constructing a
			// RegionConfig more than once for a given table.
			md.SetTableAnnotation(tabID, regionConfigAnnID, regionConfig)
		}
	}

	for id, dataSource := range from.dataSourceDeps {
		if md.dataSourceDeps == nil {
			md.dataSourceDeps = make(map[cat.StableID]cat.DataSource)
		}
		md.dataSourceDeps[id] = dataSource
	}

	for id, overload := range from.routineDeps {
		if md.routineDeps == nil {
			md.routineDeps = make(map[cat.StableID]routineDep)
		}
		md.routineDeps[id] = overload
	}

	for id, names := range from.objectRefsByName {
		if md.objectRefsByName == nil {
			md.objectRefsByName = make(map[cat.StableID][]*tree.UnresolvedObjectName)
		}
		newNames := make([]*tree.UnresolvedObjectName, len(names))
		copy(newNames, names)
		md.objectRefsByName[id] = newNames
	}

	for id, privilegeSet := range from.privileges {
		if md.privileges == nil {
			md.privileges = make(map[cat.StableID]privilegeBitmap)
		}
		md.privileges[id] = privilegeSet
	}

	for name := range from.builtinRefsByName {
		if md.builtinRefsByName == nil {
			md.builtinRefsByName = make(map[tree.UnresolvedName]struct{})
		}
		md.builtinRefsByName[name] = struct{}{}
	}

	md.sequences = append(md.sequences, from.sequences...)
	md.views = append(md.views, from.views...)
	md.currUniqueID = from.currUniqueID

	// We cannot copy the bound expressions; they must be rebuilt in the new memo.
	md.withBindings = nil

	md.rlsMeta = from.rlsMeta
	md.rlsMeta.PoliciesApplied = make(map[TableID]PolicyIDSet)
	for id, policies := range from.rlsMeta.PoliciesApplied {
		md.rlsMeta.PoliciesApplied[id] = policies.Copy()
	}
}

// MDDepName stores either the unresolved DataSourceName or the StableID from
// the query that was used to resolve a data source.
type MDDepName struct {
	// byID is non-zero if and only if the data source was looked up using the
	// StableID.
	byID cat.StableID

	// byName is non-zero if and only if the data source was looked up using a
	// name.
	byName cat.DataSourceName
}

// DepByName is used with AddDependency when the data source was looked up using a
// data source name.
func DepByName(name *cat.DataSourceName) MDDepName {
	return MDDepName{byName: *name}
}

// DepByID is used with AddDependency when the data source was looked up by ID.
func DepByID(id cat.StableID) MDDepName {
	return MDDepName{byID: id}
}

// AddDependency tracks one of the catalog data sources on which the query
// depends, as well as the privilege required to access that data source. If
// the Memo using this metadata is cached, then a call to CheckDependencies can
// detect if the name resolves to a different data source now, or if changes to
// schema or permissions on the data source has invalidated the cached metadata.
func (md *Metadata) AddDependency(name MDDepName, ds cat.DataSource, priv privilege.Kind) {
	id := ds.ID()
	md.dataSourceDeps[id] = ds
	md.privileges[id] = md.privileges[id] | (1 << priv)
	if name.byID == 0 {
		// This data source was referenced by name.
		md.objectRefsByName[id] = append(md.objectRefsByName[id], name.byName.ToUnresolvedObjectName())
	}
}

// dependencyDigestEquals checks if the stored dependency digest matches the
// current dependency digest.
func (md *Metadata) dependencyDigestEquals(currentDigest *cat.DependencyDigest) bool {
	md.digest.Lock()
	defer md.digest.Unlock()
	return currentDigest.Equal(&md.digest.depDigest)
}

// leaseObjectsInMetaData ensures that all references within this metadata
// are leased to prevent schema changes from modifying the underlying objects
// excessively. Additionally, the metadata version and leased descriptor versions
// are compared.
func (md *Metadata) leaseObjectsInMetaData(
	ctx context.Context, optCatalog cat.Catalog,
) (leasedVersionMatchesMetadata bool, err error) {
	for id, ds := range md.dataSourceDeps {
		ver, err := optCatalog.LeaseByStableID(ctx, id)
		if err != nil {
			return false, err
		}
		if ver != ds.Version() {
			return false, nil
		}
	}
	for id, rd := range md.routineDeps {
		ver, err := optCatalog.LeaseByStableID(ctx, id)
		if err != nil {
			return false, err
		}
		if ver != rd.overload.Version {
			return false, nil
		}
	}
	for _, typ := range md.userDefinedTypesSlice {
		id := typedesc.UserDefinedTypeOIDToID(typ.Oid())
		// Not a user defined type.
		if id == catid.InvalidDescID {
			continue
		}
		ver, err := optCatalog.LeaseByStableID(ctx, cat.StableID(id))
		if err != nil {
			return false, err
		}
		if ver != uint64(typ.TypeMeta.Version) {
			return false, nil
		}
	}
	return true, nil
}

// CheckDependencies resolves (again) each database object on which this
// metadata depends, in order to check the following conditions:
//  1. The object has not been modified.
//  2. If referenced by name, the name does not resolve to a different object.
//  3. The user still has sufficient privileges to access the object. Note that
//     this point currently only applies to data sources.
//
// If the dependencies are no longer up-to-date, then CheckDependencies returns
// false.
//
// This function can only swallow "undefined" or "dropped" errors, since these
// are expected. Other error types must be propagated, since CheckDependencies
// may perform KV operations on behalf of the transaction associated with the
// provided catalog.
func (md *Metadata) CheckDependencies(
	ctx context.Context, evalCtx *eval.Context, optCatalog cat.Catalog,
) (upToDate bool, err error) {
	// If the query is AOST we must check all the dependencies, since the descriptors
	// may have been different in the past. Otherwise, the dependency digest
	// is sufficient.
	currentDigest := optCatalog.GetDependencyDigest()
	if evalCtx.SessionData().CatalogDigestStalenessCheckEnabled &&
		evalCtx.Settings.Version.IsActive(ctx, clusterversion.V25_1) &&
		evalCtx.AsOfSystemTime == nil &&
		!evalCtx.Txn.ReadTimestampFixed() &&
		md.dependencyDigestEquals(&currentDigest) {
		// Lease the underlying descriptors for this metadata. If we fail to lease
		// any descriptors attempt to resolve them by name through the more expensive
		// code path below.
		upToDate, err = md.leaseObjectsInMetaData(ctx, optCatalog)
		if err == nil {
			return upToDate, nil
		}
	}

	// Check that no referenced data sources have changed.
	for id, dataSource := range md.dataSourceDeps {
		var toCheck cat.DataSource
		if names, ok := md.objectRefsByName[id]; ok {
			// The data source was referenced by name at least once.
			for _, name := range names {
				tableName := name.ToTableName()
				toCheck, _, err = optCatalog.ResolveDataSource(ctx, cat.Flags{}, &tableName)
				if err != nil || !dataSource.Equals(toCheck) {
					return false, maybeSwallowMetadataResolveErr(err)
				}
			}
		} else {
			// The data source was only referenced by ID.
			toCheck, _, err = optCatalog.ResolveDataSourceByID(ctx, cat.Flags{}, dataSource.ID())
			if err != nil || !dataSource.Equals(toCheck) {
				return false, maybeSwallowMetadataResolveErr(err)
			}
		}
	}

	// Check that no referenced user defined types have changed.
	for _, typ := range md.AllUserDefinedTypes() {
		id := cat.StableID(catid.UserDefinedOIDToID(typ.Oid()))
		if names, ok := md.objectRefsByName[id]; ok {
			for _, name := range names {
				toCheck, err := optCatalog.ResolveType(ctx, name)
				if err != nil || typ.Oid() != toCheck.Oid() ||
					typ.TypeMeta.Version != toCheck.TypeMeta.Version {
					return false, maybeSwallowMetadataResolveErr(err)
				}
			}
		} else {
			toCheck, err := optCatalog.ResolveTypeByOID(ctx, typ.Oid())
			if err != nil || typ.TypeMeta.Version != toCheck.TypeMeta.Version {
				return false, maybeSwallowMetadataResolveErr(err)
			}
		}
	}

	// Check that no referenced user defined functions or stored procedures have
	// changed.
	for id, dep := range md.routineDeps {
		overload := dep.overload
		if names, ok := md.objectRefsByName[id]; ok {
			for _, name := range names {
				definition, err := optCatalog.ResolveFunction(
					ctx, tree.MakeUnresolvedFunctionName(name.ToUnresolvedName()),
					&evalCtx.SessionData().SearchPath,
				)
				if err != nil {
					return false, maybeSwallowMetadataResolveErr(err)
				}
				routineObj := tree.RoutineObj{
					FuncName: name.ToRoutineName(),
					Params:   make(tree.RoutineParams, len(dep.invocationTypes)),
				}
				for i := 0; i < len(routineObj.Params); i++ {
					routineObj.Params[i] = tree.RoutineParam{
						Type: dep.invocationTypes[i],
						// Since we're not in the DROP context, it's sufficient
						// to specify only the input parameters.
						Class: tree.RoutineParamIn,
						// Note that we don't need to specify the DefaultVal
						// here because invocationTypes specifies the argument
						// schema that was actually used. Instead, we will ask
						// for matching overloads to use their DEFAULT
						// expressions if necessary.
					}
				}
				// NOTE: We match for all types of routines here, including
				// procedures so that if a function has been dropped and a
				// procedure is created with the same signature, we do not get a
				// "<func> is not a function" error here. Instead, we'll return
				// false and attempt to rebuild the statement.
				routineType := tree.UDFRoutine | tree.BuiltinRoutine | tree.ProcedureRoutine
				// Always allowing using DEFAULT expressions for input
				// parameters since the signature of the routine might have
				// changed even though the invocation remained the same.
				const tryDefaultExprs = true
				toCheck, err := definition.MatchOverload(
					ctx,
					optCatalog,
					&routineObj,
					&evalCtx.SessionData().SearchPath,
					routineType,
					false, /* inDropContext */
					tryDefaultExprs,
				)
				if err != nil || toCheck.Oid != overload.Oid || toCheck.Version != overload.Version {
					return false, maybeSwallowMetadataResolveErr(err)
				}
			}
		} else {
			_, toCheck, err := optCatalog.ResolveFunctionByOID(ctx, overload.Oid)
			if err != nil || overload.Version != toCheck.Version {
				return false, maybeSwallowMetadataResolveErr(err)
			}
		}
	}

	// Check that any references to builtin functions do not now resolve to a UDF
	// with the same signature (e.g. after changes to the search path).
	for name := range md.builtinRefsByName {
		definition, err := optCatalog.ResolveFunction(
			ctx, tree.MakeUnresolvedFunctionName(&name), &evalCtx.SessionData().SearchPath,
		)
		if err != nil {
			return false, maybeSwallowMetadataResolveErr(err)
		}
		for i := range definition.Overloads {
			if definition.Overloads[i].Type == tree.UDFRoutine {
				return false, nil
			}
		}
	}

	// Check that the role still has the required privileges for the data sources
	// and routines.
	//
	// NOTE: this check has to happen after the object resolution checks, or else
	// we may end up returning a privilege error when the memo should have just
	// been invalidated.
	if err := md.checkDataSourcePrivileges(ctx, optCatalog); err != nil {
		return false, err
	}
	for _, dep := range md.routineDeps {
		if err := optCatalog.CheckExecutionPrivilege(ctx, dep.overload.Oid, optCatalog.GetCurrentUser()); err != nil {
			return false, err
		}
	}

	// Check for staleness from a row-level security point of view.
	if upToDate, err := md.checkRLSDependencies(ctx, evalCtx, optCatalog); err != nil || !upToDate {
		return upToDate, err
	}

	// Update the digest after a full dependency check, since our fast
	// check did not succeed.
	if evalCtx.SessionData().CatalogDigestStalenessCheckEnabled {
		md.digest.Lock()
		md.digest.depDigest = currentDigest
		md.digest.Unlock()
	}
	return true, nil
}

// handleMetadataResolveErr swallows errors that are thrown when a database
// object is dropped, since such an error potentially only means that the
// metadata is stale and should be re-resolved.
func maybeSwallowMetadataResolveErr(err error) error {
	if err == nil {
		return nil
	}
	// Handle when the object no longer exists.
	switch pgerror.GetPGCode(err) {
	case pgcode.UndefinedObject, pgcode.UndefinedTable, pgcode.UndefinedDatabase,
		pgcode.UndefinedSchema, pgcode.UndefinedFunction, pgcode.InvalidName,
		pgcode.InvalidSchemaName, pgcode.InvalidCatalogName:
		return nil
	}
	if errors.Is(err, catalog.ErrDescriptorDropped) {
		return nil
	}
	return err
}

// checkDataSourcePrivileges checks that none of the privileges required by the
// query for the referenced data sources have been revoked.
func (md *Metadata) checkDataSourcePrivileges(ctx context.Context, optCatalog cat.Catalog) error {
	for _, dataSource := range md.dataSourceDeps {
		privileges := md.privileges[dataSource.ID()]
		for privs := privileges; privs != 0; {
			// Strip off each privilege bit and make call to CheckPrivilege for it.
			// Note that priv == 0 can occur when a dependency was added with
			// privilege.Kind = 0 (e.g. for a table within a view, where the table
			// privileges do not need to be checked). Ignore the "zero privilege".
			priv := privilege.Kind(bits.TrailingZeros32(uint32(privs)))
			if priv != 0 {
				if err := optCatalog.CheckPrivilege(ctx, dataSource, optCatalog.GetCurrentUser(), priv); err != nil {
					return err
				}
			}
			// Set the just-handled privilege bit to zero and look for next.
			privs &= ^(1 << priv)
		}
	}
	return nil
}

// AddSchema indexes a new reference to a schema used by the query.
func (md *Metadata) AddSchema(sch cat.Schema) SchemaID {
	md.schemas = append(md.schemas, sch)
	return SchemaID(len(md.schemas))
}

// Schema looks up the metadata for the schema associated with the given schema
// id.
func (md *Metadata) Schema(schID SchemaID) cat.Schema {
	return md.schemas[schID-1]
}

// AddUserDefinedType adds a user defined type to the metadata for this query.
// If the type was resolved by name, the name will be tracked as well.
func (md *Metadata) AddUserDefinedType(typ *types.T, name *tree.UnresolvedObjectName) {
	if !typ.UserDefined() {
		return
	}
	if md.userDefinedTypes == nil {
		md.userDefinedTypes = make(map[oid.Oid]struct{})
	}
	if _, ok := md.userDefinedTypes[typ.Oid()]; !ok {
		md.userDefinedTypes[typ.Oid()] = struct{}{}
		md.userDefinedTypesSlice = append(md.userDefinedTypesSlice, typ)
	}
	if name != nil {
		id := cat.StableID(catid.UserDefinedOIDToID(typ.Oid()))
		md.objectRefsByName[id] = append(md.objectRefsByName[id], name)
	}
}

// AllUserDefinedTypes returns all user defined types contained in this query.
func (md *Metadata) AllUserDefinedTypes() []*types.T {
	return md.userDefinedTypesSlice
}

// HasUserDefinedRoutines returns true if the query references a UDF or stored
// procedure.
func (md *Metadata) HasUserDefinedRoutines() bool {
	return len(md.routineDeps) > 0
}

// AddUserDefinedRoutine adds a user-defined function or stored procedure to the
// metadata for this query. If the routine was resolved by name, the name will
// also be tracked.
func (md *Metadata) AddUserDefinedRoutine(
	overload *tree.Overload, invocationTypes []*types.T, name *tree.UnresolvedObjectName,
) {
	if overload.Type == tree.BuiltinRoutine {
		return
	}
	id := cat.StableID(catid.UserDefinedOIDToID(overload.Oid))
	md.routineDeps[id] = routineDep{
		overload:        overload,
		invocationTypes: invocationTypes,
	}
	if name != nil {
		md.objectRefsByName[id] = append(md.objectRefsByName[id], name)
	}
}

// ForEachUserDefinedRoutine executes the given function for each user-defined
// routine (UDF or stored procedure) overload. The order of iteration is
// non-deterministic.
func (md *Metadata) ForEachUserDefinedRoutine(fn func(overload *tree.Overload)) {
	for _, dep := range md.routineDeps {
		fn(dep.overload)
	}
}

// AddBuiltin adds a name used to resolve a builtin function to the metadata for
// this query. This is necessary to handle the case when changes to the search
// path cause a function call to resolve as a UDF instead of a builtin function.
func (md *Metadata) AddBuiltin(name *tree.UnresolvedObjectName) {
	if name == nil {
		return
	}
	if md.builtinRefsByName == nil {
		md.builtinRefsByName = make(map[tree.UnresolvedName]struct{})
	}
	md.builtinRefsByName[*name.ToUnresolvedName()] = struct{}{}
}

// AddTable indexes a new reference to a table within the query. Separate
// references to the same table are assigned different table ids (e.g.  in a
// self-join query). All columns are added to the metadata. If mutation columns
// are present, they are added after active columns.
//
// The ExplicitCatalog/ExplicitSchema fields of the table's alias are honored so
// that its original formatting is preserved for error messages,
// pretty-printing, etc.
func (md *Metadata) AddTable(tab cat.Table, alias *tree.TableName) TableID {
	tabID := makeTableID(len(md.tables), ColumnID(len(md.cols)+1))
	if md.tables == nil {
		md.tables = make([]TableMeta, 0, 4)
	}
	md.tables = append(md.tables, TableMeta{MetaID: tabID, Table: tab, Alias: *alias})

	colCount := tab.ColumnCount()
	if md.cols == nil {
		md.cols = make([]ColumnMeta, 0, colCount)
	}

	for i := 0; i < colCount; i++ {
		col := tab.Column(i)
		colID := md.AddColumn(string(col.ColName()), col.DatumType())
		md.ColumnMeta(colID).Table = tabID
	}

	return tabID
}

// DuplicateTable creates a new reference to the table with the given ID. All
// columns are added to the metadata with new column IDs. If mutation columns
// are present, they are added after active columns. The ID of the new table
// reference is returned. This function panics if a table with the given ID does
// not exists in the metadata.
//
// remapColumnIDs must be a function that remaps the column IDs within a
// ScalarExpr to new column IDs. It takes as arguments a ScalarExpr and a
// mapping of old column IDs to new column IDs, and returns a new ScalarExpr.
// This function is used when duplicating Constraints, ComputedCols, and
// partialIndexPredicates. DuplicateTable requires this callback function,
// rather than performing the remapping itself, because remapping column IDs
// requires constructing new expressions with norm.Factory. The norm package
// depends on opt, and cannot be imported here.
//
// The ExplicitCatalog/ExplicitSchema fields of the table's alias are honored so
// that its original formatting is preserved for error messages,
// pretty-printing, etc.
func (md *Metadata) DuplicateTable(
	tabID TableID, remapColumnIDs func(e ScalarExpr, colMap ColMap) ScalarExpr,
) TableID {
	if md.tables == nil || tabID.index() >= len(md.tables) {
		panic(errors.AssertionFailedf("table with ID %d does not exist", tabID))
	}

	tabMeta := md.TableMeta(tabID)
	tab := tabMeta.Table
	newTabID := makeTableID(len(md.tables), ColumnID(len(md.cols)+1))

	// Generate new column IDs for each column in the table, and keep track of
	// a mapping from the original TableMeta's column IDs to the new ones.
	var colMap ColMap
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		col := tab.Column(i)
		oldColID := tabID.ColumnID(i)
		newColID := md.AddColumn(string(col.ColName()), col.DatumType())
		md.ColumnMeta(newColID).Table = newTabID
		colMap.Set(int(oldColID), int(newColID))
	}

	// Create new constraints by remapping the column IDs to the new TableMeta's
	// column IDs.
	var constraints ScalarExpr
	if tabMeta.Constraints != nil {
		constraints = remapColumnIDs(tabMeta.Constraints, colMap)
	}

	// Create new computed column expressions by remapping the column IDs in
	// each ScalarExpr.
	var computedCols map[ColumnID]ScalarExpr
	var referencedColsInComputedExpressions ColSet
	if len(tabMeta.ComputedCols) > 0 {
		computedCols = make(map[ColumnID]ScalarExpr, len(tabMeta.ComputedCols))
		for colID, e := range tabMeta.ComputedCols {
			newColID, ok := colMap.Get(int(colID))
			if !ok {
				panic(errors.AssertionFailedf("column with ID %d does not exist in map", colID))
			}
			computedCols[ColumnID(newColID)] = remapColumnIDs(e, colMap)
		}
		// Add columns present in newScalarExpr to referencedColsInComputedExpressions.
		referencedColsInComputedExpressions =
			tabMeta.ColsInComputedColsExpressions.CopyAndMaybeRemap(colMap)
	}

	// Create new partial index predicate expressions by remapping the column
	// IDs in each ScalarExpr.
	var partialIndexPredicates map[cat.IndexOrdinal]ScalarExpr
	if len(tabMeta.partialIndexPredicates) > 0 {
		partialIndexPredicates = make(map[cat.IndexOrdinal]ScalarExpr, len(tabMeta.partialIndexPredicates))
		for idxOrd, e := range tabMeta.partialIndexPredicates {
			partialIndexPredicates[idxOrd] = remapColumnIDs(e, colMap)
		}
	}

	var checkConstraintsStats map[ColumnID]interface{}
	if len(tabMeta.checkConstraintsStats) > 0 {
		checkConstraintsStats =
			make(map[ColumnID]interface{},
				len(tabMeta.checkConstraintsStats))
		for i := range tabMeta.checkConstraintsStats {
			if dstCol, ok := colMap.Get(int(i)); ok {
				// We remap the column ID key, but not any column IDs in the
				// ColumnStatistic as this is still being used in the statistics of the
				// original table and should be treated as immutable. When the Histogram
				// is copied in ColumnStatistic.CopyFromOther, it is initialized with
				// the proper column ID.
				checkConstraintsStats[ColumnID(dstCol)] = tabMeta.checkConstraintsStats[i]
			} else {
				panic(errors.AssertionFailedf("remapping of check constraint stats column failed"))
			}
		}
	}

	newTabMeta := TableMeta{
		MetaID:                        newTabID,
		Table:                         tabMeta.Table,
		Alias:                         tabMeta.Alias,
		IgnoreForeignKeys:             tabMeta.IgnoreForeignKeys,
		Constraints:                   constraints,
		ComputedCols:                  computedCols,
		ColsInComputedColsExpressions: referencedColsInComputedExpressions,
		partialIndexPredicates:        partialIndexPredicates,
		indexPartitionLocalities:      tabMeta.indexPartitionLocalities,
		checkConstraintsStats:         checkConstraintsStats,
	}
	newTabMeta.indexVisibility.cached = tabMeta.indexVisibility.cached
	newTabMeta.indexVisibility.notVisible = tabMeta.indexVisibility.notVisible
	md.tables = append(md.tables, newTabMeta)
	regionConfig, ok := md.TableAnnotation(tabID, regionConfigAnnID).(*multiregion.RegionConfig)
	if ok {
		// Don't waste time looking up a database descriptor and constructing a
		// RegionConfig more than once for a given table.
		md.SetTableAnnotation(newTabID, regionConfigAnnID, regionConfig)
	}

	return newTabID
}

// TableMeta looks up the metadata for the table associated with the given table
// id. The same table can be added multiple times to the query metadata and
// associated with multiple table ids.
func (md *Metadata) TableMeta(tabID TableID) *TableMeta {
	return &md.tables[tabID.index()]
}

// Table looks up the catalog table associated with the given metadata id. The
// same table can be associated with multiple metadata ids.
func (md *Metadata) Table(tabID TableID) cat.Table {
	return md.TableMeta(tabID).Table
}

// AllTables returns the metadata for all tables. The result must not be
// modified.
func (md *Metadata) AllTables() []TableMeta {
	return md.tables
}

// NumTables returns the number of tables in the metadata.
func (md *Metadata) NumTables() int {
	return len(md.tables)
}

// AddColumn assigns a new unique id to a column within the query and records
// its alias and type. If the alias is empty, a "column<ID>" alias is created.
func (md *Metadata) AddColumn(alias string, typ *types.T) ColumnID {
	if alias == "" {
		alias = fmt.Sprintf("column%d", len(md.cols)+1)
	}
	colID := ColumnID(len(md.cols) + 1)
	md.cols = append(md.cols, ColumnMeta{MetaID: colID, Alias: alias, Type: typ})
	return colID
}

// NumColumns returns the count of columns tracked by this Metadata instance.
func (md *Metadata) NumColumns() int {
	return len(md.cols)
}

// MaxColumn returns the maximum column ID tracked by this Metadata instance.
func (md *Metadata) MaxColumn() ColumnID {
	return ColumnID(len(md.cols))
}

// ColumnMeta looks up the metadata for the column associated with the given
// column id. The same column can be added multiple times to the query metadata
// and associated with multiple column ids.
func (md *Metadata) ColumnMeta(colID ColumnID) *ColumnMeta {
	return &md.cols[colID.index()]
}

// QualifiedAlias returns the column alias, possibly qualified with the table,
// schema, or database name:
//
//  1. If fullyQualify is true, then the returned alias is prefixed by the
//     original, fully qualified name of the table: tab.Name().FQString().
//
//  2. If there's another column in the metadata with the same column alias but
//     a different table name, then prefix the column alias with the table
//     name: "tabName.columnAlias". If alwaysQualify is true, then the column
//     alias is always prefixed with the table alias.
func (md *Metadata) QualifiedAlias(
	ctx context.Context, colID ColumnID, fullyQualify, alwaysQualify bool, catalog cat.Catalog,
) string {
	cm := md.ColumnMeta(colID)
	if cm.Table == 0 {
		// Column doesn't belong to a table, so no need to qualify it further.
		return cm.Alias
	}

	// If a fully qualified alias has not been requested, then only qualify it if
	// it would otherwise be ambiguous.
	var tabAlias tree.TableName
	qualify := fullyQualify || alwaysQualify
	if !fullyQualify {
		tabAlias = md.TableMeta(cm.Table).Alias
		for i := range md.cols {
			if i == int(cm.MetaID-1) {
				continue
			}

			// If there are two columns with same alias, then column is ambiguous.
			cm2 := &md.cols[i]
			if cm2.Alias == cm.Alias {
				if cm2.Table == 0 {
					qualify = true
				} else {
					// Only qualify if the qualified names are actually different.
					tabAlias2 := md.TableMeta(cm2.Table).Alias
					if tabAlias.String() != tabAlias2.String() {
						qualify = true
					}
				}
			}
		}
	}

	// If the column name should not even be partly qualified, then no more to do.
	if !qualify {
		return cm.Alias
	}

	var sb strings.Builder
	if fullyQualify {
		tn, err := catalog.FullyQualifiedName(ctx, md.TableMeta(cm.Table).Table)
		if err != nil {
			panic(err)
		}
		sb.WriteString(tn.FQString())
	} else {
		sb.WriteString(tabAlias.String())
	}
	sb.WriteRune('.')
	sb.WriteString(cm.Alias)
	return sb.String()
}

// UpdateTableMeta allows the caller to replace the cat.Table struct that a
// TableMeta instance stores.
func (md *Metadata) UpdateTableMeta(
	ctx context.Context, evalCtx *eval.Context, tables map[cat.StableID]cat.Table,
) {
	for i := range md.tables {
		oldTable := md.tables[i].Table
		if newTable, ok := tables[oldTable.ID()]; ok {
			// If there are any inverted hypothetical indexes, the hypothetical table
			// will have extra inverted columns added. Add any new inverted columns to
			// the metadata.
			for j, n := oldTable.ColumnCount(), newTable.ColumnCount(); j < n; j++ {
				md.AddColumn(string(newTable.Column(j).ColName()), types.Bytes)
			}
			if newTable.ColumnCount() > oldTable.ColumnCount() {
				// If we added any new columns, we need to recalculate the not null
				// column set.
				md.SetTableAnnotation(md.tables[i].MetaID, NotNullAnnID, nil)
			}
			md.tables[i].Table = newTable
			md.tables[i].CacheIndexPartitionLocalities(ctx, evalCtx)
		}
	}
}

// SequenceID uniquely identifies the usage of a sequence within the scope of a
// query. SequenceID 0 is reserved to mean "unknown sequence".
type SequenceID uint64

// index returns the index of the sequence in Metadata.sequences. It's biased by 1, so
// that SequenceID 0 can be be reserved to mean "unknown sequence".
func (s SequenceID) index() int {
	return int(s - 1)
}

// makeSequenceID constructs a new SequenceID from its component parts.
func makeSequenceID(index int) SequenceID {
	// Bias the sequence index by 1.
	return SequenceID(index + 1)
}

// AddSequence adds the sequence to the metadata, returning a SequenceID that
// can be used to retrieve it.
func (md *Metadata) AddSequence(seq cat.Sequence) SequenceID {
	seqID := makeSequenceID(len(md.sequences))
	if md.sequences == nil {
		md.sequences = make([]cat.Sequence, 0, 4)
	}
	md.sequences = append(md.sequences, seq)

	return seqID
}

// Sequence looks up the catalog sequence associated with the given metadata id. The
// same sequence can be associated with multiple metadata ids.
func (md *Metadata) Sequence(seqID SequenceID) cat.Sequence {
	return md.sequences[seqID.index()]
}

// AllSequences returns the metadata for all sequences. The result must not be
// modified.
func (md *Metadata) AllSequences() []cat.Sequence {
	return md.sequences
}

// UniqueID should be used to disambiguate multiple uses of an expression
// within the scope of a query. For example, a UniqueID field should be
// added to an expression type if two instances of that type might otherwise
// be indistinguishable based on the values of their other fields.
//
// See the comment for Metadata for more details on identifiers.
type UniqueID uint64

// NextUniqueID returns a fresh UniqueID which is guaranteed to never have been
// previously allocated in this memo.
func (md *Metadata) NextUniqueID() UniqueID {
	md.currUniqueID++
	return md.currUniqueID
}

// AddView adds a new reference to a view used by the query.
func (md *Metadata) AddView(v cat.View) {
	md.views = append(md.views, v)
}

// AllViews returns the metadata for all views. The result must not be
// modified.
func (md *Metadata) AllViews() []cat.View {
	return md.views
}

// WithID uniquely identifies a With expression within the scope of a query.
// WithID=0 is reserved to mean "unknown expression".
// See the comment for Metadata for more details on identifiers.
type WithID uint64

// AddWithBinding associates a WithID to its bound expression.
func (md *Metadata) AddWithBinding(id WithID, expr Expr) {
	if md.withBindings == nil {
		md.withBindings = make(map[WithID]Expr)
	}
	md.withBindings[id] = expr
}

// WithBinding returns the bound expression for the given WithID.
// Panics with an assertion error if there is none.
func (md *Metadata) WithBinding(id WithID) Expr {
	res, ok := md.withBindings[id]
	if !ok {
		panic(errors.AssertionFailedf("no binding for WithID %d", id))
	}
	return res
}

// HasWithBinding returns true if the given WithID is already bound to an
// expression.
func (md *Metadata) HasWithBinding(id WithID) bool {
	_, ok := md.withBindings[id]
	return ok
}

// ForEachWithBinding calls fn with each bound (WithID, Expr) pair in the
// metadata.
func (md *Metadata) ForEachWithBinding(fn func(WithID, Expr)) {
	for id, expr := range md.withBindings {
		fn(id, expr)
	}
}

// AddHoistedUncorrelatedSubquery marks the given uncorrelated subquery
// expression as hoisted. It is used to prevent hoisting the same uncorrelated
// subquery twice because that may cause two children of an expression to have
// intersecting columns (see #114703).
func (md *Metadata) AddHoistedUncorrelatedSubquery(subquery Expr) {
	if md.hoistedUncorrelatedSubqueries == nil {
		md.hoistedUncorrelatedSubqueries = make(map[Expr]struct{})
	}
	md.hoistedUncorrelatedSubqueries[subquery] = struct{}{}
}

// IsHoistedUncorrelatedSubquery returns true if the given subquery was
// previously marked as hoisted with AddHoistedUncorrelatedSubquery.
func (md *Metadata) IsHoistedUncorrelatedSubquery(subquery Expr) bool {
	_, ok := md.hoistedUncorrelatedSubqueries[subquery]
	return ok
}

// TestingDataSourceDeps exposes the dataSourceDeps for testing.
func (md *Metadata) TestingDataSourceDeps() map[cat.StableID]cat.DataSource {
	return md.dataSourceDeps
}

// TestingRoutineDepsEqual returns whether the routine deps of the other
// Metadata are equal to the routine deps of this Metadata.
func (md *Metadata) TestingRoutineDepsEqual(other *Metadata) bool {
	if len(md.routineDeps) != len(other.routineDeps) {
		return false
	}
	for id, otherDep := range other.routineDeps {
		dep, ok := md.routineDeps[id]
		if !ok {
			return false
		}
		if dep.overload != otherDep.overload || len(dep.invocationTypes) != len(otherDep.invocationTypes) {
			return false
		}
	}
	return true
}

// TestingObjectRefsByName exposes the objectRefsByName for testing.
func (md *Metadata) TestingObjectRefsByName() map[cat.StableID][]*tree.UnresolvedObjectName {
	return md.objectRefsByName
}

// TestingPrivileges exposes the privileges for testing.
func (md *Metadata) TestingPrivileges() map[cat.StableID]privilegeBitmap {
	return md.privileges
}

// SetRLSEnabled will update the metadata to indicate we came across a table
// that had row-level security enabled.
func (md *Metadata) SetRLSEnabled(user username.SQLUsername, isAdmin bool, tableID TableID) {
	md.rlsMeta.MaybeInit(user, isAdmin)
	md.rlsMeta.AddTableUse(tableID)
}

// ClearRLSEnabled will clear out the initialized state for the rls meta. This
// is used as a test helper.
func (md *Metadata) ClearRLSEnabled() {
	md.rlsMeta.Clear()
}

// GetRLSMeta returns the rls metadata struct
func (md *Metadata) GetRLSMeta() *RowLevelSecurityMeta {
	return &md.rlsMeta
}

// checkRLSDependencies will check the metadata for row-level security
// dependencies to see if it is up to date.
func (md *Metadata) checkRLSDependencies(
	ctx context.Context, evalCtx *eval.Context, optCatalog cat.Catalog,
) (upToDate bool, err error) {
	// rlsMeta is lazily updated. If we didn't initialize it, then we didn't come
	// across any RLS enabled tables. So, from a rls point of view the memo is up
	// to date.
	if !md.rlsMeta.IsInitialized {
		return true, nil
	}

	// RLS policies that get applied could differ vastly based on the role. So, if
	// the user is different, we cannot trust anything in the current memo.
	if md.rlsMeta.User != evalCtx.SessionData().User() {
		return false, nil
	}

	// If the role membership changes, resulting in the user gaining or losing
	// admin privileges, the memo is considered stale. Admins are exempt from
	// RLS policies.
	if hasAdminRole, err := optCatalog.HasAdminRole(ctx); err != nil {
		return false, err
	} else if md.rlsMeta.HasAdminRole != hasAdminRole {
		return false, nil
	}

	// We do not check for specific policy changes. Any time a policy is modified
	// on a table, a new version of the table descriptor is created. The metadata
	// dependency check already accounts for changes in the table descriptor version.
	return true, nil
}
