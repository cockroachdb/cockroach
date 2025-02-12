// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cat contains interfaces that are used by the query optimizer to avoid
// including specifics of sqlbase structures in the opt code.
package cat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// StableID permanently and uniquely identifies a catalog object (table, view,
// index, column, etc.) within its scope:
//
//	data source StableID: unique within database
//	index StableID: unique within table
//	column StableID: unique within table
//
// If a new catalog object is created, it will always be assigned a new StableID
// that has never, and will never, be reused by a different object in the same
// scope. This uniqueness guarantee is true even if the new object has the same
// name and schema as an old (and possibly dropped) object. The StableID will
// never change as long as the object exists.
//
// Note that while two instances of the same catalog object will always have the
// same StableID, they can have different schema if the schema has changed over
// time. See the Version type comments for more details.
//
// For most sqlbase objects, the StableID is the 32-bit descriptor ID. However,
// this is not always the case. For example, the StableID for virtual tables
// prepends the database ID, since the same descriptor ID is reused across
// databases.
type StableID uint64

// DefaultStableID is the uninitialized stable ID, used to represent an
// invalid or default state for catalog objects. GlobalPrivilege objects
// always return this value as their ID.
const DefaultStableID = StableID(catid.InvalidDescID)

// InvalidVersion is the version number used to represent objects that
// are not versioned in any way. For example the GlobalPrivilege object,
// which is not backed by a descriptor.
const InvalidVersion = uint64(0)

// SchemaName is an alias for tree.ObjectNamePrefix, since it consists of the
// catalog + schema name.
type SchemaName = tree.ObjectNamePrefix

// Flags allows controlling aspects of some Catalog operations.
type Flags struct {
	// AvoidDescriptorCaches avoids using any cached descriptors (for tables,
	// views, schemas, etc). This is useful in cases where we are running a
	// statement like SHOW and we don't want to get table leases or otherwise
	// pollute the caches.
	AvoidDescriptorCaches bool

	// NoTableStats doesn't retrieve table statistics. This should be used in all
	// cases where we don't need them (like SHOW variants), to avoid polluting the
	// stats cache.
	NoTableStats bool

	// IncludeOfflineTables considers offline tables (e.g. being imported). This is
	// useful in cases where we are running a statement like `SHOW RANGES` for
	// which we also want to show valid ranges when a table is being imported
	// (offline).
	IncludeOfflineTables bool

	// IncludeNonActiveIndexes considers non-active indexes (e.g. being
	// added). This is useful in cases where we are running a statement
	// like `SHOW RANGES` for which we also want to show valid ranges
	// when a table is being imported (offline).
	IncludeNonActiveIndexes bool
}

// DependencyDigest can be stored and confirm that all catalog objects resolved,
// at the current point in time will have the same version, stats, valid privileges,
// and name resolution.
type DependencyDigest struct {
	// LeaseGeneration tracks if any new version of descriptors has been published
	// by the lease manager.
	LeaseGeneration int64

	// StatsGeneration tracks if any new statistics have been published.
	StatsGeneration int64

	// SystemConfig tracks the current system config, which is refreshed on
	// any zone config update.
	SystemConfig *config.SystemConfig

	CurrentDatabase string
	SearchPath      sessiondata.SearchPath
	CurrentUser     username.SQLUsername
}

// Equal compares if two dependency digests match.
func (d *DependencyDigest) Equal(other *DependencyDigest) bool {
	return d.LeaseGeneration == other.LeaseGeneration &&
		d.StatsGeneration == other.StatsGeneration &&
		// Note: If the system config is modified a new SystemConfig structure
		// is always allocated. Individual fields cannot change on the caller,
		// so for the purpose of the dependency digest its sufficient to just
		// compare / store pointers.
		d.SystemConfig == other.SystemConfig &&
		d.CurrentDatabase == other.CurrentDatabase &&
		d.SearchPath.Equals(&other.SearchPath) &&
		d.CurrentUser == other.CurrentUser
}

// Clear clears a dependency digest.
func (d *DependencyDigest) Clear() {
	*d = DependencyDigest{}
}

// Catalog is an interface to a database catalog, exposing only the information
// needed by the query optimizer.
//
// NOTE: Catalog implementations need not be thread-safe. However, the objects
// returned by the Resolve methods (schemas and data sources) *must* be
// immutable after construction, and therefore also thread-safe.
type Catalog interface {
	// LookupDatabaseName locates a database with the given name and returns
	// the name if found. If no name is provided, it will return the name of
	// the current database. An error is returned if no database with the given
	// name exists or in the case of an empty name, there is no current database.
	// TODO(yang): This function can be extended if needed in the future
	// to return a new cat.Database type similar to ResolveSchema.
	LookupDatabaseName(ctx context.Context, flags Flags, name string) (tree.Name, error)

	// ResolveSchema locates a schema with the given name and returns it along
	// with the resolved SchemaName (which has all components filled in).
	// If the SchemaName is empty, returns the current database/schema (if one is
	// set; otherwise returns an error).
	//
	// The resolved SchemaName is the same with the resulting Schema.Name() except
	// that it has the ExplicitCatalog/ExplicitSchema flags set to correspond to
	// the input name. Its use is mainly for cosmetic purposes.
	//
	// If no such schema exists, then ResolveSchema returns an error.
	//
	// NOTE: The returned schema must be immutable after construction, and so can
	// be safely copied or used across goroutines.
	ResolveSchema(ctx context.Context, flags Flags, name *SchemaName) (Schema, SchemaName, error)

	// GetAllSchemaNamesForDB Gets all the SchemaNames for a database.
	GetAllSchemaNamesForDB(ctx context.Context, dbName string) ([]SchemaName, error)

	// ResolveDataSource locates a data source with the given name and returns it
	// along with the resolved DataSourceName.
	//
	// The resolved DataSourceName is the same with the resulting
	// DataSource.Name() except that it has the ExplicitCatalog/ExplicitSchema
	// flags set to correspond to the input name. Its use is mainly for cosmetic
	// purposes. For example: the input name might be "t". The fully qualified
	// DataSource.Name() would be "currentdb.public.t"; the returned
	// DataSourceName would have the same fields but would still be formatted as
	// "t".
	//
	// If no such data source exists, then ResolveDataSource returns an error.
	//
	// NOTE: The returned data source must be immutable after construction, and
	// so can be safely copied or used across goroutines.
	ResolveDataSource(
		ctx context.Context, flags Flags, name *DataSourceName,
	) (DataSource, DataSourceName, error)

	// ResolveDataSourceByID is similar to ResolveDataSource, except that it
	// locates a data source by its StableID. See the comment for StableID for
	// more details.
	//
	// If the table is in the process of being added, returns an
	// "undefined relation" error but also returns isAdding=true.
	//
	// NOTE: The returned data source must be immutable after construction, and
	// so can be safely copied or used across goroutines.
	ResolveDataSourceByID(
		ctx context.Context, flags Flags, id StableID,
	) (_ DataSource, isAdding bool, _ error)

	// ResolveTypeByOID is used to look up a user defined type by ID.
	ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error)

	// ResolveType is used to resolve an unresolved object name.
	ResolveType(
		ctx context.Context, name *tree.UnresolvedObjectName,
	) (*types.T, error)

	// ResolveIndex is used to resolve index with a TableIndexName where name of
	// table, schema, database could be missing. Index is returned together with
	// name of the table/materialized view contains the index. Error is returned
	// if the index is not found.
	ResolveIndex(
		ctx context.Context, flags Flags, name *tree.TableIndexName,
	) (Index, DataSourceName, error)

	// ResolveFunction resolves a function by name.
	ResolveFunction(
		ctx context.Context, name tree.UnresolvedRoutineName, path tree.SearchPath,
	) (*tree.ResolvedFunctionDefinition, error)

	// ResolveFunctionByOID resolves a function overload by OID.
	ResolveFunctionByOID(ctx context.Context, oid oid.Oid) (*tree.RoutineName, *tree.Overload, error)

	// CheckPrivilege verifies that the given user has the given privilege on
	// the given catalog object. If not, then CheckPrivilege returns an error.
	CheckPrivilege(ctx context.Context, o Object, user username.SQLUsername, priv privilege.Kind) error

	// CheckAnyPrivilege verifies that the current user has any privilege on
	// the given catalog object. If not, then CheckAnyPrivilege returns an error.
	CheckAnyPrivilege(ctx context.Context, o Object) error

	// CheckExecutionPrivilege verifies that the given user has execution
	// privileges for the UDF with the given OID. If not, then CheckPrivilege
	// returns an error.
	CheckExecutionPrivilege(ctx context.Context, oid oid.Oid, user username.SQLUsername) error

	// HasAdminRole checks that the current user has admin privileges. If yes,
	// returns true. Returns an error if query on the `system.users` table failed
	HasAdminRole(ctx context.Context) (bool, error)

	// UserHasAdminRole checks if the specified user has admin privileges.
	UserHasAdminRole(ctx context.Context, user username.SQLUsername) (bool, error)

	// HasRoleOption converts the roleoption to its SQL column name and checks if
	// the user belongs to a role where the option has value true. Requires a
	// valid transaction to be open.
	//
	// This check should be done on the version of the privilege that is stored in
	// the role options table. Example: CREATEROLE instead of NOCREATEROLE.
	// NOLOGIN instead of LOGIN.
	HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error)

	// FullyQualifiedName retrieves the fully qualified name of a data source.
	// Note that:
	//  - this call may involve a database operation so it shouldn't be used in
	//    performance sensitive paths;
	//  - the fully qualified name of a data source object can change without the
	//    object itself changing (e.g. when a database is renamed).
	FullyQualifiedName(ctx context.Context, ds DataSource) (DataSourceName, error)

	// CheckRoleExists returns an error if the role does not exist.
	CheckRoleExists(ctx context.Context, role username.SQLUsername) error

	// Optimizer returns the query Optimizer used to optimize SQL statements
	// referencing objects in this catalog, if any.
	Optimizer() interface{}

	// GetCurrentUser returns the username.SQLUsername of the current session.
	GetCurrentUser() username.SQLUsername

	// GetDependencyDigest fetches the current dependency digest, which can be
	// used as a fast comparison to guarantee that all dependencies will resolve
	// exactly the same.
	GetDependencyDigest() DependencyDigest

	// LeaseByStableID leases out a descriptor by the stable ID, which will block
	// schema changes. The version of the underlying object is returned.
	LeaseByStableID(ctx context.Context, id StableID) (uint64, error)

	// GetRoutineOwner returns the username.SQLUsername of the routine's
	// (specified by routineOid) owner.
	GetRoutineOwner(ctx context.Context, routineOid oid.Oid) (username.SQLUsername, error)
}
