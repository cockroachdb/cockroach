// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

// DatabaseRegionConfig is a wrapper around multiregion.RegionConfig
// related methods which avoids a circular dependency between descpb and tree.
type DatabaseRegionConfig interface {
	IsValidRegionNameString(r string) bool
	PrimaryRegionString() string
}

// HasAnyPrivilegeResult represents the non-error results of calling HasAnyPrivilege
type HasAnyPrivilegeResult = int8

const (
	// HasPrivilege means at least one of the specified privileges is granted.
	HasPrivilege HasAnyPrivilegeResult = 1
	// HasNoPrivilege means no privileges are granted.
	HasNoPrivilege HasAnyPrivilegeResult = 0
	// ObjectNotFound means the object that privileges are being checked on was not found.
	ObjectNotFound HasAnyPrivilegeResult = -1
)

// DatabaseCatalog consists of functions that reference the session database
// and is to be used from Context.
type DatabaseCatalog interface {

	// ResolveTableName expands the given table name and
	// makes it point to a valid object.
	// If the database name is not given, it uses the search path to find it, and
	// sets it on the returned TableName.
	// It returns the ID of the resolved table, and an error if the table doesn't exist.
	ResolveTableName(ctx context.Context, tn *tree.TableName) (tree.ID, error)

	// SchemaExists looks up the schema with the given name and determines
	// whether it exists.
	SchemaExists(ctx context.Context, dbName, scName string) (found bool, err error)

	// HasAnyPrivilegeForSpecifier returns whether the current user has privilege
	// to access the given object.
	HasAnyPrivilegeForSpecifier(ctx context.Context, specifier HasPrivilegeSpecifier, user username.SQLUsername, privs []privilege.Privilege) (HasAnyPrivilegeResult, error)
}

// CastFunc is a function which cases a datum to a given type.
type CastFunc = func(context.Context, tree.Datum, *types.T) (tree.Datum, error)

// CatalogBuiltins is a set of methods which can be implemented using the
// lower-level descs.Collection for use in builtins. Its functionality is
// available also during DistSQL making it possible to implement these
// functions without disallowing DistSQL.
//
// TODO(ajwerner): Ideally we'd peel more and more catalog functionality off
// of the Planner interface as we subsume its privilege checking into an
// intermediate layer.
type CatalogBuiltins interface {
	// EncodeTableIndexKey constructs a deterministic and immutable encoding of
	// a table index key from a tuple of datums. It is leveraged as the
	// input to a hash function for hash-sharded indexes.
	EncodeTableIndexKey(
		ctx context.Context,
		tableID catid.DescID,
		indexID catid.IndexID,
		rowDatums *tree.DTuple,
		performCast CastFunc,
	) ([]byte, error)

	// NumGeometryInvertedIndexEntries computes the number of inverted index
	// entries we'd expect to generate from a given geometry value given the
	// index's configuration.
	NumGeometryInvertedIndexEntries(
		ctx context.Context, tableID catid.DescID, indexID catid.IndexID, g *tree.DGeometry,
	) (int, error)

	// NumGeographyInvertedIndexEntries computes the number of inverted index
	// entries we'd expect to generate from a given geography value given the
	// index's configuration.
	NumGeographyInvertedIndexEntries(
		ctx context.Context, tableID catid.DescID, indexID catid.IndexID, g *tree.DGeography,
	) (int, error)

	// PGColumnIsUpdatable returns whether the given column can be updated.
	PGColumnIsUpdatable(
		ctx context.Context, oidArg *tree.DOid, attNumArg tree.DInt,
	) (*tree.DBool, error)

	// PGRelationIsUpdatable returns the update events the relation supports.
	PGRelationIsUpdatable(ctx context.Context, oid *tree.DOid) (*tree.DInt, error)

	// RedactDescriptor expects an encoded protobuf descriptor, decodes it,
	// redacts its expressions, and re-encodes it.
	RedactDescriptor(ctx context.Context, encodedDescriptor []byte) ([]byte, error)

	// RepairedDescriptor expects an encoded protobuf descriptor,
	// decodes it,
	// puts it into a catalog.DescriptorBuilder,
	// calls RunPostDeserializationChanges,
	// calls StripDanglingBackReferences,
	// calls StripNonExistentRoles,
	// and re-encodes it.
	RepairedDescriptor(
		ctx context.Context,
		encodedDescriptor []byte,
		descIDMightExist func(id descpb.ID) bool,
		nonTerminalJobIDMightExist func(id jobspb.JobID) bool,
		roleExists func(username username.SQLUsername) bool,
	) ([]byte, error)
}

// HasPrivilegeSpecifier specifies an object to lookup privilege for.
// Only one of { DatabaseName, DatabaseOID, SchemaName, TableName, TableOID } is filled.
type HasPrivilegeSpecifier struct {

	// Database privilege
	DatabaseName *string
	DatabaseOID  *oid.Oid

	// Schema privilege
	// Schema OID must be converted to name before using HasPrivilegeSpecifier.
	SchemaName *string
	// SchemaDatabaseName is required when SchemaName is used.
	SchemaDatabaseName *string
	// Because schemas cannot be looked up by OID directly,
	// this controls whether the result is nil (originally queried by OID) or an error (originally queried by name).
	SchemaIsRequired *bool

	// Table privilege
	TableName *string
	TableOID  *oid.Oid
	// Sequences are stored internally as a table.
	IsSequence *bool

	// Column privilege
	// Requires TableName or TableOID.
	// Only one of ColumnName, ColumnAttNum is filled.
	ColumnName   *tree.Name
	ColumnAttNum *uint32

	// Routine privilege
	// This needs to be a user-defined function OID. Builtin function OIDs won't
	// work since they're not descriptors based.
	FunctionOID *oid.Oid
}

// TypeResolver is an interface for resolving types and type OIDs.
type TypeResolver interface {
	tree.TypeReferenceResolver

	// ResolveOIDFromString looks up the populated value of the OID with the
	// desired resultType which matches the provided name.
	//
	// The return value is a fresh DOid of the input oid.Oid with name and OID
	// set to the result of the query. If there was not exactly one result to the
	// query, an error will be returned.
	ResolveOIDFromString(
		ctx context.Context, resultType *types.T, toResolve *tree.DString,
	) (_ *tree.DOid, errSafeToIgnore bool, _ error)

	// ResolveOIDFromOID looks up the populated value of the oid with the
	// desired resultType which matches the provided oid.
	//
	// The return value is a fresh DOid of the input oid.Oid with name and OID
	// set to the result of the query. If there was not exactly one result to the
	// query, an error will be returned.
	ResolveOIDFromOID(
		ctx context.Context, resultType *types.T, toResolve *tree.DOid,
	) (_ *tree.DOid, errSafeToIgnore bool, _ error)
}

// Planner is a limited planner that can be used from EvalContext.
type Planner interface {
	DatabaseCatalog
	TypeResolver
	tree.FunctionReferenceResolver

	// Mon returns the Planner's monitor.
	//
	// TODO(yuzefovich): memory usage against this monitor doesn't count against
	// sql.mem.distsql.current metric, audit the callers to see whether this is
	// undesirable in some places.
	Mon() *mon.BytesMonitor

	// ExecutorConfig returns *ExecutorConfig
	ExecutorConfig() interface{}

	// GetTypeFromValidSQLSyntax parses a column type when the input
	// string uses the parseable SQL representation of a type name, e.g.
	// `INT(13)`, `mytype`, `"mytype"`, `pg_catalog.int4` or `"public".mytype`.
	GetTypeFromValidSQLSyntax(ctx context.Context, sql string) (*types.T, error)

	// EvalSubquery returns the Datum for the given subquery node.
	EvalSubquery(expr *tree.Subquery) (tree.Datum, error)

	// EvalRoutineExpr evaluates a routine with the given argument datums and
	// returns the resulting datum.
	EvalRoutineExpr(
		ctx context.Context, expr *tree.RoutineExpr, args tree.Datums,
	) (tree.Datum, error)

	// RoutineExprGenerator returns a ValueGenerator that produces the results
	// of the routine.
	RoutineExprGenerator(
		ctx context.Context, expr *tree.RoutineExpr, args tree.Datums,
	) ValueGenerator

	// EvalTxnControlExpr produces the side effects of a COMMIT or ROLLBACK
	// statement within a PL/pgSQL stored procedure. See the sql.planner
	// implementation for details.
	EvalTxnControlExpr(
		ctx context.Context, expr *tree.TxnControlExpr, args tree.Datums,
	) (tree.Datum, error)

	// GenerateTestObjects is used to generate a large number of
	// objets quickly.
	// Note: we pass parameters as a string to avoid a package
	// dependency to randgen from users of this interface;
	GenerateTestObjects(ctx context.Context, parameters string) (string, error)

	// UnsafeUpsertDescriptor is used to repair descriptors in dire
	// circumstances. See the comment on the planner implementation.
	UnsafeUpsertDescriptor(
		ctx context.Context, descID int64, encodedDescriptor []byte, force bool,
	) error

	// UnsafeDeleteDescriptor is used to repair descriptors in dire
	// circumstances. See the comment on the planner implementation.
	UnsafeDeleteDescriptor(ctx context.Context, descID int64, force bool) error

	// ForceDeleteTableData cleans up underlying data for a table
	// descriptor ID. See the comment on the planner implementation.
	ForceDeleteTableData(ctx context.Context, descID int64) error

	// UpsertDroppedRelationGCTTL is used to upsert the GC TTL in the zone
	// configuration of a dropped table, sequence or materialized view.
	UpsertDroppedRelationGCTTL(ctx context.Context, id int64, ttl duration.Duration) error

	// UnsafeUpsertNamespaceEntry is used to repair namespace entries in dire
	// circumstances. See the comment on the planner implementation.
	UnsafeUpsertNamespaceEntry(
		ctx context.Context,
		parentID, parentSchemaID int64,
		name string,
		descID int64,
		force bool,
	) error

	// UnsafeDeleteNamespaceEntry is used to repair namespace entries in dire
	// circumstances. See the comment on the planner implementation.
	UnsafeDeleteNamespaceEntry(
		ctx context.Context,
		parentID, parentSchemaID int64,
		name string,
		descID int64,
		force bool,
	) error

	// UserHasAdminRole returns tuple of bool and error:
	// (true, nil) means that the user has an admin role (i.e. root or node)
	// (false, nil) means that the user has NO admin role
	// (false, err) means that there was an error running the query on
	// the `system.users` table
	UserHasAdminRole(ctx context.Context, user username.SQLUsername) (bool, error)

	// MemberOfWithAdminOption is used to collect a list of roles (direct and
	// indirect) that the member is part of. See the comment on the planner
	// implementation in authorization.go
	MemberOfWithAdminOption(
		ctx context.Context,
		member username.SQLUsername,
	) (map[username.SQLUsername]bool, error)

	// ExternalReadFile reads the content from an external file URI.
	ExternalReadFile(ctx context.Context, uri string) ([]byte, error)

	// ExternalWriteFile writes the content to an external file URI.
	ExternalWriteFile(ctx context.Context, uri string, content []byte) error

	// DecodeGist exposes gist functionality to the builtin functions.
	DecodeGist(ctx context.Context, gist string, external bool) ([]string, error)

	// SerializeSessionState serializes the variables in the current session
	// and returns a state, in bytes form.
	SerializeSessionState() (*tree.DBytes, error)

	// DeserializeSessionState deserializes the state as serialized variables
	// into the current session.
	DeserializeSessionState(ctx context.Context, state *tree.DBytes) (*tree.DBool, error)

	// CreateSessionRevivalToken creates a token that can be used to log in
	// as the current user, in bytes form.
	CreateSessionRevivalToken() (*tree.DBytes, error)

	// ValidateSessionRevivalToken checks if the given bytes are a valid
	// session revival token.
	ValidateSessionRevivalToken(token *tree.DBytes) (*tree.DBool, error)

	// RevalidateUniqueConstraintsInCurrentDB verifies that all unique constraints
	// defined on tables in the current database are valid. In other words, it
	// verifies that for every table in the database with one or more unique
	// constraints, all rows in the table have unique values for every unique
	// constraint defined on the table.
	RevalidateUniqueConstraintsInCurrentDB(ctx context.Context) error

	// RevalidateUniqueConstraintsInTable verifies that all unique constraints
	// defined on the given table are valid. In other words, it verifies that all
	// rows in the table have unique values for every unique constraint defined on
	// the table.
	RevalidateUniqueConstraintsInTable(ctx context.Context, tableID int) error

	// RevalidateUniqueConstraint verifies that the given unique constraint on the
	// given table is valid. In other words, it verifies that all rows in the
	// table have unique values for the columns in the constraint. Returns an
	// error if validation fails or if constraintName is not actually a unique
	// constraint on the table.
	RevalidateUniqueConstraint(ctx context.Context, tableID int, constraintName string) error

	// IsConstraintActive returns if a given constraint is currently active,
	// for the current transaction.
	IsConstraintActive(ctx context.Context, tableID int, constraintName string) (bool, error)

	// ValidateTTLScheduledJobsInCurrentDB checks scheduled jobs for each table
	// in the database maps to a scheduled job.
	ValidateTTLScheduledJobsInCurrentDB(ctx context.Context) error
	// RepairTTLScheduledJob repairs the scheduled job for the given table if
	// it is invalid.
	RepairTTLScheduledJobForTable(ctx context.Context, tableID int64) error

	// FingerprintSpan calculates a fingerprint for the given span. If a
	// startTime is passed and allRevisions is true, then the fingerprint
	// includes the MVCC history between startTime and the read timestamp of
	// the transaction.
	FingerprintSpan(ctx context.Context, span roachpb.Span, startTime hlc.Timestamp, allRevisions bool, stripped bool) (uint64, error)

	// QueryRowEx executes the supplied SQL statement and returns a single row, or
	// nil if no row is found, or an error if more that one row is returned.
	//
	// The fields set in session that are set override the respective fields if
	// they have previously been set through SetSessionData().
	QueryRowEx(ctx context.Context, opName redact.RedactableString, override sessiondata.InternalExecutorOverride, stmt string, qargs ...interface{}) (tree.Datums, error)

	// QueryIteratorEx executes the query, returning an iterator that can be used
	// to get the results. If the call is successful, the returned iterator
	// *must* be closed.
	//
	// The fields set in session that are set override the respective fields if they
	// have previously been set through SetSessionData().
	QueryIteratorEx(ctx context.Context, opName redact.RedactableString, override sessiondata.InternalExecutorOverride, stmt string, qargs ...interface{}) (InternalRows, error)

	// IsActive returns if the version specified by key is active.
	IsActive(ctx context.Context, key clusterversion.Key) bool

	// GetMultiregionConfig synthesizes a new multiregion.RegionConfig describing
	// the multiregion properties of the database identified via databaseID. The
	// second return value is false if the database doesn't exist or is not
	// multiregion.
	GetMultiregionConfig(ctx context.Context, databaseID descpb.ID) (interface{}, bool)

	// IsANSIDML returns true if the statement being planned is one of the 4 DML
	// statements, SELECT, UPDATE, INSERT, DELETE, or an EXPLAIN of one of these
	// statements.
	IsANSIDML() bool

	// EnforceHomeRegion returns true if the statement being planned is an ANSI
	// DML statement and the enforce_home_region session setting is true.
	EnforceHomeRegion() bool

	// GetRangeDescIterator gets a rangedesc.Iterator for the specified span.
	GetRangeDescIterator(context.Context, roachpb.Span) (rangedesc.Iterator, error)

	// GetRangeDescByID gets the RangeDescriptor by the specified RangeID.
	GetRangeDescByID(context.Context, roachpb.RangeID) (roachpb.RangeDescriptor, error)

	SpanStats(context.Context, roachpb.Spans) (*roachpb.SpanStatsResponse, error)

	GetDetailsForSpanStats(ctx context.Context, dbId int, tableId int) (InternalRows, error)

	// MaybeReallocateAnnotations makes a new annotations slice of size
	// numAnnotations if one is maintained by this Planner and the current one has
	// less than numAnnotations entries. If updated, the annotations in the eval
	// context held in the planner is also updated.
	MaybeReallocateAnnotations(numAnnotations tree.AnnotationIdx)

	// Optimizer returns the optimizer associated with this Planner, if any.
	Optimizer() interface{}

	// GenUniqueCursorName returns a name that is guaranteed to be unique among
	// the current list of cursors and portals. It is used to implement PLpgSQL
	// OPEN statements when used with an unnamed cursor.
	GenUniqueCursorName() tree.Name

	// PLpgSQLCloseCursor closes the cursor with the given name, returning an
	// error if the cursor doesn't exist. It is used to implement the PLpgSQL
	// CLOSE statement.
	PLpgSQLCloseCursor(cursorName tree.Name) error

	// PLpgSQLFetchCursor returns the next row from the cursor with the given
	// name, if any. It returns nil if no such row exists. Used to implement the
	// PLpgSQL FETCH statement.
	PLpgSQLFetchCursor(ctx context.Context, cursor *tree.CursorStmt) (res tree.Datums, err error)

	// AutoCommit indicates whether the Planner has flagged the current statement
	// as eligible for transaction auto-commit.
	AutoCommit() bool

	// StartHistoryRetentionJob creates a cluster-level protected timestamp
	// and a job that owns it.
	StartHistoryRetentionJob(ctx context.Context, desc string, protectTS hlc.Timestamp, expiration time.Duration) (jobspb.JobID, error)

	// ExtendHistoryRetention extends the lifetime of a a cluster-level
	// protected timestamp.
	ExtendHistoryRetention(ctx context.Context, id jobspb.JobID) error

	// InsertTemporarySchema inserts a temporary schema into the current session
	// data.
	InsertTemporarySchema(
		tempSchemaName string, databaseID descpb.ID, schemaID descpb.ID,
	)

	// ClearQueryPlanCache removes all entries from the node's query plan cache.
	ClearQueryPlanCache()

	// ClearTableStatsCache removes all entries from the node's table stats cache.
	ClearTableStatsCache()
}

// InternalRows is an iterator interface that's exposed by the internal
// executor. It provides access to the rows from a query.
// InternalRows is a copy of the one in sql/internal.go excluding the
// Types function - we don't need the Types function for use cases where
// QueryIteratorEx is used from the Executor on the Planner.
// Furthermore, we cannot include the Types function due to a cyclic
// dependency on colinfo.ResultColumns - we cannot import colinfo in tree.
type InternalRows interface {
	// Next advances the iterator by one row, returning false if there are no
	// more rows in this iterator or if an error is encountered (the latter is
	// then returned).
	//
	// The iterator is automatically closed when false is returned, consequent
	// calls to Next will return the same values as when the iterator was
	// closed.
	Next(context.Context) (bool, error)

	// Cur returns the row at the current position of the iterator. The row is
	// safe to hold onto (meaning that calling Next() or Close() will not
	// invalidate it).
	Cur() tree.Datums

	// Close closes this iterator, releasing any resources it held open. Close
	// is idempotent and *must* be called once the caller is done with the
	// iterator.
	Close() error
}

// CompactEngineSpanFunc is used to compact an engine key span at the given
// (nodeID, storeID). If we add more overloads to the compact_span builtin,
// this parameter list should be changed to a struct union to accommodate
// those overloads.
type CompactEngineSpanFunc func(
	ctx context.Context, nodeID, storeID int32, startKey, endKey []byte,
) error

// GetTableMetrics is used to retrieve sstable metrics on a key span
// (end-exclusive) at the given (nodeID, storeID).
type GetTableMetricsFunc func(
	ctx context.Context, nodeID, storeID int32, startKey, endKey []byte,
) ([]enginepb.SSTableMetricsInfo, error)

// ScanStorageInternalKeysFunc is used to retrieve pebble metrics on a key span
// (end-exclusive) at the given (nodeID, storeID).
// megabytesPerSecond is used to specify the maximmum number of bytes read per second.
type ScanStorageInternalKeysFunc func(
	ctx context.Context, nodeID, storeID int32, startKey, endKey []byte, megabytesPerSecond int64,
) ([]enginepb.StorageInternalKeysMetrics, error)

// SetCompactionConcurrencyFunc is used to change the compaction concurrency of a
// store.
type SetCompactionConcurrencyFunc func(
	ctx context.Context, nodeID, storeID int32, compactionConcurrency uint64,
) error

// SessionAccessor is a limited interface to access session variables.
type SessionAccessor interface {
	// SetSessionVar sets a session variable to a new value. If isLocal is true,
	// the setting change is scoped to the current transaction (as in SET LOCAL).
	//
	// This interface only supports strings as this is sufficient for
	// pg_catalog.set_config().
	SetSessionVar(ctx context.Context, settingName, newValue string, isLocal bool) error

	// GetSessionVar retrieves the current value of a session variable.
	GetSessionVar(ctx context.Context, settingName string, missingOk bool) (bool, string, error)

	// HasGlobalPrivilegeOrRoleOption checks if the current user has the
	// given global privilege, or the equivalent role option if one exists.
	HasGlobalPrivilegeOrRoleOption(ctx context.Context, privilege privilege.Kind) (bool, error)

	// CheckPrivilege verifies that the current user has `privilege` on `descriptor`.
	CheckPrivilege(ctx context.Context, privilegeObject privilege.Object, privilege privilege.Kind) error

	// HasViewActivityOrViewActivityRedactedRole returns true iff the current session user has the
	// VIEWACTIVITY or VIEWACTIVITYREDACTED permission.
	HasViewActivityOrViewActivityRedactedRole(ctx context.Context) (bool, bool, error)
}

// PreparedStatementState is a limited interface that exposes metadata about
// prepared statements.
type PreparedStatementState interface {
	// HasActivePortals returns true if there are portals in the session.
	HasActivePortals() bool
	// MigratablePreparedStatements returns a mapping of all prepared statements.
	MigratablePreparedStatements() []sessiondatapb.MigratableSession_PreparedStatement
	// HasPortal returns true if there exists a given named portal in the session.
	HasPortal(s string) bool
}

// ClientNoticeSender is a limited interface to send notices to the
// client.
//
// TODO(knz): as of this writing, the implementations of this
// interface only work on the gateway node (i.e. not from
// distributed processors).
type ClientNoticeSender interface {
	// BufferClientNotice buffers the notice in the command result to send to the
	// client. This is flushed before the connection is closed.
	BufferClientNotice(ctx context.Context, notice pgnotice.Notice)
	// SendClientNotice immediately flushes the notice to the client.
	// SendNotice sends the given notice to the client. The notice will be in
	// the client communication buffer until it is flushed. Flushing can be forced
	// to occur immediately by setting immediateFlush to true.
	// This is used to implement PLpgSQL RAISE statements; most cases should use
	// BufferClientNotice.
	SendClientNotice(ctx context.Context, notice pgnotice.Notice, immediateFlush bool) error
}

// DeferredRoutineSender allows a nested routine to send the information needed
// for its own evaluation to a parent routine. This is used to defer execution
// for tail-call optimization. It can only be used during local execution.
type DeferredRoutineSender interface {
	// CanOptimizeTailCall determines whether a nested routine in tail-call
	// position can be executed in its parent's context.
	CanOptimizeTailCall(nestedRoutine *tree.RoutineExpr) bool

	// SendDeferredRoutine sends a local nested routine and its arguments to its
	// parent routine.
	SendDeferredRoutine(nestedRoutine *tree.RoutineExpr, args tree.Datums)
}

// PrivilegedAccessor gives access to certain queries that would otherwise
// require someone with RootUser access to query a given data source.
// It is defined independently to prevent a circular dependency on sql, tree and sqlbase.
type PrivilegedAccessor interface {
	// LookupNamespaceID returns the id of the namespace given it's parent id and name.
	// It is meant as a replacement for looking up the system.namespace directly.
	// Returns the id, a bool representing whether the namespace exists, and an error
	// if there is one.
	LookupNamespaceID(
		ctx context.Context, parentID int64, parentSchemaID int64, name string,
	) (tree.DInt, bool, error)

	// LookupZoneConfigByNamespaceID returns the zone config given a namespace id.
	// It is meant as a replacement for looking up system.zones directly.
	// Returns the config byte array, a bool representing whether the namespace exists,
	// and an error if there is one.
	LookupZoneConfigByNamespaceID(ctx context.Context, id int64) (tree.DBytes, bool, error)

	// IsSystemTable returns if a given descriptor ID is a system table.s
	IsSystemTable(ctx context.Context, id int64) (bool, error)
}

// RegionOperator gives access to the current region, validation for all
// regions, and the ability to reset the zone configurations for tables
// or databases.
type RegionOperator interface {

	// CurrentDatabaseRegionConfig returns the RegionConfig of the current
	// session database.
	CurrentDatabaseRegionConfig(ctx context.Context) (DatabaseRegionConfig, error)

	// ValidateAllMultiRegionZoneConfigsInCurrentDatabase validates whether the current
	// database's multi-region zone configs are correctly setup. This includes
	// all tables within the database.
	ValidateAllMultiRegionZoneConfigsInCurrentDatabase(ctx context.Context) error

	// ResetMultiRegionZoneConfigsForTable resets the given table's zone
	// configuration to its multi-region default.
	ResetMultiRegionZoneConfigsForTable(ctx context.Context, id int64, forceZoneSurvival bool) error

	// ResetMultiRegionZoneConfigsForDatabase resets the given database's zone
	// configuration to its multi-region default.
	ResetMultiRegionZoneConfigsForDatabase(ctx context.Context, id int64) error
}

// SequenceOperators is used for various sql related functions that can
// be used from Context.
type SequenceOperators interface {

	// GetSerialSequenceNameFromColumn returns the sequence name for a given table and column
	// provided it is part of a SERIAL sequence.
	// Returns an empty string if the sequence name does not exist.
	GetSerialSequenceNameFromColumn(ctx context.Context, tableName *tree.TableName, columnName tree.Name) (*tree.TableName, error)

	// IncrementSequenceByID increments the given sequence and returns the result.
	// It returns an error if the given ID is not a sequence.
	// Takes in a sequence ID rather than a name, unlike IncrementSequence.
	IncrementSequenceByID(ctx context.Context, seqID int64) (int64, error)

	// GetLatestValueInSessionForSequenceByID returns the value most recently obtained by
	// nextval() for the given sequence in this session.
	// Takes in a sequence ID rather than a name, unlike GetLatestValueInSessionForSequence.
	GetLatestValueInSessionForSequenceByID(ctx context.Context, seqID int64) (int64, error)

	// SetSequenceValueByID sets the sequence's value.
	// If isCalled is false, the sequence is set such that the next time nextval() is called,
	// `newVal` is returned. Otherwise, the next call to nextval will return
	// `newVal + seqOpts.Increment`.
	// Takes in a sequence ID rather than a name, unlike SetSequenceValue.
	SetSequenceValueByID(ctx context.Context, seqID uint32, newVal int64, isCalled bool) error

	// GetLastSequenceValueByID returns the last value returned by the sequence,
	// not specific to any session. It also returns a flag to indicate if the
	// sequence has been called before.
	GetLastSequenceValueByID(ctx context.Context, seqID uint32) (value int64, wasCalled bool, err error)
}

// ChangefeedState is used to track progress and checkpointing for sinkless/core changefeeds.
// Because a CREATE CHANGEFEED statement for a sinkless changefeed will hang and return data
// over the SQL connection, this state belongs in the EvalCtx.
type ChangefeedState interface {
	// SetHighwater sets the frontier timestamp for the changefeed.
	SetHighwater(frontier hlc.Timestamp)

	// SetCheckpoint sets the checkpoint for the changefeed.
	SetCheckpoint(checkpoint *jobspb.TimestampSpansMap)
}

// TenantOperator is capable of interacting with tenant state, allowing SQL
// builtin functions to create, configure, and destroy tenants. The methods will
// return errors when run by any tenant other than the system tenant.
type TenantOperator interface {
	// CreateTenant attempts to create a new secondary tenant.
	CreateTenant(ctx context.Context, parameters string) (roachpb.TenantID, error)

	// DropTenantByID attempts to uninstall an existing tenant from the system.
	// It returns an error if the tenant does not exist. If synchronous is true
	// the gc job will not wait for a GC ttl.
	DropTenantByID(ctx context.Context, tenantID uint64, synchronous, ignoreServiceMode bool) error

	// LookupTenantID returns the ID for the given tenant name.o
	LookupTenantID(ctx context.Context, tenantName roachpb.TenantName) (roachpb.TenantID, error)

	// UpdateTenantResourceLimits reconfigures the tenant resource limits.
	// See multitenant.TenantUsageServer for more details on the arguments.
	UpdateTenantResourceLimits(
		ctx context.Context,
		tenantID uint64,
		availableTokens float64,
		refillRate float64,
		maxBurstTokens float64,
	) error
}

// JoinTokenCreator is capable of creating and persisting join tokens, allowing
// SQL builtin functions to create join tokens. The methods will return errors
// when run on multi-tenant clusters or with this functionality unavailable.
type JoinTokenCreator interface {
	// CreateJoinToken creates a new ephemeral join token and persists it
	// across the cluster. This join token can then be used to have new nodes
	// join the cluster and exchange certificates securely.
	CreateJoinToken(ctx context.Context) (string, error)
}

// GossipOperator is capable of manipulating the cluster's gossip network. The
// methods will return errors when run by any tenant other than the system
// tenant.
type GossipOperator interface {
	// TryClearGossipInfo attempts to clear an info object from the cluster's
	// gossip network.
	TryClearGossipInfo(ctx context.Context, key string) (bool, error)
}

// SQLStatsController is an interface embedded in EvalCtx which can be used by
// the builtins to reset SQL stats in the cluster. This interface is introduced
// to avoid circular dependency.
type SQLStatsController interface {
	ResetClusterSQLStats(ctx context.Context) error
	ResetActivityTables(ctx context.Context) error
	ResetInsightsTables(ctx context.Context) error
	CreateSQLStatsCompactionSchedule(ctx context.Context) error
}

// SchemaTelemetryController is an interface embedded in EvalCtx which can be
// used by the builtins to create a job schedule for schema telemetry jobs.
// This interface is introduced to avoid circular dependency.
type SchemaTelemetryController interface {
	CreateSchemaTelemetryJob(ctx context.Context, createdByName string, createdByID int64) (int64, error)
}

// IndexUsageStatsController is an interface embedded in EvalCtx which can be
// used by the builtins to reset index usage stats in the cluster. This interface
// is introduced to avoid circular dependency.
type IndexUsageStatsController interface {
	ResetIndexUsageStats(ctx context.Context) error
}

// StmtDiagnosticsRequestInsertFunc is an interface embedded in EvalCtx that can
// be used by the builtins to insert a statement diagnostics request. This
// interface is introduced to avoid circular dependency.
type StmtDiagnosticsRequestInsertFunc func(
	ctx context.Context,
	stmtFingerprint string,
	planGist string,
	antiPlanGist bool,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAfter time.Duration,
	redacted bool,
) error

// AsOfSystemTime represents the result from the evaluation of AS OF SYSTEM TIME
// clause.
type AsOfSystemTime struct {
	// Timestamp is the HLC timestamp evaluated from the AS OF SYSTEM TIME clause.
	Timestamp hlc.Timestamp
	// BoundedStaleness is true if the AS OF SYSTEM TIME clause specifies bounded
	// staleness should be used. If true, Timestamp specifies an (inclusive) lower
	// bound to read from - data can be read from a time later than Timestamp. If
	// false, data is returned at the exact Timestamp specified.
	BoundedStaleness bool
	// If this is a bounded staleness read, ensures we only read from the nearest
	// replica. The query will error if this constraint could not be satisfied.
	NearestOnly bool
	// If this is a bounded staleness read with nearest_only=True, this is set when
	// we failed to satisfy a bounded staleness read with a nearby replica as we
	// have no followers with an up-to-date schema.
	// This is be zero if there is no maximum bound.
	// In non-zero, we want a read t where Timestamp <= t < MaxTimestampBound.
	MaxTimestampBound hlc.Timestamp

	// ForBackfill indicates if this AOST expression was added to an operation
	// that requires a backfill, like CREATE TABLE AS.
	ForBackfill bool
}
