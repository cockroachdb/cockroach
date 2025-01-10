// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// BuildCtx wraps BuilderState and exposes various convenience methods for the
// benefit of the scbuildstmts package.
//
// All methods except those in Context and Dependencies may panic instead of
// explicitly returning errors.
type BuildCtx interface {
	context.Context
	ClusterAndSessionInfo
	SchemaFeatureChecker
	TemporarySchemaProvider
	BuilderState
	EventLogState
	TreeAnnotator
	TreeContextBuilder
	Telemetry
	NodeStatusInfo
	RegionProvider
	ZoneConfigProvider

	// Add adds an absent element to the BuilderState, targeting PUBLIC.
	Add(element scpb.Element)

	// AddTransient adds an absent element to the BuilderState, targeting
	// TRANSIENT_ABSENT.
	AddTransient(element scpb.Element)

	// Drop sets the ABSENT target on an existing element in the BuilderState.
	Drop(element scpb.Element)

	// WithNewSourceElementID wraps BuilderStateWithNewSourceElementID in a
	// BuildCtx return type.
	WithNewSourceElementID() BuildCtx

	// Codec returns the codec for the current tenant.
	Codec() keys.SQLCodec
}

// ClusterAndSessionInfo provides general cluster and session info.
type ClusterAndSessionInfo interface {

	// ClusterSettings returns the current cluster settings, as in execCfg.
	ClusterSettings() *cluster.Settings

	// SessionData returns the current session data, as in execCtx.
	SessionData() *sessiondata.SessionData
}

// BuilderState encapsulates the state of the planned schema changes, hiding
// its internal state to anything that ends up using it and only allowing
// state changes via the provided methods.
type BuilderState interface {
	ElementReferences
	NameResolver
	PrivilegeChecker
	TableHelpers
	FunctionHelpers
	SchemaHelpers

	// QueryByID returns all elements sharing the given descriptor ID.
	QueryByID(descID catid.DescID) ElementResultSet

	// Ensure ensures the presence of the given element in the BuilderState with
	// the given target status and metadata.
	Ensure(elem scpb.Element, target scpb.TargetStatus, meta scpb.TargetMetadata)

	// LogEventForExistingTarget tells the builder to write an entry in the event
	// log for the existing target corresponding to the provided element.
	// An error is thrown if no such target exists.
	LogEventForExistingTarget(element scpb.Element)

	// LogEventForExistingPayload is like LogEventForExistingTarget, but it allows
	// the caller to provide additional details of the payload.
	LogEventForExistingPayload(element scpb.Element, payload logpb.EventPayload)

	// GenerateUniqueDescID returns the next available descriptor id for a new
	// descriptor and mark the new id as being used for new descriptor, so that
	// the builder knows to avoid loading existing descriptor for decomposition.
	GenerateUniqueDescID() catid.DescID

	// BuildUserPrivilegesFromDefaultPrivileges generates owner and user
	// privileges elements from default privileges of the given database
	// and schemas for the given descriptor and object type.
	// `sc` can be nil if this is for building the schema itself (`descID` will
	// be the schema ID then).
	BuildUserPrivilegesFromDefaultPrivileges(
		db *scpb.Database, sc *scpb.Schema, descID descpb.ID, objType privilege.TargetObjectType, owner username.SQLUsername,
	) (*scpb.Owner, []*scpb.UserPrivileges)
}

// EventLogState encapsulates the state of the metadata to decorate the eventlog
// with.
type EventLogState interface {

	// TargetMetadata returns the current scpb.TargetMetadata for this state.
	TargetMetadata() scpb.TargetMetadata

	// IncrementSubWorkID increments the current subwork ID used for tracking
	// when a statement does operations on multiple objects or in multiple
	// commands.
	IncrementSubWorkID()

	// EventLogStateWithNewSourceElementID returns an EventLogState with an
	// incremented source element ID
	EventLogStateWithNewSourceElementID() EventLogState
}

// TreeContextBuilder exposes convenient tree-package context builder methods.
type TreeContextBuilder interface {

	// SemaCtx returns a new tree.SemaContext.
	SemaCtx() *tree.SemaContext

	// EvalCtx returns a new eval.Context.
	EvalCtx() *eval.Context
}

// TreeAnnotator provides interfaces to be able to modify the AST safely,
// by providing a copy and support for adding annotations.
type TreeAnnotator interface {

	// SetUnresolvedNameAnnotation sets an annotation on an unresolved object name.
	SetUnresolvedNameAnnotation(unresolvedName *tree.UnresolvedObjectName, ann interface{})

	// MarkNameAsNonExistent indicates that a table name is non-existent
	// in the AST, which will cause it to skip full namespace resolution
	// validation.
	MarkNameAsNonExistent(name *tree.TableName)
}

// Telemetry allows incrementing schema change telemetry counters.
type Telemetry interface {

	// IncrementSchemaChangeCreateCounter increments the selected CREATE telemetry
	// counter.
	IncrementSchemaChangeCreateCounter(counterType string)

	// IncrementSchemaChangeAlterCounter increments the selected ALTER telemetry
	// counter.
	IncrementSchemaChangeAlterCounter(counterType string, extra ...string)

	// IncrementSchemaChangeDropCounter increments the selected DROP telemetry
	// counter.
	IncrementSchemaChangeDropCounter(counterType string)

	// IncrementSchemaChangeAddColumnTypeCounter increments telemetry counters for
	// different types added as columns.
	IncrementSchemaChangeAddColumnTypeCounter(typeName string)

	// IncrementSchemaChangeAddColumnQualificationCounter increments telemetry
	// counters for different qualifications (default expressions) on a newly
	// added column.
	IncrementSchemaChangeAddColumnQualificationCounter(qualification string)

	// IncrementUserDefinedSchemaCounter increments the selected user-defined
	// schema telemetry counter.
	IncrementUserDefinedSchemaCounter(counterType sqltelemetry.UserDefinedSchemaTelemetryType)

	// IncrementEnumCounter increments the selected enum telemetry counter.
	IncrementEnumCounter(counterType sqltelemetry.EnumTelemetryType)

	// IncrementDropOwnedByCounter increments the DROP OWNED BY telemetry counter.
	IncrementDropOwnedByCounter()

	// IncrementSchemaChangeIndexCounter schema change counters related to index
	// features during creation.
	IncrementSchemaChangeIndexCounter(counterType string)
}

// SchemaFeatureChecker checks if a schema change feature is allowed by the
// database administrator.
type SchemaFeatureChecker interface {
	// CheckFeature returns if the feature name specified is allowed or disallowed,
	// by the database administrator.
	CheckFeature(ctx context.Context, featureName tree.SchemaFeatureName) error

	// CanPerformDropOwnedBy returns if we can do DROP OWNED BY for the
	// given role.
	CanPerformDropOwnedBy(
		ctx context.Context, role username.SQLUsername,
	) (bool, error)

	// CanCreateCrossDBSequenceOwnerRef returns if cross database sequence
	// owner references are allowed.
	CanCreateCrossDBSequenceOwnerRef() error

	// CanCreateCrossDBSequenceRef returns if cross database sequence
	// references are allowed.
	CanCreateCrossDBSequenceRef() error
}

// PrivilegeChecker checks an element's privileges.
type PrivilegeChecker interface {

	// HasOwnership returns true iff the current user owns the element.
	HasOwnership(e scpb.Element) bool

	// CheckPrivilege panics if the current user does not have the specified
	// privilege for the element.
	//
	// Note: This function is written on the assumption that privileges are tied
	// to descriptors. However, privileges can also live in the
	// `system.privileges` table (i.e. system-level privileges) and checking those
	// global privileges are done by the CheckGlobalPrivilege method below.
	CheckPrivilege(e scpb.Element, privilege privilege.Kind) error

	// CheckGlobalPrivilege panics if the current user does not have the specified
	// global privilege.
	CheckGlobalPrivilege(privilege privilege.Kind) error

	// HasGlobalPrivilegeOrRoleOption returns a bool representing whether the current user
	// has a global privilege or the corresponding legacy role option.
	HasGlobalPrivilegeOrRoleOption(ctx context.Context, privilege privilege.Kind) (bool, error)

	// CurrentUserHasAdminOrIsMemberOf returns true iff the current user is (1)
	// an admin or (2) has membership in the specified role.
	CurrentUserHasAdminOrIsMemberOf(role username.SQLUsername) bool

	// CurrentUser returns the user of current session.
	CurrentUser() username.SQLUsername

	// CheckRoleExists returns nil if `role` exists.
	CheckRoleExists(ctx context.Context, role username.SQLUsername) error
}

// TableHelpers has methods useful for creating new table elements.
type TableHelpers interface {

	// NextTableColumnID returns the ID that should be used for any new column
	// added to this table.
	NextTableColumnID(table *scpb.Table) catid.ColumnID

	// NextColumnFamilyID returns the ID that should be used for any new column
	// family added to this table.
	NextColumnFamilyID(table *scpb.Table) catid.FamilyID

	// NextTableIndexID returns the ID that should be used for any new index added
	// to this table.
	NextTableIndexID(tableID catid.DescID) catid.IndexID

	// NextViewIndexID returns the ID that should be used for any new index added
	// to this materialized view.
	NextViewIndexID(view *scpb.View) catid.IndexID

	// NextTableConstraintID returns the ID that should be used for any new constraint
	// added to this table.
	NextTableConstraintID(tableID catid.DescID) catid.ConstraintID

	// NextTableTriggerID returns the ID that should be used for any new trigger
	// added to this table.
	NextTableTriggerID(tableID catid.DescID) catid.TriggerID

	// NextTablePolicyID returns the ID that should be used for any new row-level
	// security policies added to this table.
	NextTablePolicyID(tableID catid.DescID) catid.PolicyID

	// NextTableTentativeIndexID returns the tentative ID, starting from
	// scbuild.TABLE_TENTATIVE_IDS_START, that should be used for any new index added to
	// this table.
	NextTableTentativeIndexID(tableID catid.DescID) catid.IndexID

	// NextTableTentativeConstraintID parallels NextTableTentativeIndexID and
	// returns tentative constraint ID, starting from scbuild.TABLE_TENTATIVE_IDS_START,
	// that should be used for any new index added to this table.
	NextTableTentativeConstraintID(tableID catid.DescID) catid.ConstraintID

	// IndexPartitioningDescriptor creates a new partitioning descriptor
	// for the secondary index element, or panics.
	IndexPartitioningDescriptor(indexName string,
		index *scpb.Index, keyColumns []*scpb.IndexColumn,
		partBy *tree.PartitionBy) catpb.PartitioningDescriptor

	// ResolveTypeRef resolves a type reference.
	ResolveTypeRef(typeref tree.ResolvableTypeReference) scpb.TypeT

	// WrapExpression constructs an expression wrapper given an AST.
	WrapExpression(parentID catid.DescID, expr tree.Expr) *scpb.Expression

	// ComputedColumnExpression returns a validated computed column expression
	// and its type.
	// TODO(postamar): make this more low-level instead of consuming an AST
	ComputedColumnExpression(tbl *scpb.Table, d *tree.ColumnTableDef) tree.Expr

	// PartialIndexPredicateExpression returns a validated partial predicate
	// wrapped expression
	PartialIndexPredicateExpression(
		tableID catid.DescID, expr tree.Expr,
	) tree.Expr

	// IsTableEmpty returns if the table is empty or not.
	IsTableEmpty(tbl *scpb.Table) bool
}

type FunctionHelpers interface {
	BuildReferenceProvider(stmt tree.Statement) ReferenceProvider
	WrapFunctionBody(fnID descpb.ID, bodyStr string, lang catpb.Function_Language,
		returnType tree.ResolvableTypeReference, provider ReferenceProvider) *scpb.FunctionBody
	ReplaceSeqTypeNamesInStatements(queryStr string, lang catpb.Function_Language) string
}

type SchemaHelpers interface {
	ResolveDatabasePrefix(schemaPrefix *tree.ObjectNamePrefix)
}

type ElementResultSet = *scpb.ElementCollection[scpb.Element]

// ElementReferences looks up an element's forward and backward references.
type ElementReferences interface {

	// ForwardReferences returns the set of elements to which we have forward
	// references in the given element. This includes the current element.
	ForwardReferences(e scpb.Element) ElementResultSet

	// BackReferences finds all descriptors with a back-reference to descriptor `id`
	// and return all elements that belong to them. Back-references also include
	// children, in the case of databases and schemas.
	BackReferences(id catid.DescID) ElementResultSet
}

// ResolveParams specifies the behavior of the methods in the
// NameResolver interface.
type ResolveParams struct {

	// IsExistenceOptional iff true causes the method to return nil when the
	// descriptor cannot be found, instead of panicking.
	IsExistenceOptional bool

	// RequiredPrivilege defines the privilege required for the resolved
	// descriptor.
	// If 0, no privilege checking is performed.
	RequiredPrivilege privilege.Kind

	// RequireOwnership if set to true, requires current user be the owner of the
	// resolved descriptor. It preempts RequiredPrivilege.
	RequireOwnership bool

	// WithOffline, if set, instructs the catalog reader to include offline
	// descriptors.
	WithOffline bool

	// ResolveTypes if set, instructs the catalog reader to resolve types
	// and not just tables, sequences, and views.
	ResolveTypes bool

	// InDropContext, if set, indicates that overload resolution is being
	// performed in the DROP routine context.
	InDropContext bool
}

// NameResolver looks up elements in the catalog by name, and vice-versa.
type NameResolver interface {

	// NamePrefix returns the name prefix for the descriptor backing the given
	// element. This is constructed on a best-effort basis.
	NamePrefix(e scpb.Element) tree.ObjectNamePrefix

	// ResolveDatabase retrieves a database by name and returns its elements.
	ResolveDatabase(name tree.Name, p ResolveParams) ElementResultSet

	// ResolveSchema retrieves a schema by name and returns its elements.
	ResolveSchema(name tree.ObjectNamePrefix, p ResolveParams) ElementResultSet

	// ResolveTargetObject retrieves database and schema given the unresolved
	// object name. The requested schema must exist and current user must have the
	// required privilege.
	ResolveTargetObject(prefix *tree.UnresolvedObjectName, requiredSchemaPriv privilege.Kind) (dbElts ElementResultSet, scElts ElementResultSet)

	// ResolveUserDefinedTypeType retrieves a type by name and returns its elements.
	ResolveUserDefinedTypeType(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveRelation retrieves a relation by name and returns its elements.
	ResolveRelation(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveTable retrieves a table by name and returns its elements.
	ResolveTable(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolvePhysicalTable retrieves a table, materialized view, or sequence
	// by name and returns its elements.
	ResolvePhysicalTable(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveSequence retrieves a sequence by name and returns its elements.
	ResolveSequence(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveView retrieves a view by name and returns its elements.
	ResolveView(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveIndex retrieves an index by name and returns its elements.
	ResolveIndex(relationID catid.DescID, indexName tree.Name, p ResolveParams) ElementResultSet

	// ResolveRoutine retrieves a user defined function or a stored procedure
	// and returns its elements.
	ResolveRoutine(routineObj *tree.RoutineObj, p ResolveParams, routineType tree.RoutineType) ElementResultSet

	// ResolveIndexByName retrieves a table which contains the target
	// index and returns its elements. Name of database, schema or table may be
	// missing.
	// If index is not found, it returns nil if p.IsExistenceOptional or panic if !p.IsExistenceOptional.
	ResolveIndexByName(tableIndexName *tree.TableIndexName, p ResolveParams) ElementResultSet

	// ResolveColumn retrieves a column by name and returns its elements.
	// N.B. Column target statuses should be handled outside of this logic (ex. resolving a column by name that is in the
	// dropping state shouldn't prevent a column of the same name being added).
	ResolveColumn(relationID catid.DescID, columnName tree.Name, p ResolveParams) ElementResultSet

	// ResolveConstraint retrieves a constraint by name and returns its elements.
	ResolveConstraint(relationID catid.DescID, constraintName tree.Name, p ResolveParams) ElementResultSet

	// ResolveTrigger retrieves a trigger by name and returns its elements.
	ResolveTrigger(relationID catid.DescID, triggerName tree.Name, p ResolveParams) ElementResultSet

	// ResolvePolicy retrieves a policy by name and returns its elements.
	ResolvePolicy(relationID catid.DescID, policyName tree.Name, p ResolveParams) ElementResultSet
}

// ReferenceProvider provides all referenced objects with in current DDL
// statement. For example, CREATE VIEW and CREATE FUNCTION both could reference
// other objects, and cross-references need to probably tracked.
type ReferenceProvider interface {
	// ForEachTableReference iterate through all referenced tables and the
	// reference details with the given function.
	ForEachTableReference(f func(tblID descpb.ID, idxID descpb.IndexID, colIDs descpb.ColumnIDs) error) error
	// ForEachViewReference iterate through all referenced views and the reference
	// details with the given function.
	ForEachViewReference(f func(viewID descpb.ID, colIDs descpb.ColumnIDs) error) error
	// ForEachFunctionReference iterates through all referenced functions for each
	// function.
	ForEachFunctionReference(f func(id descpb.ID) error) error
	// ReferencedSequences returns all referenced sequence IDs
	ReferencedSequences() catalog.DescriptorIDSet
	// ReferencedTypes returns all referenced type IDs (not including implicit
	// table types)
	ReferencedTypes() catalog.DescriptorIDSet
	// ReferencedRelationIDs Returns all referenced relation IDs.
	ReferencedRelationIDs() catalog.DescriptorIDSet
	// ReferencedRoutines returns all referenced routine IDs.
	ReferencedRoutines() catalog.DescriptorIDSet
}

// TemporarySchemaProvider provides functions needed to help support
// temporary schemas.
type TemporarySchemaProvider interface {
	// TemporarySchemaName gets the name of the temporary schema for the current
	// session.
	TemporarySchemaName() string
}

// NodeStatusInfo provides access to observe node descriptors.
type NodeStatusInfo interface {

	// NodesStatusServer gives access to the NodesStatus service and is only
	// available when running as a system tenant.
	NodesStatusServer() *serverpb.OptionalNodesStatusServer
}

// RegionProvider abstracts the lookup of regions. It is used to implement
// crdb_internal.regions, which ultimately drives `SHOW REGIONS` and the
// logic in the commands to manipulate multi-region features.
type RegionProvider interface {
	// GetRegions provides access to the set of regions available to the
	// current tenant.
	GetRegions(ctx context.Context) (*serverpb.RegionsResponse, error)

	// SynthesizeRegionConfig returns a RegionConfig that describes the
	// multiregion setup for the given database ID.
	SynthesizeRegionConfig(
		ctx context.Context,
		dbID descpb.ID,
		opts ...multiregion.SynthesizeRegionConfigOption,
	) (multiregion.RegionConfig, error)
}

type ZoneConfigProvider interface {
	// ZoneConfigGetter returns the zone config getter.
	ZoneConfigGetter() scdecomp.ZoneConfigGetter

	// GetDefaultZoneConfig is used to get the default zone config inside the
	// server.
	GetDefaultZoneConfig() *zonepb.ZoneConfig
}
