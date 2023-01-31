// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
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
	BuilderState
	EventLogState
	TreeAnnotator
	TreeContextBuilder
	Telemetry

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
	scpb.ElementStatusIterator
	ElementReferences
	NameResolver
	PrivilegeChecker
	TableHelpers
	FunctionHelpers

	// QueryByID returns all elements sharing the given descriptor ID.
	QueryByID(descID catid.DescID) ElementResultSet

	// Ensure ensures the presence of the given element in the BuilderState with
	// the given target status and metadata.
	Ensure(elem scpb.Element, target scpb.TargetStatus, meta scpb.TargetMetadata)

	// LogEventForExistingTarget tells the builder to write an entry in the event
	// log for the existing target corresponding to the provided element.
	// An error is thrown if no such target exists.
	LogEventForExistingTarget(element scpb.Element)

	// GenerateUniqueDescID returns the next available descriptor id for a new
	// descriptor and mark the new id as being used for new descriptor, so that
	// the builder knows to avoid loading existing descriptor for decomposition.
	GenerateUniqueDescID() catid.DescID

	// BuildUserPrivilegesFromDefaultPrivileges generates owner and user
	// privileges elements from default privileges of the given database
	// and schemas for the given descriptor and object type.
	BuildUserPrivilegesFromDefaultPrivileges(
		db *scpb.Database, sc *scpb.Schema, descID descpb.ID, objType privilege.TargetObjectType,
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
}

// PrivilegeChecker checks an element's privileges.
type PrivilegeChecker interface {

	// HasOwnership returns true iff the current user owns the element.
	HasOwnership(e scpb.Element) bool

	// CheckPrivilege panics if the current user does not have the specified
	// privilege for the element.
	CheckPrivilege(e scpb.Element, privilege privilege.Kind)

	// CurrentUserHasAdminOrIsMemberOf returns true iff the current user is (1)
	// an admin or (2) has membership in the specified role.
	CurrentUserHasAdminOrIsMemberOf(member username.SQLUsername) bool

	// CurrentUser returns the user of current session.
	CurrentUser() username.SQLUsername
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
	NextTableIndexID(table *scpb.Table) catid.IndexID

	// NextViewIndexID returns the ID that should be used for any new index added
	// to this materialized view.
	NextViewIndexID(view *scpb.View) catid.IndexID

	// NextTableConstraintID returns the ID that should be used for any new constraint
	// added to this table.
	NextTableConstraintID(id catid.DescID) catid.ConstraintID

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
	) *scpb.Expression

	// IsTableEmpty returns if the table is empty or not.
	IsTableEmpty(tbl *scpb.Table) bool
}

type FunctionHelpers interface {
	BuildReferenceProvider(stmt tree.Statement) ReferenceProvider
	WrapFunctionBody(fnID descpb.ID, bodyStr string, lang catpb.Function_Language, provider ReferenceProvider) *scpb.FunctionBody
}

// ElementResultSet wraps the results of an element query.
type ElementResultSet interface {
	scpb.ElementStatusIterator

	// IsEmpty returns true iff there are no elements in the result set.
	IsEmpty() bool

	// Filter returns a subset of this result set according to the predicate.
	Filter(predicate func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) bool) ElementResultSet
}

// ElementReferences looks up an element's forward and backward references.
type ElementReferences interface {

	// ForwardReferences returns the set of elements to which we have forward
	// references in the given element. This includes the current element.
	ForwardReferences(e scpb.Element) ElementResultSet

	// BackReferences returns the set of elements to which we have back-references
	// in the descriptor backing the given element. Back-references also include
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
	RequiredPrivilege privilege.Kind

	// RequireOwnership if set to true, requires current user be the owner of the
	// resolved descriptor. It preempts RequiredPrivilege.
	RequireOwnership bool
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

	// ResolvePrefix retrieves database and schema given the name prefix. The
	// requested schema must exist and current user must have the required
	// privilege.
	ResolvePrefix(
		prefix tree.ObjectNamePrefix, requiredSchemaPriv privilege.Kind,
	) (dbElts ElementResultSet, scElts ElementResultSet)

	// ResolveUserDefinedTypeType retrieves a type by name and returns its elements.
	ResolveUserDefinedTypeType(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveRelation retrieves a relation by name and returns its elements.
	ResolveRelation(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveTable retrieves a table by name and returns its elements.
	ResolveTable(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveSequence retrieves a sequence by name and returns its elements.
	ResolveSequence(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveView retrieves a view by name and returns its elements.
	ResolveView(name *tree.UnresolvedObjectName, p ResolveParams) ElementResultSet

	// ResolveIndex retrieves an index by name and returns its elements.
	ResolveIndex(relationID catid.DescID, indexName tree.Name, p ResolveParams) ElementResultSet

	// ResolveUDF retrieves a user defined function and returns its elements.
	ResolveUDF(fnObj *tree.FuncObj, p ResolveParams) ElementResultSet

	// ResolveIndexByName retrieves a table which contains the target
	// index and returns its elements. Name of database, schema or table may be
	// missing.
	// If index is not found, it returns nil if p.IsExistenceOptional or panic if !p.IsExistenceOptional.
	ResolveIndexByName(tableIndexName *tree.TableIndexName, p ResolveParams) ElementResultSet

	// ResolveColumn retrieves a column by name and returns its elements.
	ResolveColumn(relationID catid.DescID, columnName tree.Name, p ResolveParams) ElementResultSet

	// ResolveConstraint retrieves a constraint by name and returns its elements.
	ResolveConstraint(relationID catid.DescID, constraintName tree.Name, p ResolveParams) ElementResultSet
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
	// ReferencedSequences returns all referenced sequence IDs
	ReferencedSequences() catalog.DescriptorIDSet
	// ReferencedTypes returns all referenced type IDs (not including implicit
	// table types)
	ReferencedTypes() catalog.DescriptorIDSet
	// ReferencedRelationIDs Returns all referenced relation IDs.
	ReferencedRelationIDs() catalog.DescriptorIDSet
}
