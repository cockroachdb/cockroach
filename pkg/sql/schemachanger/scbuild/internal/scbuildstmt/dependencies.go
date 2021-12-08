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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/redact"
)

// BuildCtx wraps BuilderState and exposes various convenience methods for the
// benefit of the scbuildstmts package.
//
// All methods except those in Context and Dependencies may panic instead of
// explicitly returning errors.
type BuildCtx interface {
	context.Context
	Dependencies
	BuilderState
	EventLogState

	TreeContextBuilder
	PrivilegeChecker
	DescriptorReader
	NameResolver
	NodeEnqueuerAndChecker
	TableElementIDGenerator

	// WithNewSourceElementID wraps BuilderStateWithNewSourceElementID in a
	// BuildCtx return type.
	WithNewSourceElementID() BuildCtx
}

// Dependencies contains all the external dependencies required by the scbuild
// package and its children.
type Dependencies interface {
	CatalogReader() CatalogReader
	AuthorizationAccessor() AuthorizationAccessor

	// Codec returns the current session data, as in execCfg.
	// So far this is used only to build a tree.EvalContext.
	Codec() keys.SQLCodec

	// SessionData returns the current session data, as in execCtx.
	SessionData() *sessiondata.SessionData

	// ClusterSettings returns the current cluster settings, as in execCfg.
	ClusterSettings() *cluster.Settings

	// Statements returns the statements behind this schema change.
	Statements() []string

	AstFormatter() AstFormatter
}

// CatalogReader should implement descriptor resolution, namespace lookups, and
// all such catalog read operations for the builder. The following contract must
// apply:
// - errors are panicked;
// - caches are avoided at all times, we read straight from storage;
// - MayResolve* methods return zero values if nothing could be found;
// - MayResolve* methods ignore dropped or offline descriptors;
// - MustReadDescriptor does not;
// - MustReadDescriptor panics if the descriptor was not found.
type CatalogReader interface {
	tree.TypeReferenceResolver
	tree.QualifiedNameResolver

	// MayResolveDatabase looks up a database by name.
	MayResolveDatabase(ctx context.Context, name tree.Name) catalog.DatabaseDescriptor

	// MayResolveSchema looks up a schema by name.
	MayResolveSchema(ctx context.Context, name tree.ObjectNamePrefix) (catalog.DatabaseDescriptor, catalog.SchemaDescriptor)

	// MayResolveTable looks up a table by name.
	MayResolveTable(ctx context.Context, name tree.UnresolvedObjectName) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// MayResolveType looks up a type by name.
	MayResolveType(ctx context.Context, name tree.UnresolvedObjectName) (catalog.ResolvedObjectPrefix, catalog.TypeDescriptor)

	// ReadObjectNamesAndIDs looks up the namespace entries for a schema.
	ReadObjectNamesAndIDs(ctx context.Context, db catalog.DatabaseDescriptor, schema catalog.SchemaDescriptor) (tree.TableNames, descpb.IDs)

	// MustReadDescriptor looks up a descriptor by ID.
	MustReadDescriptor(ctx context.Context, id descpb.ID) catalog.Descriptor
}

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {

	// CheckPrivilege verifies that the current user has `privilege` on
	// `descriptor`.
	CheckPrivilege(
		ctx context.Context, descriptor catalog.Descriptor, privilege privilege.Kind,
	) error

	// HasAdminRole verifies if a user has an admin role.
	HasAdminRole(ctx context.Context) (bool, error)

	// HasOwnership returns true iff the role, or any role the role is a member
	// of, has ownership privilege of the desc.
	HasOwnership(ctx context.Context, descriptor catalog.Descriptor) (bool, error)
}

// BuilderState encapsulates the state of the planned schema changes, hiding
// its internal state to anything that ends up using it and only allowing
// state changes via the provided methods.
type BuilderState interface {

	// AddNode adds a node into the NodeAccumulator.
	AddNode(status, targetStatus scpb.Status, elem scpb.Element, meta scpb.TargetMetadata)

	// ForEachNode iterates over the accumulated notes in the NodeAccumulator.
	ForEachNode(fn func(status, targetStatus scpb.Status, elem scpb.Element))
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

	FinalizeEventLogState(statement tree.Statement)
}

// TreeContextBuilder exposes convenient tree-package context builder methods.
type TreeContextBuilder interface {

	// SemaCtx returns a new tree.SemaContext.
	SemaCtx() *tree.SemaContext

	// EvalCtx returns a new tree.EvalContext.
	EvalCtx() *tree.EvalContext
}

// PrivilegeChecker exposes convenient privilege-checking methods.
type PrivilegeChecker interface {

	// MustOwn panics if the descriptor is not owned by the current user.
	MustOwn(desc catalog.Descriptor)
}

// DescriptorReader exposes convenient descriptor read methods.
type DescriptorReader interface {

	// MustReadDatabase returns the database descriptor for the given ID or panics.
	MustReadDatabase(id descpb.ID) catalog.DatabaseDescriptor

	// MustReadSchema returns the schema descriptor for the given ID or panics.
	MustReadSchema(id descpb.ID) catalog.SchemaDescriptor

	// MustReadTable returns the table descriptor for the given ID or panics.
	MustReadTable(id descpb.ID) catalog.TableDescriptor

	// MustReadType returns the type descriptor for the given ID or panics.
	MustReadType(id descpb.ID) catalog.TypeDescriptor
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
}

// NameResolver exposes convenient catalog name resolution methods.
type NameResolver interface {

	// ResolveDatabase retrieves a database descriptor by name.
	ResolveDatabase(name tree.Name, p ResolveParams) catalog.DatabaseDescriptor

	// ResolveSchema retrieves a schema descriptor by name, along with its
	// parent database descriptor.
	ResolveSchema(name tree.ObjectNamePrefix, p ResolveParams) (catalog.DatabaseDescriptor, catalog.SchemaDescriptor)

	// ResolveType retrieves a type descriptor by name.
	ResolveType(name *tree.UnresolvedObjectName, p ResolveParams) (catalog.ResolvedObjectPrefix, catalog.TypeDescriptor)

	// ResolveRelation retrieves a table descriptor by name.
	ResolveRelation(name *tree.UnresolvedObjectName, p ResolveParams) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// ResolveTable retrieves a table descriptor by name, checking that it is a
	// table and not a view or anything else.
	ResolveTable(name *tree.UnresolvedObjectName, p ResolveParams) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// ResolveSequence retrieves a table descriptor by name, checking that it is a
	// sequence.
	ResolveSequence(name *tree.UnresolvedObjectName, p ResolveParams) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// ResolveView retrieves a table descriptor by name, checking that it is a view.
	ResolveView(name *tree.UnresolvedObjectName, p ResolveParams) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// ResolveIndex retrieves an index by name.
	ResolveIndex(
		relationName *tree.UnresolvedObjectName,
		indexName tree.Name,
		p ResolveParams,
	) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor, catalog.Index)
}

// NodeEnqueuerAndChecker exposes convenient methods for enqueuing and checking
// nodes in the NodeAccumulator.
type NodeEnqueuerAndChecker interface {
	// EnqueueAdd adds a node with a PUBLIC target status.
	// Panics if the element is already present.
	EnqueueAdd(elem scpb.Element)

	// EnqueueDrop adds a node with an ABSENT target status.
	// Panics if the element is already present.
	EnqueueDrop(elem scpb.Element)

	// EnqueueDropIfNotExists is like EnqueueDrop but does nothing instead of
	// panicking if the element is already present.
	EnqueueDropIfNotExists(elem scpb.Element)

	// HasNode returns true iff the builder state has a node matching the provided
	// filter function.
	HasNode(filter func(status, targetStatus scpb.Status, elem scpb.Element) bool) bool

	// HasTarget returns true iff the builder state has a node with an equal element
	// and the same target status, regardless of node status.
	HasTarget(targetStatus scpb.Status, elem scpb.Element) bool

	// HasElement returns true iff the builder state has a node with an equal
	// element regardless of target status or node status.
	HasElement(elem scpb.Element) bool
}

// TableElementIDGenerator exposes convenient ID generation methods for table
// elements.
type TableElementIDGenerator interface {
	// NextColumnID returns the ID that should be used for any new column added to
	// this table descriptor.
	NextColumnID(tbl catalog.TableDescriptor) descpb.ColumnID

	// NextColumnFamilyID returns the ID that should be used for any new column
	// family added to this table descriptor.
	NextColumnFamilyID(tbl catalog.TableDescriptor) descpb.FamilyID

	// NextIndexID returns the ID that should be used for any new index added to
	// this table descriptor.
	NextIndexID(tbl catalog.TableDescriptor) descpb.IndexID
}

// AstFormatter provides interfaces for formatting AST nodes.
type AstFormatter interface {
	// FormatAstAsRedactableString formats a tree.Statement into SQL with fully
	// qualified names, where parts can be redacted.
	FormatAstAsRedactableString(statement tree.Statement) redact.RedactableString
}
