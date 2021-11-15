// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildctx

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// BuildCtx wraps BuilderState and exposes various convenience methods for the
// benefit of the scbuildimpl package.
//
// These convenience methods are grouped into interfaces declared in this
// package and which all depend exclusively on the embedded BuilderState
// interface.
type BuildCtx interface {
	BuilderState
	TreeContextBuilder
	PrivilegeChecker
	DescriptorReader
	NameResolver
	NodeEnqueuerAndChecker
	TableElementIDGenerator

	// BuildCtxWithNewSourceElementID is like
	// BuilderStateWithNewSourceElementID but with a BuildCtx return type.
	BuildCtxWithNewSourceElementID() BuildCtx
}

// TreeContextBuilder exposes convenient tree-package context builder methods.
type TreeContextBuilder interface {

	// SemaCtx returns a new tree.SemaContext.
	SemaCtx() *tree.SemaContext

	// EvalCtx returns a new tree.EvalContext.
	EvalCtx(ctx context.Context) *tree.EvalContext
}

// PrivilegeChecker exposes convenient privilege-checking methods.
type PrivilegeChecker interface {

	// MustOwn panics if the descriptor is not owned by the current user.
	MustOwn(ctx context.Context, desc catalog.Descriptor)
}

// DescriptorReader exposes convenient descriptor read methods.
type DescriptorReader interface {

	// MustReadDatabase returns the database descriptor for the given ID or panics.
	MustReadDatabase(ctx context.Context, id descpb.ID) catalog.DatabaseDescriptor

	// MustReadSchema returns the schema descriptor for the given ID or panics.
	MustReadSchema(ctx context.Context, id descpb.ID) catalog.SchemaDescriptor

	// MustReadTable returns the table descriptor for the given ID or panics.
	MustReadTable(ctx context.Context, id descpb.ID) catalog.TableDescriptor

	// MustReadType returns the type descriptor for the given ID or panics.
	MustReadType(ctx context.Context, id descpb.ID) catalog.TypeDescriptor
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
	ResolveDatabase(
		ctx context.Context, name tree.Name, p ResolveParams,
	) catalog.DatabaseDescriptor

	// ResolveSchema retrieves a schema descriptor by name, along with its
	// parent database descriptor.
	ResolveSchema(
		ctx context.Context, name tree.ObjectNamePrefix, p ResolveParams,
	) (catalog.DatabaseDescriptor, catalog.SchemaDescriptor)

	// ResolveType retrieves a type descriptor by name.
	ResolveType(
		ctx context.Context, name *tree.UnresolvedObjectName, p ResolveParams,
	) (catalog.ResolvedObjectPrefix, catalog.TypeDescriptor)

	// ResolveRelation retrieves a table descriptor by name.
	ResolveRelation(
		ctx context.Context, name *tree.UnresolvedObjectName, p ResolveParams,
	) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// ResolveTable retrieves a table descriptor by name, checking that it is a
	// table and not a view or anything else.
	ResolveTable(
		ctx context.Context, name *tree.UnresolvedObjectName, p ResolveParams,
	) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// ResolveSequence retrieves a table descriptor by name, checking that it is a
	// sequence.
	ResolveSequence(
		ctx context.Context, name *tree.UnresolvedObjectName, p ResolveParams,
	) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// ResolveView retrieves a table descriptor by name, checking that it is a view.
	ResolveView(
		ctx context.Context, name *tree.UnresolvedObjectName, p ResolveParams,
	) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor)

	// ResolveIndex retrieves an index by name.
	ResolveIndex(
		ctx context.Context,
		relationName *tree.UnresolvedObjectName,
		indexName tree.Name,
		p ResolveParams,
	) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor, catalog.Index)
}

// NodeEnqueuerAndChecker exposes convenient methods for enqueuing and checking
// nodes in the NodeAccumulator.
type NodeEnqueuerAndChecker interface {
	// EnqueueAdd adds a target node in the ADD target direction.
	// Panics if the element is already present.
	EnqueueAdd(elem scpb.Element)

	// EnqueueDrop adds a target node in the DROP direction.
	// Panics if the element is already present.
	EnqueueDrop(elem scpb.Element)

	// EnqueueDropIfNotExists is like EnqueueDrop but does nothing instead of
	// panicking if the element is already present.
	EnqueueDropIfNotExists(elem scpb.Element)

	// HasNode returns true iff the builder state has a node matching the provided
	// filter function.
	HasNode(filter func(status scpb.Status, dir scpb.Target_Direction, elem scpb.Element) bool) bool

	// HasTarget returns true iff the builder state has a node with an equal element
	// and the same target direction, regardless of node status.
	HasTarget(dir scpb.Target_Direction, elem scpb.Element) bool

	// HasElement returns true iff the builder state has a node with an equal
	// element regardless of target direction or node status.
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
