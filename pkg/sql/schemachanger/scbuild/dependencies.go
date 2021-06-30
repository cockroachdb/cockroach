// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// Dependencies contains all the dependencies required by the builder.
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

func mustReadDatabase(
	ctx context.Context, d Dependencies, id descpb.ID,
) catalog.DatabaseDescriptor {
	desc := d.CatalogReader().MustReadDescriptor(ctx, id)
	db, err := catalog.AsDatabaseDescriptor(desc)
	onErrPanic(err)
	return db
}

func mustReadSchema(ctx context.Context, d Dependencies, id descpb.ID) catalog.SchemaDescriptor {
	desc := d.CatalogReader().MustReadDescriptor(ctx, id)
	schema, err := catalog.AsSchemaDescriptor(desc)
	onErrPanic(err)
	return schema
}

func mustReadTable(ctx context.Context, d Dependencies, id descpb.ID) catalog.TableDescriptor {
	desc := d.CatalogReader().MustReadDescriptor(ctx, id)
	table, err := catalog.AsTableDescriptor(desc)
	onErrPanic(err)
	return table
}

func mustReadType(ctx context.Context, d Dependencies, id descpb.ID) catalog.TypeDescriptor {
	desc := d.CatalogReader().MustReadDescriptor(ctx, id)
	typ, err := catalog.AsTypeDescriptor(desc)
	onErrPanic(err)
	return typ
}

func semaCtx(d Dependencies) *tree.SemaContext {
	semaCtx := tree.MakeSemaContext()
	semaCtx.Annotations = nil
	semaCtx.SearchPath = d.SessionData().SearchPath
	semaCtx.IntervalStyleEnabled = d.SessionData().IntervalStyleEnabled
	semaCtx.DateStyleEnabled = d.SessionData().DateStyleEnabled
	semaCtx.TypeResolver = d.CatalogReader()
	semaCtx.TableNameResolver = d.CatalogReader()
	semaCtx.DateStyle = d.SessionData().GetDateStyle()
	semaCtx.IntervalStyle = d.SessionData().GetIntervalStyle()
	return &semaCtx
}

func evalCtx(ctx context.Context, d Dependencies) *tree.EvalContext {
	return &tree.EvalContext{
		SessionDataStack:   sessiondata.NewStack(d.SessionData()),
		Context:            ctx,
		Planner:            &faketreeeval.DummyEvalPlanner{},
		PrivilegedAccessor: &faketreeeval.DummyPrivilegedAccessor{},
		SessionAccessor:    &faketreeeval.DummySessionAccessor{},
		ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
		Sequence:           &faketreeeval.DummySequenceOperators{},
		Tenant:             &faketreeeval.DummyTenantOperator{},
		Regions:            &faketreeeval.DummyRegionOperator{},
		Settings:           d.ClusterSettings(),
		Codec:              d.Codec(),
	}
}
