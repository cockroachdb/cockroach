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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

func (b *buildContext) dropSchemaDesc(
	ctx context.Context,
	db catalog.DatabaseDescriptor,
	schema catalog.SchemaDescriptor,
	behavior tree.DropBehavior,
) (nodeAdded bool, dropIDs catalog.DescriptorIDSet) {
	// For non-user defined schemas, another check will be
	// done each object as we go to drop them.
	if schema.SchemaKind() == catalog.SchemaUserDefined {
		isAdmin, err := b.AuthorizationAccessor().HasAdminRole(ctx)
		onErrPanic(err)
		hasOwnership, err := b.AuthorizationAccessor().HasOwnership(ctx, schema)
		onErrPanic(err)
		if !(isAdmin || hasOwnership) {
			panic(pgerror.Newf(pgcode.InsufficientPrivilege, "permission denied to drop schema %q", schema.GetName()))
		}
	}
	_, objectIDs := b.CatalogReader().ReadObjectNamesAndIDs(ctx, db, schema)
	for _, id := range objectIDs {
		dropIDs.Add(id)
	}
	if behavior != tree.DropCascade && !dropIDs.Empty() {
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"schema %q is not empty and CASCADE was not specified", schema.GetName()))
	}
	lastSourceID := b.setSourceElementID(b.newSourceElementID())
	for _, id := range dropIDs.Ordered() {
		desc := b.CatalogReader().MustReadDescriptor(ctx, id)
		switch t := desc.(type) {
		case catalog.TableDescriptor:
			if t.IsView() {
				b.maybeDropViewAndDependents(ctx, t, behavior)
			} else if t.IsSequence() {
				b.dropSequenceDesc(ctx, t, behavior)
			} else if t.IsTable() {
				b.dropTableDesc(ctx, t, behavior)
			} else {
				panic(errors.AssertionFailedf("Table descriptor %q (%d) is neither table, sequence or view",
					t.GetName(), t.GetID()))
			}
		case catalog.TypeDescriptor:
			b.dropTypeDesc(ctx, t, behavior, true /* ignoreAliases */)
		default:
			panic(errors.AssertionFailedf("Expected table or type descriptor, instead %q (%d) is %q",
				t.GetName(), t.GetID(), t.DescriptorType()))
		}
	}
	b.setSourceElementID(lastSourceID)
	switch schema.SchemaKind() {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		return false, dropIDs
	case catalog.SchemaUserDefined:
		b.addNode(scpb.Target_DROP, &scpb.Schema{
			SchemaID:         schema.GetID(),
			DependentObjects: dropIDs.Ordered(),
		})
		return true, dropIDs
	}
	panic(errors.AssertionFailedf("Unexpected schema kind %q for schema %q (%d)",
		schema.SchemaKind(), schema.GetName(), schema.GetID()))
}

func (b *buildContext) dropSchema(ctx context.Context, n *tree.DropSchema) {
	for _, name := range n.Names {
		db, schema := b.CatalogReader().MayResolveSchema(ctx, name)
		if schema == nil {
			if n.IfExists {
				continue
			}
			panic(sqlerrors.NewUndefinedSchemaError(name.String()))
		}
		switch schema.SchemaKind() {
		case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
			panic(pgerror.Newf(pgcode.InsufficientPrivilege, "cannot drop schema %q", schema.GetName()))
		case catalog.SchemaUserDefined:
			// break
		default:
			panic(errors.AssertionFailedf("unknown schema kind %d", schema.SchemaKind()))
		}
		b.dropSchemaDesc(ctx, db, schema, n.DropBehavior)
		b.incrementSubWorkID()
	}
}
