// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildimpl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/scbuildctx"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// DropSchema implements DROP SCHEMA.
func DropSchema(ctx context.Context, b scbuildctx.BuildCtx, n *tree.DropSchema) {
	for _, name := range n.Names {
		db, sc := b.ResolveSchema(ctx, name, scbuildctx.ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		if sc == nil {
			continue
		}
		dropSchema(ctx, b, db, sc, n.DropBehavior)
		b.IncrementSubWorkID()
	}
}

func dropSchema(
	ctx context.Context,
	b scbuildctx.BuildCtx,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	behavior tree.DropBehavior,
) (nodeAdded bool, dropIDs catalog.DescriptorIDSet) {
	// For non-user defined schemas, another check will be
	// done each object as we go to drop them.
	if sc.SchemaKind() == catalog.SchemaUserDefined {
		b.MustOwn(ctx, sc)
	}
	_, objectIDs := b.CatalogReader().ReadObjectNamesAndIDs(ctx, db, sc)
	for _, id := range objectIDs {
		dropIDs.Add(id)
	}
	if behavior != tree.DropCascade && !dropIDs.Empty() {
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"schema %q is not empty and CASCADE was not specified", sc.GetName()))
	}
	{
		c := b.BuildCtxWithNewSourceElementID()
		for _, id := range dropIDs.Ordered() {
			desc := c.CatalogReader().MustReadDescriptor(ctx, id)
			switch t := desc.(type) {
			case catalog.TableDescriptor:
				if t.IsView() {
					dropView(ctx, c, t, behavior)
				} else if t.IsSequence() {
					dropSequence(ctx, c, t, behavior)
				} else if t.IsTable() {
					dropTable(ctx, c, t, behavior)
				} else {
					panic(errors.AssertionFailedf("table descriptor %q (%d) is neither table, sequence or view",
						t.GetName(), t.GetID()))
				}
			case catalog.TypeDescriptor:
				dropType(ctx, c, t, behavior)
			default:
				panic(errors.AssertionFailedf("expected table or type descriptor, instead %q (%d) is %q",
					t.GetName(), t.GetID(), t.DescriptorType()))
			}
		}
	}
	switch sc.SchemaKind() {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		return false, dropIDs
	case catalog.SchemaUserDefined:
		b.EnqueueDrop(&scpb.Schema{
			SchemaID:         sc.GetID(),
			DependentObjects: dropIDs.Ordered(),
		})
		return true, dropIDs
	}
	panic(errors.AssertionFailedf("unexpected sc kind %q for sc %q (%d)",
		sc.SchemaKind(), sc.GetName(), sc.GetID()))
}
