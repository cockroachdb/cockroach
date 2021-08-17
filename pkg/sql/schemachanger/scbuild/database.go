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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

func (b *buildContext) dropDatabase(ctx context.Context, n *tree.DropDatabase) {
	// Check that the database exists.
	db := b.CatalogReader().MayResolveDatabase(ctx, n.Name)
	if db == nil {
		if n.IfExists {
			return
		}
		panic(sqlerrors.NewUndefinedDatabaseError(n.Name.String()))
	}
	onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, db, privilege.DROP))

	dropIDs := catalog.DescriptorIDSet{}
	lastSourceID := b.setSourceElementID(b.newSourceElementID())
	doSchema := func(schema catalog.SchemaDescriptor) {
		// For public and temporary schemas the drop logic
		// will only drop the underlying objects and return
		// if that no drop schema node was added (nodeAdded).
		// The schemaDroppedIDs list will have the list of
		// dependent objects, which that database will add
		// direct dependencies on.
		nodeAdded, schemaDroppedIDs := b.dropSchemaDesc(ctx, db, schema, tree.DropCascade)
		// Block drops if cascade is not set.
		if n.DropBehavior != tree.DropCascade && (nodeAdded || !schemaDroppedIDs.Empty()) {
			panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
				"database %q has a non-empty schema %q and CASCADE was not specified", db.GetName(), schema.GetName()))
		}
		// If no schema exists to depend on, then depend on dropped IDs
		if !nodeAdded {
			schemaDroppedIDs.ForEach(dropIDs.Add)
		}
	}

	doSchema(schemadesc.GetPublicSchema())
	var schemaIDs catalog.DescriptorIDSet
	_ = db.ForEachSchemaInfo(func(id descpb.ID, _ string, isDropped bool) error {
		if !isDropped {
			schemaIDs.Add(id)
		}
		return nil
	})
	for _, schemaID := range schemaIDs.Ordered() {
		schema := mustReadSchema(ctx, b, schemaID)
		if schema.Dropped() {
			continue
		}
		dropIDs.Add(schemaID)
		doSchema(schema)
	}
	b.setSourceElementID(lastSourceID)
	b.addNode(scpb.Target_DROP,
		&scpb.Database{
			DatabaseID:       db.GetID(),
			DependentObjects: dropIDs.Ordered(),
		})
}
