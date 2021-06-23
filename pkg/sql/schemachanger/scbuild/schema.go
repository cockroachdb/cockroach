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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (b *buildContext) dropSchemaDesc(
	ctx context.Context, sc catalog.SchemaDescriptor, db *dbdesc.Mutable, behavior tree.DropBehavior,
) (nodeAdded bool, dropIDs []descpb.ID) {
	nodeAdded = false
	// For non-user defined schemas, another check will be
	// done each object as we go to drop them.
	if sc.SchemaKind() == catalog.SchemaUserDefined {
		isAdmin, err := b.AuthAccessor.HasAdminRole(ctx)
		if err != nil {
			panic(err)
		}
		hasOwnership, err := b.AuthAccessor.HasOwnership(ctx, sc)
		if err != nil {
			panic(err)
		}
		if !(isAdmin || hasOwnership) {
			panic(pgerror.Newf(pgcode.InsufficientPrivilege, "permission denied to drop schema %q", sc.GetName()))
		}
	}
	_, dropIDs, err := resolver.GetObjectNamesAndIDs(ctx, b.EvalCtx.Txn, b.Res, b.EvalCtx.Codec, db, sc.GetName(), true /* explicitPrefix */)
	if err != nil {
		panic(err)
	}
	if behavior != tree.DropCascade && len(dropIDs) > 0 {
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"schema %q is not empty and CASCADE was not specified", sc.GetName()))
	}
	// Sort the dropped IDs to ensure a consistent order of
	// operations.
	sort.SliceStable(dropIDs, func(i, j int) bool {
		return dropIDs[i] < dropIDs[j]
	})
	schemaNode := scpb.Schema{
		SchemaID:         sc.GetID(),
		DependentObjects: dropIDs,
	}
	for _, descID := range dropIDs {
		tableDesc, err := b.Descs.GetMutableTableByID(ctx, b.EvalCtx.Txn, descID, tree.ObjectLookupFlagsWithRequired())
		if err == nil {
			if tableDesc.IsView() {
				b.maybeDropViewAndDependents(ctx, tableDesc, behavior)
			} else if tableDesc.IsSequence() {
				b.dropSequenceDesc(ctx, tableDesc, behavior)
			} else if tableDesc.IsTable() {
				b.dropTableDesc(ctx, tableDesc, behavior)
			}
		} else if pgerror.GetPGCode(err) == pgcode.UndefinedTable {
			typeDesc, err := b.Descs.GetMutableTypeByID(ctx, b.EvalCtx.Txn, descID, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				panic(err)
			}
			b.dropTypeDesc(ctx, typeDesc, behavior, true /* ignoreAlises*/)
		} else {
			panic(err)
		}
	}

	if sc.SchemaKind() != catalog.SchemaPublic &&
		sc.SchemaKind() != catalog.SchemaVirtual &&
		sc.SchemaKind() != catalog.SchemaTemporary {
		// Add a schema node with all the dependencies.
		b.addNode(scpb.Target_DROP, &schemaNode)
		nodeAdded = true
	}
	return nodeAdded, dropIDs
}
func (b *buildContext) dropSchema(ctx context.Context, n *tree.DropSchema) {
	for _, name := range n.Names {
		dbName := b.Res.CurrentDatabase()
		if name.ExplicitCatalog {
			dbName = name.Catalog()
		}
		scName := name.Schema()
		db, err := b.Descs.GetMutableDatabaseByName(ctx, b.EvalCtx.Txn, dbName,
			tree.DatabaseLookupFlags{Required: true})
		if err != nil {
			panic(err)
		}

		sc, err := b.Descs.GetSchemaByName(ctx, b.EvalCtx.Txn, db, scName, tree.SchemaLookupFlags{
			Required: true,
		})
		if err != nil {
			panic(err)
		}
		if sc == nil {
			if n.IfExists {
				continue
			}
			panic(pgerror.Newf(pgcode.InvalidSchemaName, "unknown schema %q", scName))
		}

		switch sc.SchemaKind() {
		case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
			panic(pgerror.Newf(pgcode.InvalidSchemaName, "cannot drop schema %q", sc.GetName()))
		case catalog.SchemaUserDefined:
			b.dropSchemaDesc(ctx, sc, db, n.DropBehavior)
		default:
			panic(errors.AssertionFailedf("unknown schema kind %d", sc.SchemaKind()))
		}
	}
}
