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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (b *buildContext) dropDatabase(ctx context.Context, n *tree.DropDatabase) {
	// Check that the database exists.
	dbDesc, err := b.Descs.GetMutableDatabaseByName(ctx, b.EvalCtx.Txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: !n.IfExists})
	if err != nil {
		panic(err)
	}
	if dbDesc == nil {
		// IfExists was specified and database was not found.
		return
	}

	if err := b.AuthAccessor.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		panic(err)
	}
	schemas, err := b.Descs.GetSchemasForDatabase(ctx, b.EvalCtx.Txn, dbDesc.GetID())
	if err != nil {
		panic(err)
	}
	// Order schemas to guarantee the order in which operations
	// are generated for testing.
	schemaIDs := make([]descpb.ID, 0, len(schemas))
	for schemaID := range schemas {
		schemaIDs = append(schemaIDs, schemaID)
	}
	sort.SliceStable(schemaIDs, func(i, j int) bool {
		return schemaIDs[i] < schemaIDs[j]
	})
	dropIDs := catalog.DescriptorIDSet{}
	for _, schemaID := range schemaIDs {
		dropIDs.Add(schemaID)
		schemaDesc, err := b.Descs.GetImmutableSchemaByID(ctx, b.EvalCtx.Txn, schemaID, tree.SchemaLookupFlags{Required: true})
		if err != nil {
			panic(err)
		}
		// For public and temporary schemas the drop logic
		// will only drop the underlying objects and return
		// if that no drop schema node was added (nodeAdded).
		// The schemaDroppedIDs list will have the list of
		// dependent objects, which that database will add
		// direct dependencies on.
		nodeAdded, schemaDroppedIDs := b.dropSchemaDesc(ctx, schemaDesc, dbDesc, tree.DropCascade)
		// Block drops if cascade is not set.
		if n.DropBehavior != tree.DropCascade &&
			(nodeAdded || len(schemaDroppedIDs) > 0) {
			panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
				"database %q is not empty and CASCADE was not specified", dbDesc.GetName()))
		}
		// If no schema exists to depend on, then depend on dropped IDs
		if !nodeAdded {
			for _, id := range schemaDroppedIDs {
				dropIDs.Add(id)
			}
		}
	}
	b.addNode(scpb.Target_DROP,
		&scpb.Database{
			DatabaseID:       dbDesc.ID,
			DependentObjects: dropIDs.Ordered()})
}
