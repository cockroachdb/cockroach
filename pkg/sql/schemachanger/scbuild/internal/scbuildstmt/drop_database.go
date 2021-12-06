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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DropDatabase implements DROP DATABASE.
func DropDatabase(b BuildCtx, n *tree.DropDatabase) {
	db := b.ResolveDatabase(n.Name, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.DROP,
	})
	if db == nil {
		return
	}

	dropIDs := catalog.DescriptorIDSet{}
	{
		c := b.WithNewSourceElementID()
		doSchema := func(schema catalog.SchemaDescriptor) {
			// Sanity: Check if the descriptor is already dropped.
			if checkIfDescOrElementAreDropped(b, schema.GetID()) {
				return
			}
			// For public and temporary schemas the drop logic
			// will only drop the underlying objects and return
			// if that no drop schema node was added (nodeAdded).
			// The schemaDroppedIDs list will have the list of
			// dependent objects, which that database will add
			// direct dependencies on.
			nodeAdded, schemaDroppedIDs := dropSchema(c, db, schema, tree.DropCascade)
			// Block drops if cascade is not set.
			if n.DropBehavior == tree.DropRestrict && (nodeAdded || !schemaDroppedIDs.Empty()) {
				panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
					"database %q is not empty and RESTRICT was specified", db.GetName()))
			}
			// If no schema exists to depend on, then depend on dropped IDs
			if !nodeAdded {
				schemaDroppedIDs.ForEach(dropIDs.Add)
			}
		}
		var schemaIDs catalog.DescriptorIDSet
		schemaIDs.Add(db.GetSchemaID(tree.PublicSchema))
		_ = db.ForEachSchemaInfo(func(id descpb.ID, _ string, isDropped bool) error {
			if !isDropped {
				schemaIDs.Add(id)
			}
			return nil
		})
		for _, schemaID := range schemaIDs.Ordered() {
			schema := c.MustReadSchema(schemaID)
			if schema.Dropped() {
				continue
			}
			dropIDs.Add(schemaID)
			doSchema(schema)
		}
	}
	b.EnqueueDrop(&scpb.Database{
		DatabaseID:       db.GetID(),
		DependentObjects: dropIDs.Ordered(),
	})
}
