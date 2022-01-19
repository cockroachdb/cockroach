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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
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
	if string(n.Name) == b.SessionData().Database && b.SessionData().SafeUpdates {
		panic(pgerror.DangerousStatementf("DROP DATABASE on current database"))
	}
	dropIDs := catalog.DescriptorIDSet{}
	{
		c := b.WithNewSourceElementID()
		doSchema := func(schema catalog.SchemaDescriptor) {
			// Sanity: Check if the descriptor is already dropped by scanning the
			// elements. We cannot use the normal helper function, since the temporary
			// descriptors are session bound and have specific code paths for lookups.
			schemaDropped := false
			b.ForEachElementStatus(func(_, targetStatus scpb.Status, elem scpb.Element) {
				if schemaDropped {
					return
				}
				if targetStatus == scpb.Status_ABSENT && screl.GetDescID(elem) == schema.GetID() {
					switch elem.(type) {
					case *scpb.Schema:
						schemaDropped = true
					}
				}
			})
			if schemaDropped {
				return
			}
			// For public and temporary schemas the drop logic
			// will only drop the underlying objects and return
			// if that no drop schema node was added (nodeAdded).
			// The schemaDroppedIDs list will have the list of
			// dependent objects, which that database will add
			// direct dependencies on.
			nodeAdded, schemaDroppedIDs := dropSchema(c, db, schema, tree.DropCascade, true)
			// Block drops if cascade is not set.
			if n.DropBehavior == tree.DropRestrict && (nodeAdded || !schemaDroppedIDs.Empty()) {
				panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
					"database %q is not empty and RESTRICT was specified", db.GetName()))
			} else if b.SessionData().SafeUpdates &&
				n.DropBehavior == tree.DropDefault && (nodeAdded || !schemaDroppedIDs.Empty()) {
				panic(pgerror.DangerousStatementf(
					"DROP DATABASE on non-empty database without explicit CASCADE"))
			}
			// If no schema exists to depend on, then depend on dropped IDs
			if !nodeAdded {
				schemaDroppedIDs.ForEach(dropIDs.Add)
			}
		}
		// Get all schemas including temporary ones for a database.
		schemas := b.CatalogReader().ReadSchemaNamesAndIDs(b, db)
		sortedSchemaNames := make([]string, 0, len(schemas))
		for _, schemaName := range schemas {
			sortedSchemaNames = append(sortedSchemaNames, schemaName)
		}
		sort.Strings(sortedSchemaNames)
		for _, schemaName := range sortedSchemaNames {
			_, schema := c.CatalogReader().MayResolveSchema(
				b,
				tree.ObjectNamePrefix{
					SchemaName:      tree.Name(schemaName),
					CatalogName:     tree.Name(db.GetName()),
					ExplicitSchema:  true,
					ExplicitCatalog: true})

			if schema.Dropped() {
				continue
			}
			doSchema(schema)
		}
	}
	b.EnqueueDrop(&scpb.Database{
		DatabaseID:       db.GetID(),
		DependentObjects: dropIDs.Ordered(),
	})
	b.EnqueueDrop(&scpb.DatabaseComment{
		DatabaseID: db.GetID(),
		Comment:    scpb.PlaceHolderComment,
	})
	// TODO(fqazi): Handle removing database base role settings during DROP
	// commands.
}
