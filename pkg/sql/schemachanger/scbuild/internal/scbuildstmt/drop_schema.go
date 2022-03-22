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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// DropSchema implements DROP SCHEMA.
func DropSchema(b BuildCtx, n *tree.DropSchema) {
	var toCheckBackrefs []catid.DescID
	for _, name := range n.Names {
		elts := b.ResolveSchema(name, ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		_, _, sc := scpb.FindSchema(elts)
		if sc == nil {
			continue
		}
		if sc.IsVirtual || sc.IsPublic {
			panic(pgerror.Newf(pgcode.InvalidSchemaName,
				"cannot drop schema %q", simpleName(b, sc.SchemaID)))
		}
		if sc.IsTemporary {
			panic(scerrors.NotImplementedErrorf(n, "dropping a temporary schema"))
		}
		if n.DropBehavior == tree.DropCascade {
			dropCascadeDescriptor(b, sc.SchemaID)
			toCheckBackrefs = append(toCheckBackrefs, sc.SchemaID)
		} else if dropRestrictDescriptor(b, sc.SchemaID) {
			toCheckBackrefs = append(toCheckBackrefs, sc.SchemaID)
		}
		b.IncrementSubWorkID()
		b.IncrementSchemaChangeDropCounter("schema")
		b.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaDrop)
	}
	// Check if there are any back-references which would prevent a DROP RESTRICT.
	for _, schemaID := range toCheckBackrefs {
		if n.DropBehavior == tree.DropCascade {
			// Special case to handle dropped types which aren't supported in CASCADE.
			var objectIDs, typeIDs catalog.DescriptorIDSet
			scpb.ForEachObjectParent(b.BackReferences(schemaID), func(_ scpb.Status, _ scpb.TargetStatus, op *scpb.ObjectParent) {
				objectIDs.Add(op.ObjectID)
			})
			objectIDs.ForEach(func(id descpb.ID) {
				elts := b.QueryByID(id)
				if _, _, enum := scpb.FindEnumType(elts); enum != nil {
					typeIDs.Add(enum.TypeID)
				} else if _, _, alias := scpb.FindAliasType(elts); alias != nil {
					typeIDs.Add(alias.TypeID)
				}
			})
			typeIDs.ForEach(func(id descpb.ID) {
				if dependentNames := dependentTypeNames(b, id); len(dependentNames) > 0 {
					panic(unimplemented.NewWithIssueDetailf(51480, "DROP TYPE CASCADE is not yet supported",
						"cannot drop type %q because other objects (%v) still depend on it",
						qualifiedName(b, id), dependentNames))
				}
			})
		} else {
			backrefs := undroppedBackrefs(b, schemaID)
			if backrefs.IsEmpty() {
				continue
			}
			panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
				"schema %q is not empty and CASCADE was not specified", simpleName(b, schemaID)))
		}
	}
}
