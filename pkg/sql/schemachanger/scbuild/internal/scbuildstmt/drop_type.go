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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// DropType implements DROP TYPE.
func DropType(b BuildCtx, n *tree.DropType) {
	if n.DropBehavior == tree.DropCascade {
		panic(scerrors.NotImplementedErrorf(n, "DROP TYPE CASCADE is not yet supported"))
	}
	var toCheckBackrefs []catid.DescID
	arrayTypesToAlsoCheck := make(map[catid.DescID]catid.DescID)
	for _, name := range n.Names {
		elts := b.ResolveEnumType(name, ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		_, _, typ := scpb.FindEnumType(elts)
		if typ == nil {
			continue
		}
		prefix := b.NamePrefix(typ)
		// Mutate the AST to have the fully resolved name from above, which will be
		// used for both event logging and errors.
		tn := tree.MakeTypeNameWithPrefix(prefix, name.Object())
		b.SetUnresolvedNameAnnotation(name, &tn)
		// Drop the type.
		if n.DropBehavior == tree.DropCascade {
			dropCascadeDescriptor(b, typ.TypeID)
		} else {
			if dropRestrictDescriptor(b, typ.TypeID) {
				toCheckBackrefs = append(toCheckBackrefs, typ.TypeID)
			}
			b.IncrementSubWorkID()
			if dropRestrictDescriptor(b.WithNewSourceElementID(), typ.ArrayTypeID) {
				arrayTypesToAlsoCheck[typ.TypeID] = typ.ArrayTypeID
			}
		}
		b.IncrementSubWorkID()
		b.IncrementEnumCounter(sqltelemetry.EnumDrop)
	}
	// Check if there are any back-references which would prevent a DROP RESTRICT.
	for _, typeID := range toCheckBackrefs {
		dependentNames := dependentTypeNames(b, typeID)
		if arrayTypeID, found := arrayTypesToAlsoCheck[typeID]; len(dependentNames) == 0 && found {
			dependentNames = dependentTypeNames(b, arrayTypeID)
		}
		if len(dependentNames) > 0 {
			panic(pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop type %q because other objects (%v) still depend on it",
				simpleName(b, typeID), dependentNames,
			))
		}
	}
}

func dependentTypeNames(b BuildCtx, typeID catid.DescID) (dependentNames []string) {
	backrefs := undroppedBackrefs(b, typeID)
	if backrefs.IsEmpty() {
		return nil
	}
	descIDs(backrefs).ForEach(func(depID descpb.ID) {
		dependentNames = append(dependentNames, qualifiedName(b, depID))
	})
	return dependentNames
}
