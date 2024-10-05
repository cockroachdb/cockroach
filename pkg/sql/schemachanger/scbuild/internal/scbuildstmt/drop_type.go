// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		elts := b.ResolveUserDefinedTypeType(name, ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		var typ scpb.Element
		var typeID, arrayTypeID catid.DescID
		if _, _, enum := scpb.FindEnumType(elts); enum != nil {
			b.IncrementEnumCounter(sqltelemetry.EnumDrop)
			typeID, arrayTypeID = enum.TypeID, enum.ArrayTypeID
			typ = enum
		} else if _, _, composite := scpb.FindCompositeType(elts); composite != nil {
			typeID, arrayTypeID = composite.TypeID, composite.ArrayTypeID
			typ = composite
		} else {
			continue
		}
		prefix := b.NamePrefix(typ)
		// Mutate the AST to have the fully resolved name from above, which will be
		// used for both event logging and errors.
		tn := tree.MakeTypeNameWithPrefix(prefix, name.Object())
		b.SetUnresolvedNameAnnotation(name, &tn)
		// Drop the type.
		if n.DropBehavior == tree.DropCascade {
			dropCascadeDescriptor(b, typeID)
		} else {
			if dropRestrictDescriptor(b, typeID) {
				toCheckBackrefs = append(toCheckBackrefs, typeID)
			}
			b.IncrementSubWorkID()
			if dropRestrictDescriptor(b.WithNewSourceElementID(), arrayTypeID) {
				arrayTypesToAlsoCheck[typeID] = arrayTypeID
			}
		}
		b.LogEventForExistingTarget(typ)
		b.IncrementSubWorkID()
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
