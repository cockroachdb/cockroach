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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// DropType implements DROP TYPE.
func DropType(b BuildCtx, n *tree.DropType) {
	if n.DropBehavior == tree.DropCascade {
		panic(unimplemented.NewWithIssue(51480, "DROP TYPE CASCADE is not yet supported"))
	}
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
			dropTypeRestrict(b, typ.TypeID)
			dropTypeRestrict(b.WithNewSourceElementID(), typ.ArrayTypeID)
		}
		b.IncrementSubWorkID()
	}
}

func dropTypeRestrict(b BuildCtx, id descpb.ID) {
	if !dropRestrict(b, id) {
		return
	}
	if _, _, enumType := scpb.FindEnumType(b.QueryByID(id)); enumType != nil && !enumType.IsMultiRegion {
		sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumDrop)
	}
	if backrefs := undroppedBackrefs(b, id); !backrefs.IsEmpty() {
		var dependentNames []string
		descIDs(backrefs).ForEach(func(depID descpb.ID) {
			dependentNames = append(dependentNames, qualifiedName(b, depID))
		})
		panic(pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"cannot drop type %q because other objects (%v) still depend on it.",
			simpleName(b, id), dependentNames,
		))
	}
}
