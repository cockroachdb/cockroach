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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// DropView implements DROP VIEW.
func DropView(b BuildCtx, n *tree.DropView) {
	var toCheckBackrefs []catid.DescID
	for i := range n.Names {
		name := &n.Names[i]
		elts := b.ResolveView(name.ToUnresolvedObjectName(), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		_, _, view := scpb.FindView(elts)
		if view == nil {
			b.MarkNameAsNonExistent(name)
			continue
		}
		// Mutate the AST to have the fully resolved name from above, which will be
		// used for both event logging and errors.
		name.ObjectNamePrefix = b.NamePrefix(view)
		// Check what we support dropping.
		if view.IsTemporary {
			panic(scerrors.NotImplementedErrorf(n, "dropping a temporary view"))
		}
		if view.IsMaterialized && !n.IsMaterialized {
			panic(errors.WithHint(pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", name.ObjectName),
				"use the corresponding MATERIALIZED VIEW command"))
		}
		if !view.IsMaterialized && n.IsMaterialized {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", name.ObjectName))
		}
		if n.DropBehavior == tree.DropCascade {
			dropCascadeDescriptor(b, view.ViewID)
		} else if dropRestrictDescriptor(b, view.ViewID) {
			toCheckBackrefs = append(toCheckBackrefs, view.ViewID)
		}
		b.IncrementSubWorkID()
		if view.IsMaterialized {
			b.IncrementSchemaChangeDropCounter("materialized_view")
		} else {
			b.IncrementSchemaChangeDropCounter("view")
		}
	}
	// Check if there are any back-references which would prevent a DROP RESTRICT.
	for _, viewID := range toCheckBackrefs {
		backrefs := undroppedBackrefs(b, viewID)
		if backrefs.IsEmpty() {
			continue
		}
		_, _, ns := scpb.FindNamespace(b.QueryByID(viewID))
		maybePanicOnDependentView(b, ns, backrefs)
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot drop view %s because other objects depend on it", ns.Name))
	}
}

func maybePanicOnDependentView(b BuildCtx, ns *scpb.Namespace, backrefs ElementResultSet) {
	_, _, depView := scpb.FindView(backrefs)
	if depView == nil {
		return
	}
	_, _, nsDep := scpb.FindNamespace(b.QueryByID(depView.ViewID))
	if nsDep.DatabaseID != ns.DatabaseID {
		panic(errors.WithHintf(sqlerrors.NewDependentObjectErrorf("cannot drop relation %q because view %q depends on it",
			ns.Name, qualifiedName(b, depView.ViewID)),
			"you can drop %s instead.", nsDep.Name))
	}
	panic(sqlerrors.NewDependentObjectErrorf("cannot drop relation %q because view %q depends on it",
		ns.Name, nsDep.Name))
}
