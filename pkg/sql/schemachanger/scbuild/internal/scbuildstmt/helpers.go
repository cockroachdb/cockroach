// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

func newExpression(b NameResolver, expr tree.Expr) *scpb.Expression {
	if expr == nil {
		return nil
	}
	// Collect type IDs.
	var typeIDs catalog.DescriptorIDSet
	visitor := &tree.TypeCollectorVisitor{OIDs: make(map[oid.Oid]struct{})}
	tree.WalkExpr(visitor, expr)
	for oid := range visitor.OIDs {
		if !types.IsOIDUserDefinedType(oid) {
			continue
		}
		id, err := typedesc.UserDefinedTypeOIDToID(oid)
		if err != nil {
			panic(err)
		}
		typeIDs.Add(id)
	}
	// Collect sequence IDs.
	var seqIDs catalog.DescriptorIDSet
	seqIdentifiers, err := seqexpr.GetUsedSequences(expr)
	if err != nil {
		panic(err)
	}
	seqNameToID := make(map[string]int64)
	for _, seqIdentifier := range seqIdentifiers {
		if seqIdentifier.IsByID() {
			seqIDs.Add(catid.DescID(seqIdentifier.SeqID))
			continue
		}
		uqName, err := parser.ParseTableName(seqIdentifier.SeqName)
		if err != nil {
			panic(err)
		}
		elts := b.ResolveSequence(uqName, ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.SELECT,
		})
		_, _, seq := scpb.FindSequence(elts)
		seqNameToID[seqIdentifier.SeqName] = int64(seq.SequenceID)
		seqIDs.Add(seq.SequenceID)
	}
	if len(seqNameToID) > 0 {
		expr, err = seqexpr.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
		if err != nil {
			panic(err)
		}
	}
	return &scpb.Expression{
		Expr:            catpb.Expression(tree.Serialize(expr)),
		UsesTypeIDs:     typeIDs.Ordered(),
		UsesSequenceIDs: seqIDs.Ordered(),
	}
}

func qualifiedName(b BuildCtx, id catid.DescID) string {
	_, _, ns := scpb.FindNamespace(b.QueryByID(id))
	_, _, sc := scpb.FindNamespace(b.QueryByID(ns.SchemaID))
	_, _, db := scpb.FindNamespace(b.QueryByID(ns.DatabaseID))
	if db == nil {
		return ns.Name
	}
	if sc == nil {
		return db.Name + "." + ns.Name
	}
	return db.Name + "." + sc.Name + "." + ns.Name
}

func simpleName(b BuildCtx, id catid.DescID) string {
	_, _, ns := scpb.FindNamespace(b.QueryByID(id))
	return ns.Name
}

func dropCascadeDescriptor(b BuildCtx, id catid.DescID) {
	elts := b.QueryByID(id)
	next := b.WithNewSourceElementID()
	// Skip synthetic descriptors.
	var isSkipped bool
	if _, _, sc := scpb.FindSchema(elts); sc != nil {
		isSkipped = sc.IsVirtual || sc.IsPublic || sc.IsTemporary
	} else if _, _, tbl := scpb.FindTable(elts); tbl != nil {
		isSkipped = tbl.IsTemporary
	} else if _, _, view := scpb.FindView(elts); view != nil {
		isSkipped = view.IsTemporary
	} else if _, _, seq := scpb.FindSequence(elts); seq != nil {
		isSkipped = seq.IsTemporary
	}
	if !isSkipped {
		// Mark element targets as ABSENT.
		var hasChanged bool
		elts.ForEachElementStatus(func(status, targetStatus scpb.Status, e scpb.Element) {
			if targetStatus == scpb.Status_ABSENT {
				return
			}
			b.CheckPrivilege(e, privilege.DROP)
			b.Drop(e)
			switch t := e.(type) {
			case *scpb.EnumType:
				dropCascadeDescriptor(next, t.ArrayTypeID)
			}
			hasChanged = true
		})
		// Exit early if all elements already had ABSENT targets.
		if !hasChanged {
			return
		}
	}
	// Recurse on back-referenced elements.
	undroppedBackrefs(b, id).ForEachElementStatus(func(status, _ scpb.Status, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.SchemaParent:
			dropCascadeDescriptor(next, t.SchemaID)
		case *scpb.ObjectParent:
			dropCascadeDescriptor(next, t.ObjectID)
		case *scpb.View:
			dropCascadeDescriptor(next, t.ViewID)
		case *scpb.AliasType:
			dropCascadeDescriptor(next, t.TypeID)
		case *scpb.EnumType:
			dropCascadeDescriptor(next, t.TypeID)
		case *scpb.Column:
			dropColumn(next, t)
		case
			*scpb.ColumnDefaultExpression,
			*scpb.ColumnOnUpdateExpression,
			*scpb.CheckConstraint,
			*scpb.ForeignKeyConstraint,
			*scpb.SequenceOwner,
			*scpb.DatabaseRegionConfig:
			b.CheckPrivilege(e, privilege.DROP)
			b.Drop(e)
		}
	})
}

func dropColumn(b BuildCtx, column *scpb.Column) {
	b.QueryByID(column.TableID).ForEachElementStatus(func(_, targetStatus scpb.Status, e scpb.Element) {
		if targetStatus == scpb.Status_ABSENT {
			return
		}
		if i, _ := screl.Schema.GetAttribute(screl.ColumnID, e); i == nil || i.(catid.ColumnID) != column.ColumnID {
			return
		}
		b.CheckPrivilege(e, privilege.DROP)
		b.Drop(e)
	})
}

func dropRestrict(b BuildCtx, id catid.DescID) (hasChanged bool) {
	elts := b.QueryByID(id)
	elts.ForEachElementStatus(func(status, targetStatus scpb.Status, e scpb.Element) {
		if targetStatus == scpb.Status_ABSENT {
			return
		}
		b.CheckPrivilege(e, privilege.DROP)
		b.Drop(e)
		hasChanged = true
	})
	return hasChanged
}

func undroppedBackrefs(b BuildCtx, id catid.DescID) ElementResultSet {
	return b.BackReferences(id).Filter(func(_, targetStatus scpb.Status, e scpb.Element) bool {
		return targetStatus != scpb.Status_ABSENT && screl.AllDescIDs(e).Contains(id)
	})
}

func descIDs(input ElementResultSet) (ids catalog.DescriptorIDSet) {
	input.ForEachElementStatus(func(_, _ scpb.Status, e scpb.Element) {
		ids.Add(screl.GetDescID(e))
	})
	return ids
}
