// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package deprules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

func targetNodeVars(el rel.Var) (element, target, node rel.Var) {
	return el, el + "-target", el + "-node"
}

func joinTargetNode(
	element, target, node rel.Var, direction scpb.Target_Direction, status scpb.Status,
) rel.Clause {
	return rel.And(
		screl.JoinTargetNode(element, target, node),
		target.AttrEq(screl.Direction, direction),
		node.AttrEq(screl.Status, status),
	)
}

const (
	add, drop = scpb.Target_ADD, scpb.Target_DROP

	public, absent, deleteOnly, deleteAndWriteOnly = scpb.Status_PUBLIC,
		scpb.Status_ABSENT, scpb.Status_DELETE_ONLY, scpb.Status_DELETE_AND_WRITE_ONLY
)

func init() {
	var (
		parent, parentTarget, parentNode = targetNodeVars("parent")
		other, otherTarget, otherNode    = targetNodeVars("other")
	)
	// TODO(ajwerner): These rules are suspect.
	register(
		"parent dependencies",
		parentNode, otherNode,
		screl.MustQuery(

			parent.Type((*scpb.Database)(nil), (*scpb.Schema)(nil)),
			other.Type(
				(*scpb.Type)(nil), (*scpb.Table)(nil), (*scpb.View)(nil), (*scpb.Sequence)(nil),
				// TODO(ajwerner): Should sequence be here if the parent is a database?
				// There was code in the old rules to have sequence but then it was
				// actually just duplicating the type case.
			),

			rel.Filter(
				"parentDependsOn",
				parent, other,
			)(func(parent, other scpb.Element) bool {
				var dependentObjects []descpb.ID
				switch parent := parent.(type) {
				case *scpb.Database:
					dependentObjects = parent.DependentObjects
				case *scpb.Schema:
					dependentObjects = parent.DependentObjects
				default:
					panic(errors.AssertionFailedf("invalid parent type %T", parent))
				}
				return idInIDs(dependentObjects, screl.GetDescID(other))
			}),

			screl.JoinTargetNode(parent, parentTarget, parentNode),
			parentTarget.AttrEq(screl.Direction, drop),
			parentNode.AttrIn(screl.Status, deleteOnly, deleteAndWriteOnly),

			joinTargetNode(other, otherTarget, otherNode, drop, absent),
		),
	)
}

func init() {
	ownedBy, ownedByTarget, ownedByNode := targetNodeVars("owned-by")
	seq, seqTarget, seqNode := targetNodeVars("seq")
	var id rel.Var = "id"
	register(
		"sequence owned by being dropped relies on sequence entering delete only",
		ownedByNode, seqNode,
		screl.MustQuery(
			ownedBy.Type((*scpb.SequenceOwnedBy)(nil)),
			seq.Type((*scpb.Sequence)(nil)),

			id.Entities(screl.DescID, ownedBy, seq),

			joinTargetNode(ownedBy, ownedByTarget, ownedByNode, drop, absent),
			joinTargetNode(seq, seqTarget, seqNode, drop, absent),
		))
}

func init() {
	ownedBy, ownedByTarget, ownedByNode := targetNodeVars("owned-by")
	id := rel.Var("id")
	// TODO(ajwerner): What is this about? It seems, if anything, just wrong.
	tab, tabTarget, tabNode := targetNodeVars("tab")
	register(
		"table drop in public depends on absent sequence owned by?",
		tabNode, ownedByNode,
		screl.MustQuery(
			tab.Type((*scpb.Table)(nil)),
			ownedBy.Type((*scpb.SequenceOwnedBy)(nil)),

			tab.AttrEqVar(screl.DescID, id),
			ownedBy.AttrEqVar(screl.ReferencedDescID, id),

			joinTargetNode(tab, tabTarget, tabNode, drop, public),
			joinTargetNode(ownedBy, ownedByTarget, ownedByNode, drop, absent),
		))
}

func init() {

	typ, typTarget, typNode := targetNodeVars("type")
	typRef, typRefTarget, typRefNode := targetNodeVars("type-ref")
	var id rel.Var = "id"
	register(
		"type reference something",
		typNode, typRefNode,
		screl.MustQuery(
			typ.Type((*scpb.Type)(nil)),
			typRef.Type((*scpb.TypeReference)(nil)),

			typ.AttrEqVar(screl.DescID, id),
			typRef.AttrEqVar(screl.ReferencedDescID, id),

			joinTargetNode(typ, typTarget, typNode, drop, public),
			joinTargetNode(typRef, typRefTarget, typRefNode, drop, absent),
		),
	)
}

func init() {
	from, fromTarget, fromNode := targetNodeVars("from")
	to, toTarget, toNode := targetNodeVars("to")
	var id rel.Var = "id"
	register(
		"view depends on view",
		fromNode, toNode,
		screl.MustQuery(
			from.Type((*scpb.View)(nil)),
			to.Type((*scpb.View)(nil)),
			rel.Filter(
				"depended-on-by",
				from, to,
			)(func(from, to *scpb.View) bool {
				return from != to && idInIDs(from.DependedOnBy, to.TableID)
			}),

			joinTargetNode(from, fromTarget, fromNode, drop, absent),
			joinTargetNode(to, toTarget, toNode, drop, absent),
		),
	)

	// TODO(ajwerner): What does this even mean? The table starts in
	// public.
	register(
		"table drop depended on by on view",
		fromNode, toNode,
		screl.MustQuery(
			from.Type((*scpb.Table)(nil)),
			to.Type((*scpb.View)(nil)),
			rel.Filter(
				"viewDependsOnTable",
				to, from,
			)(func(to *scpb.View, from *scpb.Table) bool {
				return idInIDs(to.DependsOn, from.TableID)
			}),

			joinTargetNode(from, fromTarget, fromNode, drop, public),
			joinTargetNode(to, toTarget, toNode, drop, absent),
		),
	)

	register(
		"view depends on type",
		fromNode, toNode,
		screl.MustQuery(

			from.Type((*scpb.View)(nil)),
			to.Type((*scpb.TypeReference)(nil)),
			id.Entities(screl.DescID, from, to),

			joinTargetNode(from, fromTarget, fromNode, drop, absent),
			joinTargetNode(to, toTarget, toNode, drop, absent),
		),
	)
}

func init() {

	column, columnTarget, columnNode := targetNodeVars("column")
	index, indexTarget, indexNode := targetNodeVars("index")
	var id, status, direction rel.Var = "id", "status", "direction"
	register(
		"column depends on indexes",
		columnNode, indexNode,
		screl.MustQuery(

			status.In(deleteAndWriteOnly, public),
			direction.Eq(add),

			column.Type((*scpb.Column)(nil)),
			index.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),

			id.Entities(screl.DescID, column, index),

			rel.Filter(
				"columnInIndex", column, index,
			)(func(from *scpb.Column, to scpb.Element) bool {
				var idx *descpb.IndexDescriptor
				switch to := to.(type) {
				case *scpb.PrimaryIndex:
					idx = &to.Index
				case *scpb.SecondaryIndex:
					idx = &to.Index
				default:
					panic(errors.AssertionFailedf("unexpected type %T", to))
				}
				return indexContainsColumn(idx, from.Column.ID)
			}),

			direction.Entities(screl.Direction, columnTarget, indexTarget),
			status.Entities(screl.Status, columnNode, indexNode),

			screl.JoinTargetNode(column, columnTarget, columnNode),
			screl.JoinTargetNode(index, indexTarget, indexNode),
		),
	)
}

func init() {

	addIdx, addTarget, addNode := targetNodeVars("add-idx")
	dropIdx, dropTarget, dropNode := targetNodeVars("drop-idx")
	var id rel.Var = "id"
	primaryIndexReferenceEachOther := screl.MustQuery(
		addIdx.Type((*scpb.PrimaryIndex)(nil)),
		dropIdx.Type((*scpb.PrimaryIndex)(nil)),
		id.Entities(screl.DescID, addIdx, dropIdx),

		rel.Filter(
			"referenceEachOther", addIdx, dropIdx,
		)(func(add, drop *scpb.PrimaryIndex) bool {
			return add.OtherPrimaryIndexID == drop.Index.ID
		}),

		joinTargetNode(addIdx, addTarget, addNode,
			add, public),
		joinTargetNode(dropIdx, dropTarget, dropNode,
			drop, deleteAndWriteOnly),
	)

	register(
		"primary index add depends on drop",
		addNode, dropNode,
		primaryIndexReferenceEachOther,
	)
	register(
		"primary index drop depends on add",
		dropNode, addNode,
		primaryIndexReferenceEachOther,
	)
}

// TODO(ajwerner): What does this even mean? The sequence starts in
// public.

func init() {
	seq, seqTarget, seqNode := targetNodeVars("seq")
	defExpr, defExprTarget, defExprNode := targetNodeVars("def-expr")
	register(
		"sequence default expr",
		seqNode, defExprNode,
		screl.MustQuery(
			seq.Type((*scpb.Sequence)(nil)),
			defExpr.Type((*scpb.DefaultExpression)(nil)),

			rel.Filter("defaultExpressionUsesSequence", seq, defExpr)(func(
				this *scpb.Sequence, that *scpb.DefaultExpression,
			) bool {
				return idInIDs(that.UsesSequenceIDs, this.SequenceID)
			}),

			joinTargetNode(seq, seqTarget, seqNode, drop, public),
			joinTargetNode(defExpr, defExprTarget, defExprNode, drop, absent),
		),
	)
}

func init() {

	// TODO(ajwerner): What does this even mean? The table starts in
	// public.
	tab, tabTarget, tabNode := targetNodeVars("tab")
	defExpr, defExprTarget, defExprNode := targetNodeVars("def-expr")
	id := rel.Var("id")
	register(
		"table default expr",
		tabNode, defExprNode,
		screl.MustQuery(

			tab.Type((*scpb.Table)(nil)),
			defExpr.Type((*scpb.DefaultExpression)(nil)),

			id.Entities(screl.DescID, tab, defExpr),

			joinTargetNode(tab, tabTarget, tabNode, drop, public),
			joinTargetNode(defExpr, defExprTarget, defExprNode, drop, absent),
		),
	)
}

func init() {

	// TODO(ajwerner): What does this even mean? The table starts in
	// public.
	tab, tabTarget, tabNode := targetNodeVars("tab")
	relDepOnBy, relDepOnByTarget, relDepOnByNode := targetNodeVars("rel-dep")
	tabID := rel.Var("dep-id")
	register(
		"table drop relation depended on",
		tabNode, relDepOnByNode,
		screl.MustQuery(

			tab.Type((*scpb.Table)(nil)),
			relDepOnBy.Type((*scpb.RelationDependedOnBy)(nil)),

			tab.AttrEqVar(screl.DescID, tabID),
			relDepOnBy.AttrEqVar(screl.ReferencedDescID, tabID),

			joinTargetNode(tab, tabTarget, tabNode, drop, public),
			joinTargetNode(relDepOnBy, relDepOnByTarget, relDepOnByNode, drop, absent),
		),
	)
}

func init() {

	// These are more weird rules where the public drop table
	// depends on something else.
	tab, tabTarget, tabNode := targetNodeVars("tab")
	fkBy, fkByTarget, fkByNode := targetNodeVars("fk")
	tabID := rel.Var("dep-id")
	register(
		"table drop depends on outbound fk",
		tabNode, fkByNode,
		screl.MustQuery(

			tab.Type((*scpb.Table)(nil)),
			fkBy.Type((*scpb.OutboundForeignKey)(nil)),

			tab.AttrEqVar(screl.DescID, tabID),
			fkBy.AttrEqVar(screl.DescID, tabID),

			joinTargetNode(tab, tabTarget, tabNode, drop, public),
			joinTargetNode(fkBy, fkByTarget, fkByNode, drop, absent),
		),
	)
}
