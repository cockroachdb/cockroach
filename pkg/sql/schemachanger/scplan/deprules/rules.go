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

	public, txnDropped, dropped, absent, deleteOnly, deleteAndWriteOnly = scpb.Status_PUBLIC,
		scpb.Status_TXN_DROPPED, scpb.Status_DROPPED, scpb.Status_ABSENT, scpb.Status_DELETE_ONLY,
		scpb.Status_DELETE_AND_WRITE_ONLY
	// Make the linter happy
	_ = txnDropped
	_ = deleteOnly
)

func init() {
	var (
		parent, parentTarget, parentNode = targetNodeVars("parent")
		other, otherTarget, otherNode    = targetNodeVars("other")
	)
	// Ensures that child objects under a database or schema
	// are dropped before any children are dealt with.
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
			parentNode.AttrIn(screl.Status, absent),

			joinTargetNode(other, otherTarget, otherNode, drop, absent),
		),
	)
}

func init() {
	from, fromTarget, fromNode := targetNodeVars("from")
	to, toTarget, toNode := targetNodeVars("to")
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

func init() {
	depNeedsRelationToExitSynthDrop := func(ruleName string, depTypes []interface{}, depDescIDMatch rel.Attr) {
		// Before any parts of a relation/type can be dropped, the relation
		// should exit the synthetic drop state.
		relation, relationTarget, relationNode := targetNodeVars("relation")
		dep, depTarget, depNode := targetNodeVars("dep")
		var id rel.Var = "id"
		register(
			"dependency needs relation/type as non-synthetically dropped",
			depNode, relationNode,
			screl.MustQuery(

				relation.Type((*scpb.Table)(nil), (*scpb.View)(nil), (*scpb.Sequence)(nil), (*scpb.Type)(nil)),
				dep.Type(depTypes[0], depTypes[1:]...),

				relation.AttrEqVar(screl.DescID, id),
				dep.AttrEqVar(depDescIDMatch, id),
				joinTargetNode(relation, relationTarget, relationNode, drop, dropped),
				joinTargetNode(dep, depTarget, depNode, drop, absent),
			),
		)
	}
	depNeedsRelationToExitSynthDrop("dependency needs relation/type as non-synthetically dropped",
		[]interface{}{(*scpb.DefaultExpression)(nil), (*scpb.RelationDependedOnBy)(nil),
			(*scpb.SequenceOwnedBy)(nil), (*scpb.OutboundForeignKey)(nil)},
		screl.DescID)

	depNeedsRelationToExitSynthDrop("dependency (ref desc) needs relation/type as non-synthetically dropped",
		[]interface{}{(*scpb.InboundForeignKey)(nil), (*scpb.TypeReference)(nil)},
		screl.ReferencedDescID)
}

func init() {
	relationNeedsDepToBeRemoved := func(ruleName string, depTypes []interface{}, depDescIDMatch rel.Attr) {
		// Before any parts of a relation can be dropped, the relation
		// should exit the synthetic drop state.
		relation, relationTarget, relationNode := targetNodeVars("relation")
		dep, depTarget, depNode := targetNodeVars("dep")
		var id rel.Var = "id"
		register(
			"relation/type needs dependency as dropped",
			relationNode, depNode,
			screl.MustQuery(

				relation.Type((*scpb.Table)(nil), (*scpb.View)(nil), (*scpb.Sequence)(nil), (*scpb.Type)(nil)),
				dep.Type(depTypes[0], depTypes[1:]...),

				id.Entities(screl.DescID, relation, dep),

				joinTargetNode(relation, relationTarget, relationNode, drop, absent),
				joinTargetNode(dep, depTarget, depNode, drop, absent),
			),
		)
	}
	relationNeedsDepToBeRemoved("relation/type needs dependency as dropped",
		[]interface{}{(*scpb.DefaultExpression)(nil), (*scpb.RelationDependedOnBy)(nil),
			(*scpb.SequenceOwnedBy)(nil), (*scpb.OutboundForeignKey)(nil)},
		screl.DescID)

	relationNeedsDepToBeRemoved("relation/type (ref desc) needs dependency as dropped",
		[]interface{}{(*scpb.InboundForeignKey)(nil), (*scpb.TypeReference)(nil)},
		screl.ReferencedDescID)
}
