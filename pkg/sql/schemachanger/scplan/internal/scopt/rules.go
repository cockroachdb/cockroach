// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scopt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// When dropping objects, we need to mark the DROP op edges for their
// dependent elements as no-op.
func init() {
	// Dependent objects that will have edges marked as no-op.
	dep, depTarget, depNode := targetNodeVars("dep")
	// Object that is being dropped.
	object, objectTarget, objectNode := targetNodeVars("obj")
	var id rel.Var = "id"

	// Skip the column and index intermediate transitions, go straight from PUBLIC
	// to ABSENT when dropping a relation.
	registerNoOpEdges(
		depNode, // source node of op edge to mark as no-op.
		screl.MustQuery(
			object.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			dep.Type(
				(*scpb.Column)(nil),
				(*scpb.PrimaryIndex)(nil),
				(*scpb.SecondaryIndex)(nil),
			),
			id.Entities(screl.DescID, object, dep),

			screl.JoinTargetNode(object, objectTarget, objectNode),
			objectTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),

			screl.JoinTargetNode(dep, depTarget, depNode),
			depTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)

	// Skip ops on other skippable dependent elements when dropping a descriptor.
	registerNoOpEdges(
		depNode, // source node of op edge to mark as no-op.
		screl.MustQuery(
			object.Type(
				// Top-level elements which own a descriptor.
				(*scpb.Database)(nil),
				(*scpb.Schema)(nil),
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
				(*scpb.Sequence)(nil),
				(*scpb.AliasType)(nil),
				(*scpb.EnumType)(nil),
			),
			dep.Type(
				(*scpb.TableLocality)(nil),
				(*scpb.ColumnFamily)(nil),
				(*scpb.UniqueWithoutIndexConstraint)(nil),
				(*scpb.Owner)(nil),
				(*scpb.UserPrivileges)(nil),
				(*scpb.DatabaseRegionConfig)(nil),
			),
			id.Entities(screl.DescID, object, dep),

			screl.JoinTargetNode(object, objectTarget, objectNode),
			objectTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),

			screl.JoinTargetNode(dep, depTarget, depNode),
			depTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)
}

// When dropping a column we need to mark the DROP op edges for its column name
// element as no-op.
func init() {
	dep, depTarget, depNode := targetNodeVars("dep")
	col, colTarget, colNode := targetNodeVars("column")
	var descID rel.Var = "desc-id"
	var colID rel.Var = "column-id"
	registerNoOpEdges(
		depNode,
		screl.MustQuery(
			col.Type((*scpb.Column)(nil)),
			dep.Type((*scpb.ColumnName)(nil)),
			descID.Entities(screl.DescID, col, dep),
			colID.Entities(screl.ColumnID, col, dep),

			screl.JoinTargetNode(dep, depTarget, depNode),
			dep.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),

			screl.JoinTargetNode(col, colTarget, colNode),
			col.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)
}

// When dropping an index we need to mark the DROP op edges for its index name
// and partitioning element as no-op.
func init() {
	dep, depTarget, depNode := targetNodeVars("dep")
	idx, idxTarget, idxNode := targetNodeVars("index")
	var descID rel.Var = "desc-id"
	var idxID rel.Var = "index-id"
	registerNoOpEdges(
		depNode,
		screl.MustQuery(
			idx.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
			dep.Type((*scpb.IndexName)(nil), (*scpb.IndexPartitioning)(nil)),
			descID.Entities(screl.DescID, idx, dep),
			idxID.Entities(screl.IndexID, idx, dep),

			screl.JoinTargetNode(dep, depTarget, depNode),
			dep.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),

			screl.JoinTargetNode(idx, idxTarget, idxNode),
			idx.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)
}

// When dropping a constraint we need to mark the DROP op edges for its
// constraint name element as no-op.
func init() {
	dep, depTarget, depNode := targetNodeVars("dep")
	constraint, constraintTarget, constraintNode := targetNodeVars("constraint")
	var descID rel.Var = "desc-id"
	var constraintID rel.Var = "constraint-id"
	registerNoOpEdges(
		depNode,
		screl.MustQuery(
			constraint.Type(
				(*scpb.UniqueWithoutIndexConstraint)(nil),
				(*scpb.CheckConstraint)(nil),
				(*scpb.ForeignKeyConstraint)(nil),
			),
			dep.Type((*scpb.ConstraintName)(nil)),
			descID.Entities(screl.DescID, constraint, dep),
			constraintID.Entities(screl.ConstraintID, constraint, dep),

			screl.JoinTargetNode(dep, depTarget, depNode),
			depTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),

			screl.JoinTargetNode(constraint, constraintTarget, constraintNode),
			constraintTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)
}

// TODO(fqazi): For create operations we will need to have the ability
// to have transformations that will combine transitions into a single
// stage for execution. For example, a newly CREATE TABLE will be represented
// by a TABLE, COLUMN, and INDEX elements (among others), all of the operations
// for these elements are executable in a single stage. Having them execute
// across multiple stages would be problematic both from a validation and
// correctness viewpoint.
