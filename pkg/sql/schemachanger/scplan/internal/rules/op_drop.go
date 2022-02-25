// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// When dropping a table or a view, skip all removal ops for column elements
// and column-dependent elements (like column names).
// Columns get implicitly removed once the descriptor is marked as dropped.
//
// The column transition to ABSENT needs to remain to clean up back-references
// in types referred to by the column in its type or its computed expression.
//
// We can't skip ops for column-dependent elements which reference other
// descriptors, like for example default expressions, again because of the need
// to clean up back-references.
//
// We also can't skip ops for column-dependent elements which don't like in the
// descriptor, like column comments (which live in a dedicated system table).
func init() {
	relation, relationTarget, _ := targetNodeVars("relation")
	column, columnTarget, columnNode := targetNodeVars("column")
	dep, depTarget, depNode := targetNodeVars("column-dep")
	relationID, columnID := rel.Var("relation-id"), rel.Var("column-id")

	registerOpRule(
		"skip column removal ops on relation drop",
		columnNode,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			column.Type(
				(*scpb.Column)(nil),
			),

			relationID.Entities(screl.DescID, relation, column),

			screl.JoinTarget(relation, relationTarget),
			relationTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTargetNode(column, columnTarget, columnNode),
			columnTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			columnNode.AttrIn(screl.CurrentStatus,
				// All but DELETE_ONLY which is the status leading to ABSENT.
				scpb.Status_PUBLIC,
				scpb.Status_WRITE_ONLY,
			),
		),
	)

	registerOpRule(
		"skip column dependents removal ops on relation drop",
		depNode,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			column.Type(
				(*scpb.Column)(nil),
			),
			dep.Type(
				(*scpb.ColumnName)(nil),
			),

			relationID.Entities(screl.DescID, relation, column, dep),
			columnID.Entities(screl.ColumnID, column, dep),

			screl.JoinTarget(relation, relationTarget),
			relationTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTarget(column, columnTarget),
			columnTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTargetNode(dep, depTarget, depNode),
			depTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)
}

// When dropping a table or a view, skip all removal ops for index elements
// as well as elements which depend on them, provided there are no
// back-references that need to be cleaned up. This is similar as for columns.
func init() {
	relation, relationTarget, _ := targetNodeVars("relation")
	index, indexTarget, indexNode := targetNodeVars("index")
	dep, depTarget, depNode := targetNodeVars("index-dep")
	relationID, indexID := rel.Var("relation-id"), rel.Var("index-id")

	registerOpRule(
		"skip index removal ops on relation drop",
		indexNode,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			index.Type(
				(*scpb.PrimaryIndex)(nil),
				(*scpb.SecondaryIndex)(nil),
			),

			relationID.Entities(screl.DescID, relation, index),

			screl.JoinTarget(relation, relationTarget),
			relationTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTargetNode(index, indexTarget, indexNode),
			indexTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)

	registerOpRule(
		"skip index dependents removal ops on relation drop",
		depNode,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			index.Type(
				(*scpb.PrimaryIndex)(nil),
				(*scpb.SecondaryIndex)(nil),
			),
			dep.Type(
				(*scpb.IndexName)(nil),
				(*scpb.IndexPartitioning)(nil),
			),

			relationID.Entities(screl.DescID, relation, index, dep),
			indexID.Entities(screl.IndexID, index, dep),

			screl.JoinTarget(relation, relationTarget),
			relationTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTarget(index, indexTarget),
			indexTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTargetNode(dep, depTarget, depNode),
			depTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)
}

// When dropping a table or a view, skip all removal ops for constraint elements
// as well as elements which depend on them, provided there are no
// back-references that need to be cleaned up. This is similar as for columns
// and indexes.
func init() {
	relation, relationTarget, _ := targetNodeVars("relation")
	constraint, constraintTarget, constraintNode := targetNodeVars("constraint")
	dep, depTarget, depNode := targetNodeVars("constraint-dep")
	relationID, constraintID := rel.Var("relation-id"), rel.Var("constraint-id")

	registerOpRule(
		"skip constraint removal ops on relation drop",
		constraintNode,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			constraint.Type(
				(*scpb.UniqueWithoutIndexConstraint)(nil),
			),

			relationID.Entities(screl.DescID, relation, constraint),

			screl.JoinTarget(relation, relationTarget),
			relationTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTargetNode(constraint, constraintTarget, constraintNode),
			constraintTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)

	registerOpRule(
		"skip constraint dependents removal ops on relation drop",
		depNode,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			constraint.Type(
				(*scpb.UniqueWithoutIndexConstraint)(nil),
				(*scpb.CheckConstraint)(nil),
				(*scpb.ForeignKeyConstraint)(nil),
			),
			dep.Type(
				(*scpb.ConstraintName)(nil),
			),

			relationID.Entities(screl.DescID, relation, constraint),
			constraintID.Entities(screl.ConstraintID, constraint, dep),

			screl.JoinTarget(relation, relationTarget),
			relationTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTarget(constraint, constraintTarget),
			constraintTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTargetNode(dep, depTarget, depNode),
			depTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
		),
	)
}

// Skip all removal ops on elements which depend on a descriptor element that
// is being removed as well. These elements get implicitly removed once the
// descriptor is marked as dropped.
// This rule excludes:
//   - index and column elements, which were handled above,
//   - elements which don't live inside the descriptor, like comments,
//   - elements which have forward references to other descriptors: back-
//     references need to be cleaned up.
func init() {
	desc, descTarget, _ := targetNodeVars("desc")
	dep, depTarget, depNode := targetNodeVars("dep")
	descID := rel.Var("desc-id")

	registerOpRule(
		"skip element removal ops on descriptor drop",
		depNode,
		screl.MustQuery(
			desc.Type(
				(*scpb.Database)(nil),
				(*scpb.Schema)(nil),
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
				(*scpb.Sequence)(nil),
				(*scpb.AliasType)(nil),
				(*scpb.EnumType)(nil),
			),
			dep.Type(
				(*scpb.ColumnFamily)(nil),
				(*scpb.Owner)(nil),
				(*scpb.UserPrivileges)(nil),
			),

			descID.Entities(screl.DescID, desc, dep),

			screl.JoinTarget(desc, descTarget),
			descTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			screl.JoinTargetNode(dep, depTarget, depNode),
			depTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
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
