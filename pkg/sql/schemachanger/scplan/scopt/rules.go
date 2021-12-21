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

// When dropping relation objects, we need to mark the DROP op edges for their
// dependent elements as no-op.
func init() {
	// Dependent objects that will have edges marked as no-op.
	dep, depTarget, depNode := targetNodeVars("dep")
	// Relation that is being dropped.
	relation, relationTarget, relationNode := targetNodeVars("rel")
	var id rel.Var = "id"
	registerNoOpEdges(
		depNode, // source node of op edge to mark as no-op.
		screl.MustQuery(
			relation.Type((*scpb.Table)(nil), (*scpb.View)(nil), (*scpb.Sequence)(nil)),
			dep.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil),
				(*scpb.IndexName)(nil), (*scpb.Column)(nil), (*scpb.ColumnName)(nil),
				(*scpb.ForeignKeyBackReference)(nil), (*scpb.ForeignKey)(nil),
				(*scpb.CheckConstraint)(nil), (*scpb.UniqueConstraint)(nil),
				(*scpb.ConstraintName)(nil), (*scpb.Owner)(nil),
				(*scpb.Locality)(nil), (*scpb.UserPrivileges)(nil)),
			id.Entities(screl.DescID, relation, dep),

			// If the relation is in any drop state in the current phase,
			// then any dependent edges should be cleaned up.
			screl.JoinTargetNode(relation, relationTarget, relationNode),
			relationTarget.AttrEq(screl.Direction, scpb.Target_DROP),

			screl.JoinTargetNode(dep, depTarget, depNode),
			depTarget.AttrEq(screl.Direction, scpb.Target_DROP),
		),
	)
}

// When dropping a column we need to mark the DROP op edges for its column name
// element as no-op.
func init() {
	name, nameTarget, nameNode := targetNodeVars("dep")
	col, colTarget, colNode := targetNodeVars("col")
	var id rel.Var = "id"
	registerNoOpEdges(
		nameNode,
		screl.MustQuery(
			col.Type((*scpb.Column)(nil)),
			name.Type((*scpb.ColumnName)(nil)),
			id.Entities(screl.ColumnID, col, name),

			// If the relation is in any drop state in the current phase,
			// then any dependent edges should be cleaned up.
			screl.JoinTargetNode(name, nameTarget, nameNode),
			name.AttrEq(screl.Direction, scpb.Target_DROP),

			screl.JoinTargetNode(col, colTarget, colNode),
			col.AttrEq(screl.Direction, scpb.Target_DROP),
		),
	)
}

// When dropping an index we need to mark the DROP op edges for its index name
// element as no-op.
func init() {
	name, nameTarget, nameNode := targetNodeVars("dep")
	idx, idxTarget, idxNode := targetNodeVars("idx")
	var id rel.Var = "id"
	registerNoOpEdges(
		nameNode,
		screl.MustQuery(
			idx.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
			name.Type((*scpb.IndexName)(nil)),
			id.Entities(screl.IndexID, idx, name),

			// If the relation is in any drop state in the current phase,
			// then any dependent edges should be cleaned up.
			screl.JoinTargetNode(name, nameTarget, nameNode),
			name.AttrEq(screl.Direction, scpb.Target_DROP),

			screl.JoinTargetNode(idx, idxTarget, idxNode),
			idx.AttrEq(screl.Direction, scpb.Target_DROP),
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
