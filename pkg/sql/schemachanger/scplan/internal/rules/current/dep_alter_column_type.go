// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package current

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// This rule ensures that when changing the column type, that the old column
// type is dropped before the new type is added.
func init() {
	registerDepRule(
		"column type update is decomposed as a drop then add",
		scgraph.Precedence,
		"old-column-type", "new-column-type",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnType)(nil)),
				to.Type((*scpb.ColumnType)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_PUBLIC),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"column type is changed to public after doing validation of a transient check constraint",
		scgraph.SameStagePrecedence,
		"transient-check-constraint", "column-type",
		func(from, to NodeVars) rel.Clauses {
			colID := rel.Var("columnID")
			tableID := rel.Var("table-id")
			oldColumnType := MkNodeVars("old-column-type")
			return rel.Clauses{
				from.Type((*scpb.CheckConstraint)(nil)),
				to.Type((*scpb.ColumnType)(nil)),
				JoinOnDescID(from, to, tableID),
				to.El.AttrEqVar(screl.ColumnID, colID),
				from.ReferencedColumnIDsContains(colID),
				from.TargetStatus(scpb.TransientAbsent),
				to.TargetStatus(scpb.ToPublic),
				from.CurrentStatus(scpb.Status_TRANSIENT_VALIDATED),
				to.CurrentStatus(scpb.Status_PUBLIC),
				// Only apply this rule if there's an old column type being dropped,
				// which indicates this is an ALTER COLUMN TYPE operation.
				oldColumnType.Type((*scpb.ColumnType)(nil)),
				oldColumnType.El.AttrEqVar(screl.DescID, tableID),
				oldColumnType.El.AttrEqVar(screl.ColumnID, colID),
				oldColumnType.TargetStatus(scpb.ToAbsent),
				oldColumnType.JoinTargetNode(),
			}
		},
	)

	registerDepRule(
		"adding a transient column compute expression moves to 'absent' after PK validation to ensures it's there for the backfill",
		scgraph.Precedence,
		"primary-index", "transient-column-compute",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.ColumnComputeExpression)(nil)),
				JoinOnDescID(from, to, "table-id"),
				from.TargetStatus(scpb.ToPublic),
				to.TargetStatus(scpb.TransientAbsent),
				from.CurrentStatus(scpb.Status_VALIDATED),
				to.CurrentStatus(scpb.Status_TRANSIENT_ABSENT),
			}
		},
	)

	// The transient expression must keep converting concurrent writes until the
	// replacement column becomes public. Removing it in an earlier stage would
	// let writes populate the write-only column with a default instead.
	registerDepRule(
		"transient column compute expression is removed when the column becomes public",
		scgraph.SameStagePrecedence,
		"transient-column-compute", "column",
		func(from, to NodeVars) rel.Clauses {
			publicNode := rel.Var("public-transient-column-compute-node")
			return rel.Clauses{
				from.Type((*scpb.ColumnComputeExpression)(nil)),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				from.El.AttrEq(screl.Usage, scpb.ColumnComputeExpression_ALTER_TYPE_USING),
				from.TargetStatus(scpb.TransientAbsent),
				to.TargetStatus(scpb.ToPublic),
				from.CurrentStatus(scpb.Status_TRANSIENT_ABSENT),
				to.CurrentStatus(scpb.Status_PUBLIC),
				// An older version may already have removed the expression. Do not
				// require the two transitions to share a stage in that case.
				publicNode.Type((*screl.Node)(nil)),
				publicNode.AttrEqVar(screl.Target, from.Target),
				publicNode.AttrEq(screl.CurrentStatus, scpb.Status_PUBLIC),
			}
		},
	)
}
