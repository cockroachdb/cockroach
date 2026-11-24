// Copyright 2025 The Cockroach Authors.
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

// These rules ensure that when renaming a column, the old ColumnName element
// is dropped before the new ColumnName element is added. This prevents
// conflicts and ensures proper rollback behavior.
func init() {
	registerDepRule(
		"old column name is dropped before new column name is added",
		scgraph.Precedence,
		"old-column-name", "new-column-name",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnName)(nil)),
				to.Type((*scpb.ColumnName)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// This rule ensures that when a column name is being freed up (going to ABSENT),
	// that name cannot be used by a different column until the old name is fully removed.
	// This prevents duplicate column name errors when renaming one column and adding
	// another column with the old name in the same statement.
	registerDepRule(
		"column name is dropped before same name is used by different column",
		scgraph.Precedence,
		"old-column-name", "new-column-name",
		func(from, to NodeVars) rel.Clauses {
			name := rel.Var("name")
			return rel.Clauses{
				from.Type((*scpb.ColumnName)(nil)),
				to.Type((*scpb.ColumnName)(nil)),
				JoinOnDescID(from, to, "table-id"),
				from.El.AttrEqVar(screl.Name, name),
				to.El.AttrEqVar(screl.Name, name),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
				FilterElements("DifferentColumnIDs", from, to, func(from, to *scpb.ColumnName) bool {
					// This filter should always return true, since the builder will
					// already deduplicate elements with the same name, columnID, and
					// tableID. Keeping it here makes the intent clearer.
					return from.ColumnID != to.ColumnID
				}),
			}
		},
	)
}
