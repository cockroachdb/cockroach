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

// These rules ensure that when renaming a constraint, the old name element
// is dropped before the new name element is added. This prevents conflicts
// and ensures proper rollback behavior.

func init() {
	// Rule for index-backed constraints (UNIQUE with index, PRIMARY KEY).
	registerDepRule(
		"old index name is dropped before new index name is added",
		scgraph.SameStagePrecedence,
		"old-index-name", "new-index-name",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexName)(nil)),
				to.Type((*scpb.IndexName)(nil)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// Rule for non-index constraints (CHECK, FK, UNIQUE without index).
	registerDepRule(
		"old constraint name is dropped before new constraint name is added",
		scgraph.SameStagePrecedence,
		"old-constraint-name", "new-constraint-name",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ConstraintWithoutIndexName)(nil)),
				to.Type((*scpb.ConstraintWithoutIndexName)(nil)),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// Rule to handle cross-constraint name conflicts for index-backed constraints.
	// This rule must NOT match during secondary index swaps (ALTER PRIMARY KEY),
	// where an old index is replaced by a new one with the same name.
	registerDepRule(
		"old index name is dropped before conflicting new index name is added",
		scgraph.Precedence,
		"old-index-name", "new-index-name",
		func(from, to NodeVars) rel.Clauses {
			nameVar := rel.Var("name")
			return rel.Clauses{
				from.Type((*scpb.IndexName)(nil)),
				to.Type((*scpb.IndexName)(nil)),
				JoinOnDescID(from, to, "table-id"),
				// Both have the same name (conflict).
				from.El.AttrEqVar(screl.Name, nameVar),
				to.El.AttrEqVar(screl.Name, nameVar),
				to.El.AttrEqVar(screl.IndexID, "index-id"),
				// Ensure the new index is not part of a secondary index swap.
				// This check will succeed for rename-only operations because there
				// won't be a matching secondary index swap pattern in the graph.
				IsNotPotentialSecondaryIndexSwap("table-id", "index-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// Rule to handle cross-constraint name conflicts for other constraint type
	// combos that involve constraints without index names.
	registerDepRule(
		"old constraint name is dropped before conflicting new constraint name is added when at least one is non-index-backed",
		scgraph.Precedence,
		"old-index-name", "new-index-name",
		func(from, to NodeVars) rel.Clauses {
			nameVar := rel.Var("name")
			return rel.Clauses{
				// Accept IndexName and ConstraintWithoutIndexName elements.
				from.Type((*scpb.IndexName)(nil), (*scpb.ConstraintWithoutIndexName)(nil)),
				to.Type((*scpb.IndexName)(nil), (*scpb.ConstraintWithoutIndexName)(nil)),
				JoinOnDescID(from, to, "table-id"),
				// Both have the same name (conflict).
				from.El.AttrEqVar(screl.Name, nameVar),
				to.El.AttrEqVar(screl.Name, nameVar),
				// But they must be different elements (handles both same-type and cross-type).
				FilterElements("names-are-for-different-elements", from, to,
					func(from, to scpb.Element) bool {
						_, fromIsIndexName := from.(*scpb.IndexName)
						_, toIsIndexName := to.(*scpb.IndexName)
						// The case where both are IndexName elements is handled
						// by a different rule.
						return !(fromIsIndexName && toIsIndexName) && from != to
					},
				),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
