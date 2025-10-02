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
}
