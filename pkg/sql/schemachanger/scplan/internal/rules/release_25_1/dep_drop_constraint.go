// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_25_1

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

// These rules ensure that constraint-dependent elements, like an constraint's
// name, etc. disappear once the constraint reaches a suitable state.
func init() {

	registerDepRuleForDrop(
		"constraint no longer public before dependents",
		scgraph.Precedence,
		"constraint", "dependent",
		scpb.Status_VALIDATED, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, isSubjectTo2VersionInvariant),
				to.TypeFilter(rulesVersionKey, isConstraintDependent, Not(isConstraintWithoutIndexName)),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)

	registerDepRuleForDrop(
		"dependents removed before constraint",
		scgraph.Precedence,
		"dependents", "constraint",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isConstraintDependent, Not(isConstraintWithoutIndexName)),
				to.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, isSubjectTo2VersionInvariant),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)
}

// These rules apply to simple constraints and ensure that their dependents, like
// their names, comments, etc., disappear right before the simple constraint.
func init() {

	registerDepRuleForDrop(
		"dependents removed right before simple constraint",
		scgraph.SameStagePrecedence,
		"dependents", "constraint",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isConstraintDependent, Not(isConstraintWithoutIndexName)),
				to.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, Not(isSubjectTo2VersionInvariant)),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)

	// Constraint name should be cleared right before the constraint is no
	// longer visible.
	registerDepRuleForDrop(
		"Constraint should be hidden before name",
		scgraph.Precedence,
		"constraint-name", "constraint",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ConstraintWithoutIndexName)(nil)),
				to.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)
	// Constraint should be validated before the constraint name is attempted
	// to be cleaned.
	registerDepRuleForDrop(
		"Constraint should be hidden before name",
		scgraph.Precedence,
		"constraint", "constraint-name",
		scpb.Status_VALIDATED, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint),
				to.Type((*scpb.ConstraintWithoutIndexName)(nil)),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)
}
