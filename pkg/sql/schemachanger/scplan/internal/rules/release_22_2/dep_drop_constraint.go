// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_22_2

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
		"constraint dependent absent right before constraint",
		scgraph.SameStagePrecedence,
		"dependent", "constraint",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isConstraintDependent),
				to.TypeFilter(rulesVersionKey, isConstraint, Not(IsIndex)),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)

	registerDepRuleForDrop(
		"constraint dependent absent right before constraint",
		scgraph.SameStagePrecedence,
		"dependent", "constraint",
		scpb.Status_VALIDATED, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isConstraintDependent),
				to.TypeFilter(rulesVersionKey, isConstraint, IsIndex),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)
}
