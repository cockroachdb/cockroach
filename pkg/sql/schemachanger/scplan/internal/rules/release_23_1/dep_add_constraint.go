// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package release_23_1

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

// These rules ensure that constraint-dependent elements, like a constraint's
// name, etc. appear once the constraint reaches a suitable state.
func init() {
	registerDepRule(
		"constraint dependent public right before complex constraint",
		scgraph.SameStagePrecedence,
		"dependent", "complex-constraint",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isConstraintDependent, Not(isConstraintWithIndexName)),
				to.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, isSubjectTo2VersionInvariant),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"simple constraint public right before its dependents",
		scgraph.SameStagePrecedence,
		"simple-constraint", "dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, Not(isNonIndexBackedCrossDescriptorConstraint)),
				to.TypeFilter(rulesVersionKey, isConstraintDependent),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)

	// Constraint name should be assigned right before it becomes visible, otherwise
	// we won't have the correct message inside errors.
	registerDepRule(
		"simple constraint visible before name",
		scgraph.Precedence,
		"simple-constraint", "constraint-name",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint),
				to.TypeFilter(rulesVersionKey, isConstraintWithIndexName),
				JoinOnConstraintID(from, to, "table-id", "constraint-id"),
				StatusesToPublicOrTransient(from, scpb.Status_WRITE_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)
}
