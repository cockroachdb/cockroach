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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

// These rules ensure that constraint-dependent elements, like an constraint's
// name, etc. disappear once the constraint reaches a suitable state.
// TODO (xiang): The reason why we have two ugly dep rules here is because
// we only properly supported check constraint, but not UniqueWithIndex nor
// ForeignKey constraint yet. They currently directly transition between public
// and absent, while they should transition through an intermediate state.
// We can come back and condense all three rule into one rule -- constraint
// dependent absent right before constraint reaches validated (i.e. non-pubic).
func init() {

	registerDepRuleForDrop(
		"constraint dependent absent right before constraint",
		scgraph.SameStagePrecedence,
		"dependent", "constraint",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isConstraintDependent),
				to.typeFilter(isConstraint, not(isSupportedConstraint)),
				joinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)

	registerDepRuleForDrop(
		"constraint dependent absent right before constraint",
		scgraph.SameStagePrecedence,
		"dependent", "constraint",
		scpb.Status_ABSENT, scpb.Status_VALIDATED,
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isConstraintDependent),
				to.typeFilter(isSupportedConstraint),
				joinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)
}
