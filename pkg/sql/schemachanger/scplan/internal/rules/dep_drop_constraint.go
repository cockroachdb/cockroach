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
func init() {

	registerDepRuleForDrop(
		"constraint dependent absent right before constraint",
		scgraph.SameStagePrecedence,
		"dependent", "constraint",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isConstraintDependent),
				to.typeFilter(isConstraint, not(isIndex)),
				joinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)

	registerDepRuleForDrop(
		"constraint dependent absent right before constraint",
		scgraph.SameStagePrecedence,
		"dependent", "constraint",
		scpb.Status_VALIDATED, scpb.Status_ABSENT,
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isConstraintDependent),
				to.typeFilter(isConstraint, isIndex),
				joinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)
}
