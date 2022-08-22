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

// These rules ensure that constraint-dependent elements, like a constraint's
// name, etc. appear once the constraint reaches a suitable state.
func init() {

	registerDepRule(
		"constraint dependent public right before constraint",
		scgraph.SameStagePrecedence,
		"constraint", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isConstraint),
				to.typeFilter(isConstraintDependent),
				joinOnConstraintID(from, to, "table-id", "constraint-id"),
				statusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}
