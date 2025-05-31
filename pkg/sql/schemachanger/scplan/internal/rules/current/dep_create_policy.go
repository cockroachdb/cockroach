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

func init() {
	registerDepRule(
		"dependents added after policy",
		scgraph.SameStagePrecedence,
		"policy", "dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Policy)(nil)),
				to.TypeFilter(rulesVersionKey, isPolicyDependent),
				JoinOnPolicyID(from, to, "table-id", "policy-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}

func init() {
	// This rule ensures that when a policy dependent is being swapped are
	// done in the correct order. The dropped element should disappear before
	// the replacement element is added.
	registerDepRule(
		"policy dependents are swapped in order",
		scgraph.Precedence,
		"drop-policy-dependent", "add-policy-dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isPolicyDependent),
				to.TypeFilter(rulesVersionKey, isPolicyDependent),
				// Confirm we are joining on the same types.
				from.El.AttrEqVar(rel.Type, "sameType"),
				to.El.AttrEqVar(rel.Type, "sameType"),
				JoinOnPolicyID(from, to, "table-id", "policy-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic, scpb.TransientAbsent),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
