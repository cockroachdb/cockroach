// Copyright 2024 The Cockroach Authors.
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

// These rules ensure that trigger-dependent elements, like a trigger's name,
// etc. disappear once the trigger reaches a suitable state.
func init() {
	registerDepRuleForDrop(
		"trigger no longer public before dependents",
		scgraph.Precedence,
		"trigger", "dependent",
		scpb.Status_PUBLIC, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Trigger)(nil)),
				to.TypeFilter(rulesVersionKey, isTriggerDependent),
				JoinOnConstraintID(from, to, "table-id", "trigger-id"),
			}
		},
	)
	registerDepRuleForDrop(
		"dependents removed before trigger",
		scgraph.Precedence,
		"dependents", "trigger",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isTriggerDependent),
				to.Type((*scpb.Trigger)(nil)),
				JoinOnConstraintID(from, to, "table-id", "trigger-id"),
			}
		},
	)
}
