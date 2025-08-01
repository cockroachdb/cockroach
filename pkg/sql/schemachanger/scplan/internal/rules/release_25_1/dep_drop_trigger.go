// Copyright 2024 The Cockroach Authors.
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

func init() {
	// The Trigger element must be removed before TriggerDeps in particular, in
	// order to ensure that back-references are updated correctly.
	registerDepRuleForDrop(
		"trigger removed before dependents",
		scgraph.Precedence,
		"trigger", "dependents",
		scpb.Status_ABSENT, scpb.Status_PUBLIC,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				to.Type((*scpb.Trigger)(nil)),
				from.TypeFilter(rulesVersionKey, isTriggerDependent),
				JoinOnTriggerID(from, to, "table-id", "trigger-id"),
			}
		},
	)
}
