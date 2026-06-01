// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package current

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// These rules ensure that when renaming a trigger, the old TriggerName element
// is dropped before the new TriggerName element is added. This prevents
// conflicts and ensures proper rollback behavior.
func init() {
	registerDepRule(
		"old trigger name is dropped before new trigger name is added",
		scgraph.Precedence,
		"old-trigger-name", "new-trigger-name",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TriggerName)(nil)),
				to.Type((*scpb.TriggerName)(nil)),
				JoinOnTriggerID(from, to, "table-id", "trigger-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// This rule ensures that when a trigger name is being freed up (going to
	// ABSENT), that name cannot be used by a different trigger until the old
	// name is fully removed. This supports same-statement name swaps between
	// distinct triggers on the same table.
	registerDepRule(
		"trigger name is dropped before same name is used by different trigger",
		scgraph.Precedence,
		"old-trigger-name", "new-trigger-name",
		func(from, to NodeVars) rel.Clauses {
			name := rel.Var("name")
			return rel.Clauses{
				from.Type((*scpb.TriggerName)(nil)),
				to.Type((*scpb.TriggerName)(nil)),
				JoinOnDescID(from, to, "table-id"),
				from.El.AttrEqVar(screl.Name, name),
				to.El.AttrEqVar(screl.Name, name),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
				FilterElements("DifferentTriggerIDs", from, to, func(from, to *scpb.TriggerName) bool {
					return from.TriggerID != to.TriggerID
				}),
			}
		},
	)
}
