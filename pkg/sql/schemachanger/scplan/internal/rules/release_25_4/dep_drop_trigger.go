// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_25_4

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

	// Ensure that trigger dependencies are cleared before any external table
	// references.
	registerDepRuleForDrop("trigger references cleaned before columns",
		scgraph.Precedence,
		"trigger dependency", "column",
		scpb.Status_ABSENT, scpb.Status_WRITE_ONLY,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TriggerDeps)(nil)),
				to.Type((*scpb.Column)(nil)),
				FilterElements("trigger refers to column", from, to, func(trigger *scpb.TriggerDeps, column *scpb.Column) bool {
					// Note: This rule is fairly expensive since it needs to join on
					// all columns in a plan. Sadly, the current embedding makes this
					// suboptimal so we can only use a filter here.
					for _, colRef := range trigger.UsesRelations {
						// Check if it refers to this table.
						if colRef.ID != column.TableID {
							continue
						}
						// Check if it refers to this column.
						for _, columnID := range colRef.ColumnIDs {
							if columnID == column.ColumnID {
								return true
							}
						}
					}
					// Otherwise, no references exist.
					return false
				}),
			}
		})
}
