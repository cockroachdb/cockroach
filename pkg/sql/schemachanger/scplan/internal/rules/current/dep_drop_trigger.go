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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

func init() {
	// The trigger dependents are removed before the trigger.
	registerDepRuleForDrop(
		"dependents remove trigger",
		scgraph.SameStagePrecedence,
		"dependents", "trigger",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Trigger)(nil)),
				to.TypeFilter(rulesVersionKey, isTriggerDependent),
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
	// Ensure that trigger dependencies are cleared before any referenced
	// relation descriptors are removed.
	registerDepRuleForDrop("trigger references cleaned before referenced relations",
		scgraph.Precedence,
		"trigger dependency", "relation",
		scpb.Status_ABSENT, scpb.Status_DROPPED,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TriggerDeps)(nil)),
				to.Type((*scpb.Table)(nil), (*scpb.View)(nil), (*scpb.Sequence)(nil)),
				FilterElements("trigger refers to relation", from, to, func(trigger *scpb.TriggerDeps, relation scpb.Element) bool {
					descID := screl.GetDescID(relation)
					// Avoid self-references to avoid cycles.
					if descID == trigger.TableID {
						return false
					}
					for _, ref := range trigger.UsesRelations {
						if ref.ID == descID {
							return true
						}
					}
					return false
				}),
			}
		})

	registerDepRuleForDrop("trigger references cleaned before referenced types",
		scgraph.Precedence,
		"trigger dependency", "types",
		scpb.Status_ABSENT, scpb.Status_DROPPED,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TriggerDeps)(nil)),
				to.TypeFilter(rulesVersionKey, isTypeDescriptor),
				to.El.AttrEqVar(screl.DescID, "desc-id"),
				from.El.AttrContainsVar(screl.ReferencedTypeIDs, "desc-id"),
			}
		})

	registerDepRuleForDrop("trigger references cleaned before referenced functions",
		scgraph.SameStagePrecedence,
		"trigger dependency", "functions",
		scpb.Status_ABSENT, scpb.Status_DROPPED,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TriggerDeps)(nil)),
				to.Type((*scpb.Function)(nil)),
				to.El.AttrEqVar(screl.DescID, "desc-id"),
				from.El.AttrContainsVar(screl.ReferencedFunctionIDs, "desc-id"),
			}
		})
}
