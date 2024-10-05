// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_22_2

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

// These rules ensure that columns and indexes containing these columns
// disappear from existence in the correct order.
func init() {

	// Without this rule, we might have an index which exists and contains
	// a column which does rules.Not exist. This would lead to panics inside the
	// optimizer and an invalid table descriptor.
	registerDepRuleForDrop("indexes containing column reach absent before column",
		scgraph.Precedence,
		"index", "column",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			ic := MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				ColumnInIndex(ic, from, relationID, columnID, "index-id"),
				JoinOnColumnID(ic, to, relationID, columnID),
				descriptorIsNotBeingDropped(ic.El),
			}
		})

	registerDepRule("secondary indexes containing column as key reach write-only before column",
		scgraph.Precedence,
		"index", "column",
		func(from, to NodeVars) rel.Clauses {
			ic := MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				ColumnInIndex(ic, from, relationID, columnID, "index-id"),
				JoinOnColumnID(ic, to, relationID, columnID),
				StatusesToAbsent(from, scpb.Status_VALIDATED, to, scpb.Status_WRITE_ONLY),
				descriptorIsNotBeingDropped(ic.El),
				rel.Filter("rules.IsIndexKeyColumnKey", ic.El)(
					func(ic *scpb.IndexColumn) bool {
						return ic.Kind == scpb.IndexColumn_KEY
					},
				),
			}
		})

}
