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

// These rules ensure that columns and indexes containing these columns
// disappear from existence in the correct order.
func init() {

	// Without this rule, we might have an index which exists and contains
	// a column which does not exist. This would lead to panics inside the
	// optimizer and an invalid table descriptor.
	registerDepRuleForDrop("indexes containing column reach absent before column",
		scgraph.Precedence,
		"index", "column",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to nodeVars) rel.Clauses {
			ic, ct := mkNodeVars("index-column"), mkNodeVars("column-type")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				ct.Type((*scpb.ColumnType)(nil)),
				columnInIndex(ic, from, relationID, columnID, "index-id"),
				joinOnColumnID(ic, to, relationID, columnID),
				joinOnColumnID(ic, ct, relationID, columnID),
				rel.Filter("relationIsNotBeingDropped", ct.el)(
					func(ct *scpb.ColumnType) bool {
						return !ct.IsRelationBeingDropped
					},
				),
			}
		})

	registerDepRule("secondary indexes containing column as key reach write-only before column",
		scgraph.Precedence,
		"index", "column",
		func(from, to nodeVars) rel.Clauses {
			ic, ct := mkNodeVars("index-column"), mkNodeVars("column-type")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				columnInIndex(ic, from, relationID, columnID, "index-id"),
				joinOnColumnID(ic, to, relationID, columnID),
				statusesToAbsent(from, scpb.Status_VALIDATED, to, scpb.Status_WRITE_ONLY),
				ct.Type((*scpb.ColumnType)(nil)),
				joinOnColumnID(ic, ct, relationID, columnID),
				rel.Filter("relationIsNotBeingDropped", ct.el)(
					func(ct *scpb.ColumnType) bool {
						return !ct.IsRelationBeingDropped
					},
				),
				rel.Filter("isIndexKeyColumnKey", ic.el)(
					func(ic *scpb.IndexColumn) bool {
						return ic.Kind == scpb.IndexColumn_KEY
					},
				),
			}
		})

}
