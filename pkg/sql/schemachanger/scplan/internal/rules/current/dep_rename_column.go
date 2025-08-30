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

// These rules ensure that when renaming a column, the old ColumnName element
// is dropped before the new ColumnName element is added. This prevents
// conflicts and ensures proper rollback behavior.
func init() {
	registerDepRule(
		"old column name is dropped before new column name is added",
		scgraph.Precedence,
		"old-column-name", "new-column-name",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnName)(nil)),
				to.Type((*scpb.ColumnName)(nil)),
				from.El.AttrEqVar(rel.Type, "same-type"),
				to.El.AttrEqVar(rel.Type, "same-type"),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
