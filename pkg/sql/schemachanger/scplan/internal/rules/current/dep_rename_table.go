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

// These rules ensure that when renaming a table, the old Namespace element
// is dropped before the new Namespace element is added. This prevents
// conflicts and ensures proper rollback behavior.
func init() {
	registerDepRule(
		"old table namespace is dropped before new table namespace is added",
		scgraph.Precedence,
		"old-namespace", "new-namespace",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Namespace)(nil)),
				to.Type((*scpb.Namespace)(nil)),
				from.El.AttrEqVar(rel.Type, "same-type"),
				to.El.AttrEqVar(rel.Type, "same-type"),
				JoinOnDescID(from, to, "desc-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}