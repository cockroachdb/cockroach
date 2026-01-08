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

// These rules ensure that when adding, updating, or removing an identity column,
// the old ColumnGeneratedAsIdentity element is dropped before the new one is added.
// This prevents conflicts and ensures proper rollback behavior.
func init() {
	registerDepRule(
		"old column generated as identity is dropped before new one is added",
		scgraph.SameStagePrecedence,
		"old-column-generated-as-identity", "new-column-generated-as-identity",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnGeneratedAsIdentity)(nil)),
				to.Type((*scpb.ColumnGeneratedAsIdentity)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
