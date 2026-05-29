// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package current

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

// These rules ensure that when renaming an enum value, the old EnumTypeValue
// element is dropped before the new EnumTypeValue element is added. This
// prevents conflicts and ensures proper rollback behavior.
func init() {
	registerDepRule(
		"old enum type value is dropped before new enum type value is added",
		scgraph.Precedence,
		"old-enum-value", "new-enum-value",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.EnumTypeValue)(nil)),
				to.Type((*scpb.EnumTypeValue)(nil)),
				JoinOnDescID(from, to, "type-id"),
				FilterElements("SamePhysicalRepresentation", from, to,
					func(from, to *scpb.EnumTypeValue) bool {
						return bytes.Equal(from.PhysicalRepresentation, to.PhysicalRepresentation)
					},
				),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
