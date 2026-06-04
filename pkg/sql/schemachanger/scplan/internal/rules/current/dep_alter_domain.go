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
)

// Special rule to ensure that swapping a domain DEFAULT expression happens in
// the correct order.
func init() {
	registerDepRule(
		"handle domain default expression swaps",
		scgraph.SameStagePrecedence,
		"old-domain-default", "new-domain-default",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.DomainDefault)(nil)),
				to.Type((*scpb.DomainDefault)(nil)),
				JoinOnDescID(from, to, "type-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_WRITE_ONLY),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_WRITE_ONLY),
			}
		},
	)
}
