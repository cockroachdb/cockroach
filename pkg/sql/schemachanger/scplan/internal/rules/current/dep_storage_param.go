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

// These rules ensure that when setting a storage param, the old element is dropped
// before the new one is added. This prevents conflicts and ensures proper rollback behavior.
func init() {
	registerDepRule(
		"old storage param is dropped before the new one is added",
		scgraph.SameStagePrecedence,
		"old-storage-param", "new-storage-param",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TableStorageParam)(nil)),
				to.Type((*scpb.TableStorageParam)(nil)),
				JoinOnDescID(from, to, "table-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// This rule ensures that when modifying TTL configurations, the old TTL
	// element (with lower SeqNum) is processed before the new TTL element (with
	// higher SeqNum).
	registerDepRule(
		"ensure row level TTL configs are in increasing seqNum order",
		scgraph.Precedence,
		"later-seqNum", "earlier-seqNum",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.RowLevelTTL)(nil)),
				to.Type((*scpb.RowLevelTTL)(nil)),
				JoinOnDescID(from, to, "table-id"),
				FilterElements("SmallerSeqNumFirst", from, to, func(from, to *scpb.RowLevelTTL) bool {
					return from.SeqNum < to.SeqNum
				}),
			}
		})
}
