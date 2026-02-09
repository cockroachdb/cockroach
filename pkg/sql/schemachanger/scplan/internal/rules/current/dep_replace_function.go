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

// These rules ensure that during CREATE OR REPLACE FUNCTION, old function
// elements are dropped before new ones are added, following the drop+add
// pattern used by TableStorageParam and RowLevelTTL.
func init() {
	registerDepRule(
		"old function body is dropped before the new one is added",
		scgraph.SameStagePrecedence,
		"old-function-body", "new-function-body",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.FunctionBody)(nil)),
				to.Type((*scpb.FunctionBody)(nil)),
				JoinOnDescID(from, to, "function-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"old function volatility is dropped before the new one is added",
		scgraph.SameStagePrecedence,
		"old-function-volatility", "new-function-volatility",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.FunctionVolatility)(nil)),
				to.Type((*scpb.FunctionVolatility)(nil)),
				JoinOnDescID(from, to, "function-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"old function leakproof is dropped before the new one is added",
		scgraph.SameStagePrecedence,
		"old-function-leakproof", "new-function-leakproof",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.FunctionLeakProof)(nil)),
				to.Type((*scpb.FunctionLeakProof)(nil)),
				JoinOnDescID(from, to, "function-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"old function null input behavior is dropped before the new one is added",
		scgraph.SameStagePrecedence,
		"old-function-null-input", "new-function-null-input",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.FunctionNullInputBehavior)(nil)),
				to.Type((*scpb.FunctionNullInputBehavior)(nil)),
				JoinOnDescID(from, to, "function-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"old function security is dropped before the new one is added",
		scgraph.SameStagePrecedence,
		"old-function-security", "new-function-security",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.FunctionSecurity)(nil)),
				to.Type((*scpb.FunctionSecurity)(nil)),
				JoinOnDescID(from, to, "function-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"old function params dropped before new one is added",
		scgraph.SameStagePrecedence,
		"old-function-params", "new-function-params",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.FunctionParams)(nil)),
				to.Type((*scpb.FunctionParams)(nil)),
				JoinOnDescID(from, to, "function-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
