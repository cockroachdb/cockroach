// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package current

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/common"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

var registry = common.NewRegistry()

func registerDepRule(
	ruleName scgraph.RuleName,
	kind scgraph.DepEdgeKind,
	fromEl, toEl string,
	def func(from, to common.NodeVars) rel.Clauses,
) {
	registry.RegisterDepRule(ruleName,
		kind,
		fromEl, toEl,
		def)
}

func registerOpRule(rn scgraph.RuleName, from rel.Var, q *rel.Query) {
	registry.RegisterOpRule(rn,
		from,
		q)
}

func registerDepRuleForDrop(
	ruleName scgraph.RuleName,
	kind scgraph.DepEdgeKind,
	from, to string,
	fromStatus, toStatus scpb.Status,
	fn func(from, to common.NodeVars) rel.Clauses,
) {
	common.RegisterDepRuleForDrop(registry,
		ruleName,
		kind,
		from, to,
		fromStatus, toStatus,
		fn)
}

func ApplyOpRules(ctx context.Context, g *scgraph.Graph) (*scgraph.Graph, error) {
	return registry.ApplyOpRules(ctx, g)
}

func ApplyDepRules(ctx context.Context, g *scgraph.Graph) error {
	return registry.ApplyDepRules(ctx, g)
}
