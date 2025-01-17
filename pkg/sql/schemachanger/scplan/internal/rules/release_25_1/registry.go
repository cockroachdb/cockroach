// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_25_1

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

var registry = NewRegistry()

func registerDepRule(
	ruleName scgraph.RuleName,
	kind scgraph.DepEdgeKind,
	fromEl, toEl string,
	def func(from, to NodeVars) rel.Clauses,
) {
	registry.RegisterDepRule(ruleName,
		kind,
		fromEl, toEl,
		def)
}

func registerDepRuleForDrop(
	ruleName scgraph.RuleName,
	kind scgraph.DepEdgeKind,
	from, to string,
	fromStatus, toStatus scpb.Status,
	fn func(from, to NodeVars) rel.Clauses,
) {
	RegisterDepRuleForDrop(registry,
		ruleName,
		kind,
		from, to,
		fromStatus, toStatus,
		fn)
}

// GetRegistry returns the registry for this cockroach release.
func GetRegistry() *Registry {
	return registry
}
