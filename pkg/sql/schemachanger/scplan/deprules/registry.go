// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package deprules contains rules to generate dependency edges for a
// graph which contains op edges.
package deprules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scgraph"
)

// Apply will add the dependency edges to the graph which ought to exist
// according to the rules defined in this package.
func Apply(g *scgraph.Graph) error {
	for _, dr := range depRules {
		if err := dr.q.Iterate(g.Database(), func(r rel.Result) error {
			from := r.Var(dr.from).(*scpb.Node)
			to := r.Var(dr.to).(*scpb.Node)
			return g.AddDepEdge(
				dr.name, dr.kind, from.Target, from.Status, to.Target, to.Status,
			)
		}); err != nil {
			return err
		}
	}
	return nil
}

// depRules is a singleton which contains all the rules.
var depRules []rule

type rule struct {
	name     string
	from, to rel.Var
	q        *rel.Query
	kind     scgraph.DepEdgeKind
}

func register(ruleName string, edgeKind scgraph.DepEdgeKind, from, to rel.Var, query *rel.Query) {
	depRules = append(depRules, rule{
		name: ruleName,
		kind: edgeKind,
		from: from,
		to:   to,
		q:    query,
	})
}
