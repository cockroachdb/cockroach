// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_23_1

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

func init() {
	registerDepRule(
		"descriptor existence precedes dependents",
		scgraph.Precedence,
		"relation", "dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.TypeFilter(rulesVersionKey, Not(isDescriptor)),
				JoinOnDescID(from, to, "relation-id"),
				StatusesToPublicOrTransient(from, scpb.Status_DESCRIPTOR_ADDED, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"dependents exist before descriptor becomes public",
		scgraph.Precedence,
		"dependent", "relation",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, Not(isDescriptor)),
				to.TypeFilter(rulesVersionKey, isDescriptor),
				JoinOnDescID(from, to, "relation-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}
