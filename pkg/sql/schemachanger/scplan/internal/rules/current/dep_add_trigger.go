// Copyright 2024 The Cockroach Authors.
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

func init() {
	registerDepRule(
		"trigger public before its dependents",
		scgraph.Precedence,
		"trigger", "dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Trigger)(nil)),
				to.TypeFilter(rulesVersionKey, isTriggerDependent),
				JoinOnTriggerID(from, to, "table-id", "trigger-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}
