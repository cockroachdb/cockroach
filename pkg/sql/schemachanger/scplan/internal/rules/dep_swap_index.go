// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// This rule ensures that a new primary index becomes public right after the
// old primary index starts getting removed, effectively swapping one for the
// other. This rule also applies when the schema change gets reverted.
func init() {

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"old-index", "new-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"old-index-id",
				),
				from.targetStatus(scpb.ToAbsent),
				from.currentStatus(scpb.Status_VALIDATED),
				to.targetStatus(scpb.ToPublic, scpb.Transient),
				to.currentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"old-index", "new-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"old-index-id",
				),
				from.targetStatus(scpb.Transient),
				from.currentStatus(scpb.Status_TRANSIENT_VALIDATED),
				to.targetStatus(scpb.ToPublic, scpb.Transient),
				to.currentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"new-index", "old-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.SourceIndexID,
					to, screl.IndexID,
					"old-index-id",
				),
				from.targetStatus(scpb.ToAbsent),
				from.currentStatus(scpb.Status_VALIDATED),
				to.targetStatus(scpb.ToPublic),
				to.currentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}

// This rule ensures that when a transient primary index is involved in the
// swap, the old index is gone before the new index is instated.
func init() {

	registerDepRule(
		"old index absent before new index public when swapping with transient",
		scgraph.Precedence,
		"old-primary-index", "new-primary-index",
		func(from, to nodeVars) rel.Clauses {
			union := mkNodeVars("transient-primary-index")
			relationID := rel.Var("table-id")
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				union.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from, union, relationID),
				joinOn(
					from, screl.IndexID,
					union, screl.SourceIndexID,
					"old-index-id",
				),
				joinOnDescID(union, to, relationID),
				joinOn(
					union, screl.IndexID,
					to, screl.SourceIndexID,
					"transient-index-id",
				),
				from.targetStatus(scpb.ToAbsent),
				from.currentStatus(scpb.Status_ABSENT),
				to.targetStatus(scpb.ToPublic),
				to.currentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
