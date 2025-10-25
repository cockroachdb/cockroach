// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_25_4

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
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
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"old-index-id",
				),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_VALIDATED),
				to.TargetStatus(scpb.ToPublic, scpb.TransientAbsent),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"old-index", "new-index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"old-index-id",
				),
				from.TargetStatus(scpb.TransientAbsent),
				from.CurrentStatus(scpb.Status_TRANSIENT_VALIDATED),
				to.TargetStatus(scpb.ToPublic, scpb.TransientAbsent),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"new-index", "old-index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.SourceIndexID,
					to, screl.IndexID,
					"old-index-id",
				),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_VALIDATED),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
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
		func(from, to NodeVars) rel.Clauses {
			union := MkNodeVars("transient-primary-index")
			relationID := rel.Var("table-id")
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				union.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				JoinOnDescID(from, union, relationID),
				JoinOn(
					from, screl.IndexID,
					union, screl.SourceIndexID,
					"old-index-id",
				),
				JoinOnDescID(union, to, relationID),
				JoinOn(
					union, screl.IndexID,
					to, screl.SourceIndexID,
					"transient-index-id",
				),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}

// This rule ensures that when secondary indexes are re-created after a primary
// index key is changed, that the secondary indexes are swapped in an atomic
// manner, so that queries are not impacted by missing indexes.
func init() {
	// This ia strict version of the rule that will only work, when a node
	// is generating a plan on the latest master / 23.1. The StrictRecreate flag
	// will be used to tag if the existing secondary index was created on a
	// new enough version.
	registerDepRule(
		"replacement secondary index should be validated before the old one becomes invisible",
		scgraph.Precedence,
		"new-index", "old-index",
		func(from, to NodeVars) rel.Clauses {
			// Detect a potential secondary index recreation because of a ALTER
			// PRIMARY KEY, and require that the new index should be public,
			// before the old index can be hidden (i.e. they are swapped
			// an atomic manner).
			return append(IsPotentialSecondaryIndexSwapWithNodeVars(to, from, "index-id", "table-id"),
				from.CurrentStatus(scpb.Status_PUBLIC),
				to.CurrentStatus(scpb.Status_VALIDATED),
			)
		},
	)

	registerDepRule(
		"old secondary indexes that have been recreated should be removed"+
			" when their replacement is usable.",
		scgraph.SameStagePrecedence,
		"new-primary-index", "old-secondary-index",
		func(from, to NodeVars) rel.Clauses {
			newIndex := MkNodeVars("new-index")
			return append(rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_VALIDATED),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
				newIndex.El.AttrEqVar(screl.RecreateTargetIndexID, "new-primary-index-id"),
				to.El.AttrEqVar(screl.IndexID, "new-primary-index-id"),
				// Only active this rule on secondary indexes created after 25.4
				FilterElements("only apply to secondary indexes created after 25.4",
					newIndex, to,
					func(newIndex *scpb.SecondaryIndex, to *scpb.PrimaryIndex) bool {
						return newIndex.HideForPrimaryKeyRecreated
					}),
			},
				IsPotentialSecondaryIndexSwapWithNodeVars(from, newIndex, "index-id", "table-id")...)
		},
	)
}
