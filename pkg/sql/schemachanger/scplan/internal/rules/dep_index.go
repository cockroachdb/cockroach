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
				from.targetStatus(scpb.ToAbsent),
				to.targetStatus(scpb.ToPublic),
				from.currentStatus(scpb.Status_VALIDATED),
				to.currentStatus(scpb.Status_PUBLIC),
				filterElements("primaryIndexesDependency", from, to, func(i1, i2 *scpb.PrimaryIndex) bool {
					return i1.SourceIndexID == i2.IndexID || i1.IndexID == i2.SourceIndexID
				}),
			}
		},
	)
}

// These rules ensure that index-dependent elements, like an index's name, its
// partitioning, etc. appear once the index reaches a suitable state.
// Vice-versa for index removal.
func init() {
	registerDepRule(
		"index existence precedes index name and comment",
		scgraph.Precedence,
		"index", "index-dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				to.Type(
					(*scpb.IndexName)(nil),
					(*scpb.IndexComment)(nil),
				),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToPublic(from, scpb.Status_BACKFILL_ONLY, to, scpb.Status_PUBLIC),
			}
		})

	// This rule pairs with the rule which ensures that columns are added to
	// the index before it receives writes.
	registerDepRule(
		"temp index exists before columns, partitioning, and partial",
		scgraph.Precedence,
		"temp-index", "index-partitioning",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type(
					(*scpb.IndexColumn)(nil),
					(*scpb.IndexPartitioning)(nil),
					(*scpb.SecondaryIndexPartial)(nil),
				),
				joinOnIndexID(from, to, "table-id", "index-id"),
				from.targetStatus(scpb.Transient),
				to.targetStatus(scpb.ToPublic),
				from.currentStatus(scpb.Status_DELETE_ONLY),
				to.currentStatus(scpb.Status_PUBLIC),
			}
		})

	// Once the index is public, its comment should be visible.
	registerDepRule(
		"comment existence precedes index becoming public",
		scgraph.Precedence,
		"child", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.IndexComment)(nil),
				),
				to.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToPublic(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
	registerDepRule(
		"index named right before index becomes public",
		scgraph.SameStagePrecedence,
		"index-name", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexName)(nil)),
				to.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				statusesToPublic(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
				joinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)

	// If we're going to be removing columns from an index, we know that
	// it'll be because we're dropping the index. If we're dropping the
	// index and not the descriptor, we need to make sure that we only
	// do it once the index is definitely being dropped. The reason for
	// this is roundabout: dropping a column from an index which is itself
	// being dropped is treated as a no-op by the execution layer.
	//
	// TODO(ajwerner): This pair of rules really feels like it ought to be a
	// same stage precedence sort of rule where we remove the columns from the
	// index when we remove the index, but for some reason, that overconstrains
	// the graph when dropping the table. Because of that, we allow the column
	// to be removed from the index in DELETE_ONLY, and we no-op the removal.
	registerDepRule(
		"secondary index columns removed before removing the index",
		scgraph.Precedence,
		"index-column", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexColumn)(nil)),
				to.Type((*scpb.SecondaryIndex)(nil)),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		},
	)
	registerDepRule(
		"secondary index in DELETE_ONLY before removing columns",
		scgraph.Precedence,
		"index", "index-column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.IndexColumn)(nil)),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToAbsent(from, scpb.Status_DELETE_ONLY, to, scpb.Status_ABSENT),
			}
		},
	)
	registerDepRule(
		"temp index columns removed before removing the index",
		scgraph.Precedence,
		"index-column", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexColumn)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_TRANSIENT_ABSENT),
			}
		},
	)
	registerDepRule(
		"temp index in DELETE_ONLY before removing columns",
		scgraph.Precedence,
		"index", "index-column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.IndexColumn)(nil)),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToAbsent(from, scpb.Status_TRANSIENT_DELETE_ONLY, to, scpb.Status_ABSENT),
			}
		},
	)

	// This rule is suspect. We absolutely cannot remove the partial predicate
	// until the index is not longer being written to. I think the same goes
	// for the columns. The partitioning is less clear. I think the name could
	// go earlier.
	registerDepRule(
		"index no longer public before dependents removed",
		scgraph.Precedence,
		"index", "child",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isIndex),
				to.typeFilter(isIndexDependent),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToAbsent(from, scpb.Status_VALIDATED, to, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"dependents removed before index",
		scgraph.Precedence,
		"dependent", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isIndexDependent),
				to.typeFilter(isIndex),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		},
	)
}

// These rules ensure that before an offline-backfilled index can begin
// backfilling, the corresponding temporary index exists in WRITE_ONLY.
func init() {
	registerDepRule(
		"temp index is WRITE_ONLY before backfill",
		scgraph.Precedence,
		"temp", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.TemporaryIndexID,
					"temp-index-id",
				),
				from.targetStatus(scpb.Transient),
				to.targetStatus(scpb.ToPublic),
				from.currentStatus(scpb.Status_WRITE_ONLY),
				to.currentStatus(scpb.Status_BACKFILLED),
			}
		},
	)
}

// We want to say that all columns which are part of a secondary index need
// to be in a primary index which is validated
// To do that, we want to find a secondary index which has a source which
// is a primary index which is itself new. Then we want to find
func init() {
	registerDepRule(
		"primary index with new columns should exist before secondary indexes",
		scgraph.Precedence,
		"primary-index", "second-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"primary-index-id",
				),
				statusesToPublic(from, scpb.Status_PUBLIC, to, scpb.Status_BACKFILL_ONLY),
			}
		})
	registerDepRule(
		"primary index with new columns should exist before temp indexes",
		scgraph.Precedence,
		"primary-index", "second-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(from, screl.IndexID, to, screl.SourceIndexID, "primary-index-id"),
				from.targetStatus(scpb.ToPublic),
				to.targetStatus(scpb.Transient),
				from.currentStatus(scpb.Status_PUBLIC),
				to.currentStatus(scpb.Status_DELETE_ONLY),
			}
		})
}

// This is a pair of somewhat brute-force hack to ensure that we only create
// a single GC job for all the indexes of a table being dropped by a
// transaction.
func init() {

	registerDepRule("temp indexes reach absent at the same time as other indexes",
		scgraph.SameStagePrecedence,
		"index-a", "index-b",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from, to, "desc-id"),
				from.targetStatus(scpb.Transient),
				to.targetStatus(scpb.ToAbsent),
				from.currentStatus(scpb.Status_TRANSIENT_ABSENT),
				to.currentStatus(scpb.Status_ABSENT),
			}
		})
	// TODO(postamar): reimplement rule
	// "indexes reach absent at the same time as other indexes"
}
