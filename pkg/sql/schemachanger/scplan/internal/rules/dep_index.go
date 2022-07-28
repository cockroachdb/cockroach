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
				from.el.Type((*scpb.PrimaryIndex)(nil)),
				to.el.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from.el, to.el, "table-id"),
				targetStatus(from.target, scpb.ToAbsent),
				targetStatus(to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_VALIDATED),
				currentStatus(to.node, scpb.Status_PUBLIC),
				rel.Filter(
					"primary-indexes-depend-on-each-other", from.el, to.el,
				)(func(idx, otherIdx *scpb.PrimaryIndex) bool {
					return idx.SourceIndexID == otherIdx.IndexID || idx.IndexID == otherIdx.SourceIndexID
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
				from.el.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				to.el.Type(
					(*scpb.IndexName)(nil),
					(*scpb.IndexComment)(nil),
				),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_BACKFILL_ONLY),
				currentStatus(to.node, scpb.Status_PUBLIC),
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
				from.el.Type((*scpb.TemporaryIndex)(nil)),
				to.el.Type(
					(*scpb.IndexColumn)(nil),
					(*scpb.IndexPartitioning)(nil),
					(*scpb.SecondaryIndexPartial)(nil),
				),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				targetStatus(from.target, scpb.Transient),
				targetStatus(to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_PUBLIC),
			}
		})

	// Once the index is public, its comment should be visible.
	registerDepRule(
		"comment existence precedes index becoming public",
		scgraph.Precedence,
		"child", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type(
					(*scpb.IndexComment)(nil),
				),
				to.el.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_PUBLIC),
				currentStatus(to.node, scpb.Status_PUBLIC),
			}
		},
	)
	registerDepRule(
		"index named right before index becomes public",
		scgraph.SameStagePrecedence,
		"index-name", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.IndexName)(nil)),
				to.el.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatusEq(from.node, to.node, scpb.Status_PUBLIC),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
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
				from.el.Type((*scpb.IndexColumn)(nil)),
				to.el.Type((*scpb.SecondaryIndex)(nil)),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_ABSENT),
				currentStatus(to.node, scpb.Status_ABSENT),
			}
		},
	)
	registerDepRule(
		"secondary index in DELETE_ONLY before removing columns",
		scgraph.Precedence,
		"index", "index-column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.SecondaryIndex)(nil)),
				to.el.Type((*scpb.IndexColumn)(nil)),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_ABSENT),
			}
		},
	)
	registerDepRule(
		"temp index columns removed before removing the index",
		scgraph.Precedence,
		"index-column", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.IndexColumn)(nil)),
				to.el.Type((*scpb.TemporaryIndex)(nil)),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_ABSENT),
				currentStatus(to.node, scpb.Status_TRANSIENT_ABSENT),
			}
		},
	)
	registerDepRule(
		"temp index in DELETE_ONLY before removing columns",
		scgraph.Precedence,
		"index", "index-column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.TemporaryIndex)(nil)),
				to.el.Type((*scpb.IndexColumn)(nil)),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_TRANSIENT_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_ABSENT),
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
				indexDependents(from.el, to.el, "table-id", "index-id"),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_VALIDATED),
				currentStatus(to.node, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"dependents removed before index",
		scgraph.Precedence,
		"dependent", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				indexDependents(to.el, from.el, "table-id", "index-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),
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
				from.el.Type((*scpb.TemporaryIndex)(nil)),
				to.el.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from.el, to.el, "desc-id"),
				joinOn(from.el, screl.IndexID, to.el, screl.TemporaryIndexID, "temp-index-id"),
				targetStatus(from.target, scpb.Transient),
				targetStatus(to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_WRITE_ONLY),
				currentStatus(to.node, scpb.Status_BACKFILLED),
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
				from.el.Type((*scpb.PrimaryIndex)(nil)),
				to.el.Type((*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from.el, to.el, "table-id"),
				joinOn(
					from.el, screl.IndexID,
					to.el, screl.SourceIndexID,
					"primary-index-id",
				),
				targetStatus(from.target, scpb.ToPublic),
				targetStatus(to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_PUBLIC),
				currentStatus(to.node, scpb.Status_BACKFILL_ONLY),
			}
		})
	registerDepRule(
		"primary index with new columns should exist before temp indexes",
		scgraph.Precedence,
		"primary-index", "second-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.PrimaryIndex)(nil)),
				to.el.Type((*scpb.TemporaryIndex)(nil)),
				joinOnDescID(from.el, to.el, "table-id"),
				joinOn(from.el, screl.IndexID, to.el, screl.SourceIndexID, "primary-index-id"),
				targetStatus(from.target, scpb.ToPublic),
				targetStatus(to.target, scpb.Transient),
				currentStatus(from.node, scpb.Status_PUBLIC),
				currentStatus(to.node, scpb.Status_DELETE_ONLY),
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
				from.el.Type((*scpb.TemporaryIndex)(nil)),
				to.el.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from.el, to.el, "descID"),
				targetStatus(from.target, scpb.Transient),
				targetStatus(to.target, scpb.ToAbsent),
				currentStatus(from.node, scpb.Status_TRANSIENT_ABSENT),
				currentStatus(to.node, scpb.Status_ABSENT),
			}
		})
	registerDepRule("indexes reach absent at the same time as other indexes",
		scgraph.SameStagePrecedence,
		"index-a", "index-b",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				to.el.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from.el, to.el, "descID"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),

				// Use the index ID to provide an ordering for dropping the indexes
				// to ensure that there is no cycle in the edges.
				//
				// TODO(ajwerner): It'd be nice to be able to express this in rel
				// directly.
				rel.Filter("indexes-id-less", "a", "b")(func(a, b scpb.Element) bool {
					aID, _ := screl.GetIndexID(a)
					bID, _ := screl.GetIndexID(b)
					return aID < bID
				}),
			}
		})
}
