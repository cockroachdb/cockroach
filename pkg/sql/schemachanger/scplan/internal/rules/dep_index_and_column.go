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

// Special cases for removal of column types and index partial predicates,
// which hold references to other descriptors.
//
// When the whole table is dropped, we can (and in fact, should) remove these
// right away in-txn. However, when only the column (or the index) is
// dropped but the table remains, we need to wait until the column is
// DELETE_ONLY, which happens post-commit because of the need to uphold the
// 2-version invariant.
//
// We distinguish the two cases using a flag in ColumnType and
// SecondaryIndexPartial which is set iff the parent relation is dropped. This
// is a dirty hack, ideally we should be able to express the _absence_ of a
// target as a query clause.
//
// Note that DEFAULT and ON UPDATE expressions are column-dependent elements
// which also hold references to other descriptors. The rule prior to this one
// ensures that they transition to ABSENT before scpb.ColumnType does.
//
// TODO(postamar): express this rule in a saner way
func init() {

	registerDepRule(
		"column type removed right before column when not dropping relation",
		scgraph.SameStagePrecedence,
		"column-type", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.ColumnType)(nil)),
				to.el.Type((*scpb.Column)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),
				rel.Filter("columnTypeIsNotBeingDropped", from.el)(func(
					ct *scpb.ColumnType,
				) bool {
					return !ct.IsRelationBeingDropped
				}),
			}
		},
	)

	// Without this rule, we might have an index which exists and contains
	// a column which does not exist. This would lead to panics inside the
	// optimizer and an invalid table descriptor.
	registerDepRule("indexes containing columns reach absent before column",
		scgraph.Precedence,
		"index", "column",
		func(from, to nodeVars) rel.Clauses {
			ct := rel.Var("column-type")
			return rel.Clauses{
				from.el.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				to.el.Type((*scpb.Column)(nil)),
				indexContainsColumn(
					from.el, to.el, "index-column", "table-id", "column-id", "index-id",
				),
				ct.Type((*scpb.ColumnType)(nil)),
				joinOnColumnID(to.el, ct, "table-id", "column-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),
				rel.Filter("columnTypeIsNotBeingDropped", ct)(func(
					ct *scpb.ColumnType,
				) bool {
					return !ct.IsRelationBeingDropped
				}),
			}
		})

	registerDepRule(
		"partial predicate removed right before secondary index when not dropping relation",
		scgraph.SameStagePrecedence,
		"partial-predicate", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.SecondaryIndexPartial)(nil)),
				to.el.Type((*scpb.SecondaryIndex)(nil)),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),
				rel.Filter("secondaryIndexPartialIsNotBeingDropped", from.el)(func(
					ip *scpb.SecondaryIndexPartial,
				) bool {
					return !ip.IsRelationBeingDropped
				}),
			}
		},
	)
}

// These rules ensure that columns and indexes containing these columns
// appear into existence in the correct order.
func init() {

	registerDepRule(
		"adding column depends on primary index",
		scgraph.Precedence,
		"index", "column",
		func(from, to nodeVars) rel.Clauses {
			var status rel.Var = "status"
			return rel.Clauses{
				from.el.Type((*scpb.PrimaryIndex)(nil)),
				to.el.Type((*scpb.Column)(nil)),
				columnInPrimaryIndexSwap(
					from.el, to.el, "index-column", "table-id", "column-id", "index-id",
				),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				status.In(scpb.Status_PUBLIC),
				status.Entities(screl.CurrentStatus, from.node, to.node),
			}
		},
	)

	registerDepRule(
		"primary index should be cleaned up before newly added column when reverting",
		scgraph.Precedence,
		"index", "column",
		func(from, to nodeVars) rel.Clauses {
			var status rel.Var = "status"
			return rel.Clauses{
				from.el.Type((*scpb.PrimaryIndex)(nil)),
				to.el.Type((*scpb.Column)(nil)),
				toAbsent(from.target, to.target),
				columnInPrimaryIndexSwap(
					from.el, to.el, "indexColumn", "table-id", "column-id", "index-id",
				),
				status.Eq(scpb.Status_WRITE_ONLY),
				status.Entities(screl.CurrentStatus, from.node, to.node),
			}
		})
	registerDepRule(
		"column existence precedes index existence",
		scgraph.Precedence,
		"column", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				indexContainsColumn(
					to.el, from.el, "index-column", "table-id", "column-id", "index-id",
				),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_BACKFILL_ONLY),
			}
		},
	)

	// We need to make sure that we don't add a temporary index to a table
	// until the column has been added to the table.
	registerDepRule(
		"column existence precedes temporary index existence",
		scgraph.Precedence,
		"column", "temp-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type((*scpb.TemporaryIndex)(nil)),
				indexContainsColumn(
					to.el, from.el, "index-column", "table-id", "column-id", "index-id",
				),
				targetStatus(from.target, scpb.ToPublic),
				targetStatus(to.target, scpb.Transient),
				currentStatusEq(from.node, to.node, scpb.Status_DELETE_ONLY),
			}
		},
	)

	// We want to ensure that column names are not dropped until the column is
	// no longer in use in any dropping indexes.
	registerDepRule(
		"column name and type to public after all index column to public",
		scgraph.Precedence,
		"column-name-or-type", "index-column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.ColumnName)(nil), (*scpb.ColumnType)(nil)),
				to.el.Type((*scpb.IndexColumn)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "column-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatusEq(from.node, to.node, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule("index-column added to index after index exists",
		scgraph.Precedence,
		"index", "index-column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				to.el.Type((*scpb.IndexColumn)(nil)),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_BACKFILL_ONLY),
				currentStatus(to.node, scpb.Status_PUBLIC),
			}
		})
	// We need to make sure that no columns are added to the index after it
	// receives any data due to a backfill.
	registerDepRule("index-column added to index before index is backfilled",
		scgraph.Precedence,
		"index-column", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.IndexColumn)(nil)),
				to.el.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),

				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_PUBLIC),
				currentStatus(to.node, scpb.Status_BACKFILLED),
			}
		})
	registerDepRule("index-column added to index after temp index exists",
		scgraph.Precedence,
		"index", "index-column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.TemporaryIndex)(nil)),
				to.el.Type((*scpb.IndexColumn)(nil)),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				targetStatus(from.target, scpb.Transient),
				targetStatus(to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_PUBLIC),
			}
		})
	// We need to make sure that no columns are added to the temp index after it
	// receives any writes.
	registerDepRule("index-column added to index before temp index receives writes",
		scgraph.Precedence,
		"index-column", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.IndexColumn)(nil)),
				to.el.Type((*scpb.TemporaryIndex)(nil)),
				joinOnIndexID(from.el, to.el, "table-id", "index-id"),
				targetStatus(from.target, scpb.ToPublic),
				targetStatus(to.target, scpb.Transient),
				currentStatus(from.node, scpb.Status_PUBLIC),
				currentStatus(to.node, scpb.Status_WRITE_ONLY),
			}
		})

	// We need to ensure that the temporary index has all the relevant writes
	// to any columns it contains. We ensure elsewhere that any index which
	// will later be merged with the temporary index is not backfilled until
	// that temporary index is receiving writes. This rule ensures that those
	// write operations contain data for all columns.
	registerDepRule(
		"column is WRITE_ONLY before temporary index is WRITE_ONLY",
		scgraph.Precedence,
		"column", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type((*scpb.TemporaryIndex)(nil)),
				indexContainsColumn(to.el, from.el, "index-column", "table-id", "column-id", "index-id"),
				targetStatus(from.target, scpb.ToPublic),
				targetStatus(to.target, scpb.Transient),
				currentStatus(from.node, scpb.Status_WRITE_ONLY),
				currentStatus(to.node, scpb.Status_WRITE_ONLY),
			}
		},
	)

}
