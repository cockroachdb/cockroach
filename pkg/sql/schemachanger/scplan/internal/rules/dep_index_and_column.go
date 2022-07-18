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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// This registeredDepRule ensures that a new primary index becomes public right after the
// old primary index starts getting removed, effectively swapping one for the
// other.
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
					"new-primary-index-depends-on-old", to.el, from.el,
				)(func(add, drop *scpb.PrimaryIndex) bool {
					return add.SourceIndexID == drop.IndexID
				}),
			}
		},
	)

	registerDepRule(
		"reverting primary index swap",
		scgraph.SameStagePrecedence,
		"new-index", "old-index",
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
					"new-primary-index-depends-on-old", from.el, to.el,
				)(func(add, drop *scpb.PrimaryIndex) bool {
					return add.SourceIndexID == drop.IndexID
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

	indexDependents := screl.Schema.Def4("index-dependents",
		"index", "dep",
		"table-id", "index-id", func(
			index, dep, tableID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				dep.Type(
					(*scpb.IndexName)(nil),
					(*scpb.IndexPartitioning)(nil),
					(*scpb.SecondaryIndexPartial)(nil),
					(*scpb.IndexComment)(nil),
					(*scpb.IndexColumn)(nil),
				),
				index.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.TemporaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),

				joinOnIndexID(dep, index, "table-id", "index-id"),
			}
		})

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

// These rules ensure that column-dependent elements, like a column's name, its
// DEFAULT expression, etc. appear once the column reaches a suitable state.
// Vice-versa for column removal.
func init() {
	registerDepRule(
		"column name set right after column existence",
		scgraph.SameStagePrecedence,
		"column", "column-name",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type((*scpb.ColumnName)(nil)),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_PUBLIC),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
			}
		},
	)
	registerDepRule(
		"column existence precedes column dependents",
		scgraph.Precedence,
		"column", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type(
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
					(*scpb.ColumnComment)(nil),
					(*scpb.IndexColumn)(nil),
				),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"DEFAULT or ON UPDATE existence precedes writes to column",
		scgraph.Precedence,
		"expr", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type(
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
				),
				to.el.Type((*scpb.Column)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_PUBLIC),
				currentStatus(to.node, scpb.Status_WRITE_ONLY),
			}
		},
	)

	// TODO(ajwerner): Understand this rule and why it needs to exist.
	registerDepRule(
		"column named before column type becomes public",
		scgraph.Precedence,
		"column-name", "column-type",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.ColumnName)(nil)),
				to.el.Type((*scpb.ColumnType)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatusEq(from.node, to.node, scpb.Status_PUBLIC),
			}
		},
	)

	// The comment is all that is remaining column dependents now that the name
	// and the DEFAULT and ON UPDATE expressions have already been dealt with.
	registerDepRule(
		"column comment exists before column becomes public",
		scgraph.Precedence,
		"column-comment", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.ColumnComment)(nil)),
				to.el.Type((*scpb.Column)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatusEq(from.node, to.node, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"column dependents removed after column no longer public",
		scgraph.Precedence,
		"column", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type(
					(*scpb.ColumnType)(nil),
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnComment)(nil),
				),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatus(from.node, scpb.Status_WRITE_ONLY),
				currentStatus(to.node, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"column type dependents removed right before column type",
		scgraph.SameStagePrecedence,
		"dependent", "column-type",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type(
					(*scpb.SequenceOwner)(nil),
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
				),
				to.el.Type((*scpb.ColumnType)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"dependents removed before column",
		scgraph.Precedence,
		"dependent", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type(
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnType)(nil),
					(*scpb.ColumnComment)(nil),
				),
				to.el.Type((*scpb.Column)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),
			}
		},
	)

}

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

var indexContainsColumn = screl.Schema.Def6(
	"indexContainsColumn",
	"index", "column", "index-column", "table-id", "column-id", "index-id", func(
		index, column, indexColumn, tableID, columnID, indexID rel.Var,
	) rel.Clauses {
		return rel.Clauses{
			index.AttrEqVar(screl.IndexID, indexID),
			indexColumn.Type((*scpb.IndexColumn)(nil)),
			indexColumn.AttrEqVar(screl.DescID, rel.Blank),
			joinOnColumnID(column, indexColumn, tableID, columnID),
			joinOnIndexID(index, indexColumn, tableID, indexID),
		}
	})

// These rules ensure that columns and indexes containing these columns
// appear into existence in the correct order.
func init() {

	sourceIndexNotSet := screl.Schema.Def1("sourceIndexNotSet", "index", func(
		index rel.Var,
	) rel.Clauses {
		return rel.Clauses{
			index.AttrNeq(screl.SourceIndexID, catid.IndexID(0)),
		}
	})
	columnInPrimaryIndexSwap := screl.Schema.Def6(
		"columnInPrimaryIndexSwap",
		"index", "column", "index-column", "table-id", "column-id", "index-id", func(
			index, column, indexColumn, tableID, columnID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				indexContainsColumn(
					index, column, indexColumn, tableID, columnID, indexID,
				),
				sourceIndexNotSet(index),
			}
		})
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
	// We want to say that all columns which are part of a secondary index need
	// to be in a primary index which is validated
	// To do that, we want to find a secondary index which has a source which
	// is a primary index which is itself new. Then we want to find
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

	// This is a pair of somewhat brute-force hack to ensure that we only create
	// a single GC job for all the indexes of a table being dropped by a
	// transaction.
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

// This rule ensures that columns depend on each other in increasing order.
func init() {
	registerDepRule(
		"ensure columns are in increasing order",
		scgraph.SameStagePrecedence,
		"later-column", "earlier-column",
		func(from, to nodeVars) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type((*scpb.Column)(nil)),
				join(from.el, to.el, screl.DescID, "table-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				status.In(scpb.Status_WRITE_ONLY, scpb.Status_PUBLIC),
				status.Entities(screl.CurrentStatus, from.node, to.node),
				rel.Filter("columnHasSmallerID", from.el, to.el)(func(
					from *scpb.Column, to *scpb.Column,
				) bool {
					return from.ColumnID < to.ColumnID
				}),
			}
		})
}
