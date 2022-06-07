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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// This registeredDepRule ensures that a new primary index becomes public right after the
// old primary index starts getting removed, effectively swapping one for the
// other.
func init() {
	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"old-index", "new-index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				targetStatus(fromTarget, scpb.ToAbsent),
				targetStatus(toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_VALIDATED),
				currentStatus(toNode, scpb.Status_PUBLIC),
				rel.Filter(
					"new-primary-index-depends-on-old", to, from,
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
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				join(from, to, screl.DescID, "table-id"),
				targetStatus(fromTarget, scpb.ToAbsent),
				targetStatus(toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_VALIDATED),
				currentStatus(toNode, scpb.Status_PUBLIC),
				rel.Filter(
					"new-primary-index-depends-on-old", from, to,
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
		"index existence precedes index dependents",
		scgraph.Precedence,
		"index", "index-dependent",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				to.Type(
					(*scpb.IndexName)(nil),
					(*scpb.IndexPartitioning)(nil),
					(*scpb.IndexComment)(nil),
				),
				joinOnIndexID(from, to, "table-id", "index-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_BACKFILL_ONLY),
				currentStatus(toNode, scpb.Status_PUBLIC),
			}
		})

	registerDepRule(
		"partitioning set right after temp index existence",
		scgraph.SameStagePrecedence,
		"temp-index", "index-partitioning",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.IndexPartitioning)(nil)),
				joinOnIndexID(from, to, "table-id", "index-id"),
				targetStatus(fromTarget, scpb.Transient),
				targetStatus(toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_DELETE_ONLY),
				currentStatus(toNode, scpb.Status_PUBLIC),
			}
		})

	registerDepRule(
		"partial predicate set right after secondary index existence",
		scgraph.SameStagePrecedence,
		"index", "index-predicate",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.SecondaryIndexPartial)(nil)),
				joinOnIndexID(from, to, "table-id", "index-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_BACKFILL_ONLY),
				currentStatus(to, scpb.Status_PUBLIC),
			}
		})

	registerDepRule(
		"dependents existence precedes writes to index",
		scgraph.Precedence,
		"child", "index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.IndexPartitioning)(nil),
					(*scpb.IndexComment)(nil),
				),
				to.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				joinOnIndexID(from, to, "table-id", "index-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_PUBLIC),
				currentStatus(toNode, scpb.Status_WRITE_ONLY),
			}
		},
	)
	registerDepRule(
		"index named right before index becomes public",
		scgraph.SameStagePrecedence,
		"index-name", "index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexName)(nil)),
				to.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatusEq(fromNode, toNode, scpb.Status_PUBLIC),
				joinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)

	registerDepRule(
		"dependents removed after index no longer public",
		scgraph.SameStagePrecedence,
		"child", "index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.IndexName)(nil),
					(*scpb.IndexPartitioning)(nil),
					(*scpb.SecondaryIndexPartial)(nil),
					(*scpb.IndexComment)(nil),
				),
				to.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				joinOnIndexID(from, to, "table-id", "index-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_VALIDATED),
			}
		},
	)

	registerDepRule(
		"dependents removed before index",
		scgraph.Precedence,
		"dependent", "index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.IndexName)(nil),
					(*scpb.IndexPartitioning)(nil),
					(*scpb.SecondaryIndexPartial)(nil),
					(*scpb.IndexComment)(nil),
				),
				to.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				joinOnIndexID(from, to, "table-id", "index-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_ABSENT),
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
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from, to, "desc-id"),
				joinOn(from, screl.IndexID, to, screl.TemporaryIndexID, "temp-index-id"),
				targetStatus(fromTarget, scpb.Transient),
				targetStatus(toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_WRITE_ONLY),
				currentStatus(toNode, scpb.Status_BACKFILLED),
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
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.ColumnName)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_DELETE_ONLY),
				currentStatus(toNode, scpb.Status_PUBLIC),
				joinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)
	registerDepRule(
		"column existence precedes column dependents",
		scgraph.Precedence,
		"column", "dependent",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type(
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
					(*scpb.ColumnComment)(nil),
				),
				joinOnColumnID(from, to, "table-id", "col-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_DELETE_ONLY),
				currentStatus(toNode, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"DEFAULT or ON UPDATE existence precedes writes to column",
		scgraph.Precedence,
		"expr", "column",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
				),
				to.Type((*scpb.Column)(nil)),
				joinOnColumnID(from, to, "table-id", "col-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_PUBLIC),
				currentStatus(toNode, scpb.Status_WRITE_ONLY),
			}
		},
	)

	registerDepRule(
		"column named right before column type becomes public",
		scgraph.SameStagePrecedence,
		"column-name", "column-type",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnName)(nil)),
				to.Type((*scpb.ColumnType)(nil)),
				joinOnColumnID(from, to, "table-id", "col-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatusEq(fromNode, toNode, scpb.Status_PUBLIC),
			}
		},
	)

	// The comment is all that is remaining column dependents now that the name
	// and the DEFAULT and ON UPDATE expressions have already been dealt with.
	registerDepRule(
		"column comment exists before column becomes public",
		scgraph.Precedence,
		"column-comment", "column",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnComment)(nil)),
				to.Type((*scpb.Column)(nil)),
				joinOnColumnID(from, to, "table-id", "col-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatusEq(fromNode, toNode, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"column dependents removed after column no longer public",
		scgraph.Precedence,
		"column", "dependent",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type(
					(*scpb.ColumnType)(nil),
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnComment)(nil),
				),
				joinOnColumnID(from, to, "table-id", "col-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_WRITE_ONLY),
				currentStatus(toNode, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"column type dependents removed right before column type",
		scgraph.SameStagePrecedence,
		"dependent", "column-type",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.SequenceOwner)(nil),
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
				),
				to.Type((*scpb.ColumnType)(nil)),
				joinOnColumnID(from, to, "table-id", "col-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"dependents removed before column",
		scgraph.Precedence,
		"dependent", "column",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnType)(nil),
					(*scpb.ColumnComment)(nil),
				),
				to.Type((*scpb.Column)(nil)),
				joinOnColumnID(from, to, "table-id", "col-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_ABSENT),
			}
		},
	)

}

// Special cases for removal of column types and index partial predicates,
// which hold references to other descriptors.
//
// When the whole table is dropped, we can (and in fact, should) remove these
// right away pre-commit. However, when only the column (or the index) is
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
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnType)(nil)),
				to.Type((*scpb.Column)(nil)),
				joinOnColumnID(from, to, "table-id", "col-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_ABSENT),
				rel.Filter("columnTypeIsNotBeingDropped", from)(func(
					ct *scpb.ColumnType,
				) bool {
					return !ct.IsRelationBeingDropped
				}),
			}
		},
	)

	registerDepRule(
		"partial predicate removed right before secondary index when not dropping relation",
		scgraph.SameStagePrecedence,
		"partial-predicate", "index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndexPartial)(nil)),
				to.Type((*scpb.SecondaryIndex)(nil)),
				joinOnIndexID(from, to, "table-id", "index-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_ABSENT),
				rel.Filter("secondaryIndexPartialIsNotBeingDropped", from)(func(
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
	columnInList := func(targetColumn descpb.ColumnID, columnList descpb.ColumnIDs) bool {
		for _, column := range columnList {
			if targetColumn == column {
				return true
			}
		}
		return false
	}
	columnInIndex := func(from *scpb.Column, to scpb.Element) bool {
		var idx *scpb.Index
		switch to := to.(type) {
		case *scpb.PrimaryIndex:
			idx = &to.Index
		case *scpb.SecondaryIndex:
			idx = &to.Index
		case *scpb.TemporaryIndex:
			idx = &to.Index
		}
		if idx == nil {
			return false
		}
		return columnInList(from.ColumnID, idx.KeyColumnIDs) ||
			columnInList(from.ColumnID, idx.StoringColumnIDs) ||
			columnInList(from.ColumnID, idx.KeySuffixColumnIDs)
	}
	columnInPrimaryIndexSwap := func(from *scpb.Column, to *scpb.PrimaryIndex) bool {
		return columnInIndex(from, to) && to.SourceIndexID != 0
	}

	registerDepRule(
		"column depends on primary index",
		scgraph.Precedence,
		"index", "column",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			var status rel.Var = "status"
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				join(from, to, screl.DescID, "table-id"),
				rel.Filter("columnFeaturedInIndex", to, from)(columnInIndex),
				status.In(scpb.Status_WRITE_ONLY, scpb.Status_PUBLIC),
				status.Entities(screl.CurrentStatus, fromNode, toNode),
			}
		},
	)
	registerDepRule(
		"primary index should be cleaned up before newly added column when reverting",
		scgraph.Precedence,
		"index", "column",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			var status rel.Var = "status"
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				join(from, to, screl.DescID, "table-id"),
				rel.Filter("columnFeaturedInIndex", to, from)(columnInPrimaryIndexSwap),
				status.Eq(scpb.Status_WRITE_ONLY),
				status.Entities(screl.CurrentStatus, fromNode, toNode),
			}
		})
	registerDepRule(
		"column existence precedes index existence",
		scgraph.Precedence,
		"column", "index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_DELETE_ONLY),
				currentStatus(toNode, scpb.Status_BACKFILL_ONLY),
				join(from, to, screl.DescID, "table-id"),
				rel.Filter("columnFeaturedInIndex", from, to)(columnInIndex),
			}
		},
	)

	registerDepRule(
		"column existence precedes temporary index existence",
		scgraph.Precedence,
		"column", "temp-index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				join(from, to, screl.DescID, "table-id"),
				targetStatus(fromTarget, scpb.ToPublic),
				targetStatus(toTarget, scpb.Transient),
				currentStatusEq(fromNode, toNode, scpb.Status_DELETE_ONLY),
				rel.Filter("columnFeaturedInIndex", from, to)(columnInIndex),
			}
		},
	)
	primaryIndexHasSecondaryColumns := func(from *scpb.PrimaryIndex, to scpb.Element) bool {
		switch to := to.(type) {
		case *scpb.SecondaryIndex:
			for _, colID := range from.StoringColumnIDs {
				if columnInList(colID, to.KeyColumnIDs) ||
					columnInList(colID, to.StoringColumnIDs) ||
					columnInList(colID, to.KeySuffixColumnIDs) {
					return true
				}
			}
		case *scpb.TemporaryIndex:
			if !to.IsUsingSecondaryEncoding {
				return false
			}
			for _, colID := range from.StoringColumnIDs {
				if columnInList(colID, to.KeyColumnIDs) ||
					columnInList(colID, to.StoringColumnIDs) ||
					columnInList(colID, to.KeySuffixColumnIDs) {
					return true
				}
			}
		}
		return false
	}
	secondaryIndexHasPrimarySwapColumns := func(to scpb.Element, from *scpb.PrimaryIndex) bool {
		if from.SourceIndexID == 0 {
			return false
		}
		switch to := to.(type) {
		case *scpb.SecondaryIndex:
			for _, colID := range from.StoringColumnIDs {
				if columnInList(colID, to.KeyColumnIDs) ||
					columnInList(colID, to.StoringColumnIDs) ||
					columnInList(colID, to.KeySuffixColumnIDs) {
					return true
				}
			}
		case *scpb.TemporaryIndex:
			if !to.IsUsingSecondaryEncoding {
				return false
			}
			for _, colID := range from.StoringColumnIDs {
				if columnInList(colID, to.KeyColumnIDs) ||
					columnInList(colID, to.StoringColumnIDs) ||
					columnInList(colID, to.KeySuffixColumnIDs) {
					return true
				}
			}
		}
		return false
	}
	registerDepRule(
		"primary index with new columns should exist before secondary/temp indexes",
		scgraph.Precedence,
		"primary-index", "second-index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.SecondaryIndex)(nil), (*scpb.TemporaryIndex)(nil)),
				join(from, to, screl.DescID, "table-id"),
				targetStatus(fromTarget, scpb.ToPublic),
				targetStatus(toTarget, scpb.ToPublic),
				currentStatus(fromNode, scpb.Status_VALIDATED),
				currentStatus(toNode, scpb.Status_BACKFILL_ONLY),
				rel.Filter("newColumnFeaturedInIndex", from, to)(primaryIndexHasSecondaryColumns),
			}
		})
	registerDepRule(
		"secondary indexes should be cleaned up before any primary index with columns when reverting",
		scgraph.Precedence,
		"second-index", "primary-index",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil), (*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				join(from, to, screl.DescID, "table-id"),
				targetStatus(fromTarget, scpb.ToAbsent),
				targetStatus(toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_VALIDATED),
				rel.Filter("newColumnFeaturedInIndex", from, to)(secondaryIndexHasPrimarySwapColumns),
			}
		})
}

// This rule ensures that columns depend on each other in increasing order.
func init() {
	registerDepRule(
		"ensure columns are in increasing order",
		scgraph.SameStagePrecedence,
		"later-column", "earlier-column",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.Column)(nil)),
				join(from, to, screl.DescID, "table-id"),
				targetStatusEq(fromTarget, toTarget, scpb.ToPublic),
				status.In(scpb.Status_WRITE_ONLY, scpb.Status_PUBLIC),
				status.Entities(screl.CurrentStatus, fromNode, toNode),
				rel.Filter("columnHasSmallerID", from, to)(func(
					from *scpb.Column, to *scpb.Column,
				) bool {
					return from.ColumnID < to.ColumnID
				}),
			}
		})
}
