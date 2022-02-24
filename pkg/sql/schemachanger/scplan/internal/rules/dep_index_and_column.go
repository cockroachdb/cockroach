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
	newIndex, newIndexTarget, newIndexNode := targetNodeVars("new-index")
	oldIndex, oldIndexTarget, oldIndexNode := targetNodeVars("old-index")
	var tableID rel.Var = "table-id"

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		oldIndexNode, newIndexNode,
		screl.MustQuery(
			newIndex.Type((*scpb.PrimaryIndex)(nil)),
			oldIndex.Type((*scpb.PrimaryIndex)(nil)),
			tableID.Entities(screl.DescID, newIndex, oldIndex),

			rel.Filter(
				"new-primary-index-depends-on-old", newIndex, oldIndex,
			)(func(add, drop *scpb.PrimaryIndex) bool {
				return add.SourceIndexID == drop.IndexID
			}),

			screl.JoinTargetNode(newIndex, newIndexTarget, newIndexNode),
			newIndexTarget.AttrEq(screl.TargetStatus, scpb.Status_PUBLIC),
			newIndexNode.AttrEq(screl.CurrentStatus, scpb.Status_PUBLIC),

			screl.JoinTargetNode(oldIndex, oldIndexTarget, oldIndexNode),
			oldIndexTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			oldIndexNode.AttrEq(screl.CurrentStatus, scpb.Status_VALIDATED),
		),
	)
}

// These rules ensure that index-dependent elements, like an index's name, its
// partitioning, etc. appear once the index reaches a suitable state.
// Vice-versa for index removal.
func init() {
	depRule(
		"index existence precedes index dependents",
		scgraph.Precedence,
		scpb.ToPublic,
		element(scpb.Status_DELETE_ONLY,
			(*scpb.PrimaryIndex)(nil),
			(*scpb.SecondaryIndex)(nil),
		),
		element(scpb.Status_PUBLIC,
			(*scpb.IndexName)(nil),
			(*scpb.IndexPartitioning)(nil),
			(*scpb.IndexComment)(nil),
		),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"partial predicate set right after secondary index existence",
		scgraph.SameStagePrecedence,
		scpb.ToPublic,
		element(scpb.Status_DELETE_ONLY,
			(*scpb.SecondaryIndex)(nil),
		),
		element(scpb.Status_PUBLIC,
			(*scpb.SecondaryIndexPartial)(nil),
		),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"dependents existence precedes writes to index",
		scgraph.Precedence,
		scpb.ToPublic,
		element(scpb.Status_PUBLIC,
			(*scpb.IndexPartitioning)(nil),
			(*scpb.IndexComment)(nil),
		),
		element(scpb.Status_WRITE_ONLY,
			(*scpb.PrimaryIndex)(nil),
			(*scpb.SecondaryIndex)(nil),
		),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"index named right before index becomes public",
		scgraph.SameStagePrecedence,
		scpb.ToPublic,
		element(scpb.Status_PUBLIC,
			(*scpb.IndexName)(nil),
		),
		element(scpb.Status_PUBLIC,
			(*scpb.PrimaryIndex)(nil),
			(*scpb.SecondaryIndex)(nil),
		),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"dependents removed after index no longer public",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_VALIDATED,
			(*scpb.PrimaryIndex)(nil),
			(*scpb.SecondaryIndex)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.IndexName)(nil),
			(*scpb.IndexPartitioning)(nil),
			(*scpb.SecondaryIndexPartial)(nil),
			(*scpb.IndexComment)(nil),
		),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"dependents removed before index",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.IndexName)(nil),
			(*scpb.IndexPartitioning)(nil),
			(*scpb.SecondaryIndexPartial)(nil),
			(*scpb.IndexComment)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.PrimaryIndex)(nil),
			(*scpb.SecondaryIndex)(nil),
		),
		screl.DescID,
		screl.IndexID,
	).register()
}

// These rules ensure that column-dependent elements, like a column's name, its
// DEFAULT expression, etc. appear once the column reaches a suitable state.
// Vice-versa for column removal.
func init() {
	depRule(
		"column type set right after column existence",
		scgraph.SameStagePrecedence,
		scpb.ToPublic,
		element(scpb.Status_DELETE_ONLY,
			(*scpb.Column)(nil),
		),
		element(scpb.Status_PUBLIC,
			(*scpb.ColumnType)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"column existence precedes column dependents",
		scgraph.Precedence,
		scpb.ToPublic,
		element(scpb.Status_DELETE_ONLY,
			(*scpb.Column)(nil),
		),
		element(scpb.Status_PUBLIC,
			(*scpb.ColumnName)(nil),
			(*scpb.ColumnDefaultExpression)(nil),
			(*scpb.ColumnOnUpdateExpression)(nil),
			(*scpb.ColumnComment)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"DEFAULT or ON UPDATE existence precedes writes to column",
		scgraph.Precedence,
		scpb.ToPublic,
		element(scpb.Status_PUBLIC,
			(*scpb.ColumnDefaultExpression)(nil),
			(*scpb.ColumnOnUpdateExpression)(nil),
		),
		element(scpb.Status_WRITE_ONLY,
			(*scpb.Column)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"column named right before column becomes public",
		scgraph.SameStagePrecedence,
		scpb.ToPublic,
		element(scpb.Status_PUBLIC,
			(*scpb.ColumnName)(nil),
		),
		element(scpb.Status_PUBLIC,
			(*scpb.Column)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"column dependents exist before column becomes public",
		scgraph.Precedence,
		scpb.ToPublic,
		element(scpb.Status_PUBLIC,
			// These are all remaining column dependents now that the name and the
			// DEFAULT and ON UPDATE expressions have already been dealt with.
			(*scpb.ColumnComment)(nil),
		),
		element(scpb.Status_PUBLIC,
			(*scpb.Column)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"column dependents removed after column no longer public",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_WRITE_ONLY,
			(*scpb.Column)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.ColumnType)(nil),
			(*scpb.ColumnName)(nil),
			(*scpb.ColumnComment)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"column type dependents removed right before column type",
		scgraph.SameStagePrecedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.SequenceOwner)(nil),
			(*scpb.ColumnDefaultExpression)(nil),
			(*scpb.ColumnOnUpdateExpression)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.ColumnType)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"dependents removed before column",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.ColumnName)(nil),
			(*scpb.ColumnComment)(nil),
			(*scpb.ColumnType)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.Column)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).register()
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

	depRule(
		"column type removed right before column when not dropping relation",
		scgraph.SameStagePrecedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.ColumnType)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.Column)(nil),
		),
		screl.DescID,
		screl.ColumnID,
	).withFilter("parent-relation-is-not-dropped", func(ct *scpb.ColumnType, _ *scpb.Column) bool {
		return !ct.IsRelationBeingDropped
	}).register()

	depRule(
		"partial predicate removed right before secondary index when not dropping relation",
		scgraph.SameStagePrecedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.SecondaryIndexPartial)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.SecondaryIndex)(nil),
		),
		screl.DescID,
		screl.IndexID,
	).withFilter("parent-relation-is-not-dropped", func(ip *scpb.SecondaryIndexPartial, _ *scpb.SecondaryIndex) bool {
		return !ip.IsRelationBeingDropped
	}).register()
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
	columnInPrimaryIndex := func(from *scpb.Column, to scpb.Element) bool {
		switch to := to.(type) {
		case *scpb.PrimaryIndex:
			if columnInList(from.ColumnID, to.KeyColumnIDs) ||
				columnInList(from.ColumnID, to.StoringColumnIDs) ||
				columnInList(from.ColumnID, to.KeySuffixColumnIDs) {
				return true
			}
		}
		return false
	}
	columnInSecondaryIndex := func(from *scpb.Column, to scpb.Element) bool {
		switch to := to.(type) {
		case *scpb.SecondaryIndex:
			if columnInList(from.ColumnID, to.KeyColumnIDs) ||
				columnInList(from.ColumnID, to.StoringColumnIDs) ||
				columnInList(from.ColumnID, to.KeySuffixColumnIDs) {
				return true
			}
		}
		return false
	}
	columnInIndex := func(from *scpb.Column, to scpb.Element) bool {
		return columnInPrimaryIndex(from, to) || columnInSecondaryIndex(from, to)
	}

	column, columnTarget, columnNode := targetNodeVars("column")
	index, indexTarget, indexNode := targetNodeVars("index")
	var tableID, status, targetStatus rel.Var = "table-id", "status", "target-status"
	registerDepRule(
		"column depends on primary index",
		scgraph.Precedence,
		indexNode, columnNode,
		screl.MustQuery(
			status.In(scpb.Status_WRITE_ONLY, scpb.Status_PUBLIC),
			targetStatus.Eq(scpb.Status_PUBLIC),

			column.Type((*scpb.Column)(nil)),
			index.Type((*scpb.PrimaryIndex)(nil)),

			tableID.Entities(screl.DescID, column, index),

			rel.Filter("column-featured-in-index", column, index)(columnInIndex),

			targetStatus.Entities(screl.TargetStatus, columnTarget, indexTarget),
			status.Entities(screl.CurrentStatus, columnNode, indexNode),

			screl.JoinTargetNode(column, columnTarget, columnNode),
			screl.JoinTargetNode(index, indexTarget, indexNode),
		),
	)

	depRule(
		"column existence precedes index existence",
		scgraph.Precedence,
		scpb.ToPublic,
		element(scpb.Status_DELETE_ONLY,
			(*scpb.Column)(nil),
		),
		element(scpb.Status_DELETE_ONLY,
			(*scpb.PrimaryIndex)(nil),
			(*scpb.SecondaryIndex)(nil),
		),
		screl.DescID,
	).withFilter("column-featured-in-index", columnInIndex).register()

}
