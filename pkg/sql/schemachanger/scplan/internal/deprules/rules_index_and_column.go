// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package deprules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

func init() {
	addIdx, addTarget, addNode := targetNodeVars("adding-index")
	dropIdx, dropTarget, dropNode := targetNodeVars("dropping-index")
	var id rel.Var = "id"
	primaryIndexReferenceEachOther := screl.MustQuery(
		addIdx.Type((*scpb.PrimaryIndex)(nil)),
		dropIdx.Type((*scpb.PrimaryIndex)(nil)),
		id.Entities(screl.DescID, addIdx, dropIdx),

		rel.Filter(
			"indexes-reference-each-other", addIdx, dropIdx,
		)(func(add, drop *scpb.PrimaryIndex) bool {
			return add.IndexID != drop.IndexID
		}),

		joinTargetNode(addIdx, addTarget, scpb.Status_PUBLIC, addNode, scpb.Status_PUBLIC),
		joinTargetNode(dropIdx, dropTarget, scpb.Status_ABSENT, dropNode, scpb.Status_VALIDATED),
	)

	register(
		"primary index add depends on drop",
		scgraph.SameStagePrecedence,
		dropNode, addNode,
		primaryIndexReferenceEachOther,
	)
}

func init() {
	depRule(
		"index existence precedes partitioning",
		scgraph.Precedence,
		scpb.Status_PUBLIC,
		element(scpb.Status_DELETE_ONLY, (*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
		element(scpb.Status_PUBLIC, (*scpb.IndexPartitioning)(nil)),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"index partitioning precedes allowing writes",
		scgraph.Precedence,
		scpb.Status_PUBLIC,
		element(scpb.Status_PUBLIC, (*scpb.IndexPartitioning)(nil)),
		element(scpb.Status_DELETE_AND_WRITE_ONLY, (*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
		screl.DescID,
		screl.IndexID,
	).register()
}

func init() {
	depRule(
		"index existence precedes naming",
		scgraph.Precedence,
		scpb.Status_PUBLIC,
		element(scpb.Status_DELETE_ONLY, (*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
		element(scpb.Status_PUBLIC, (*scpb.IndexName)(nil)),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"index named right before index becomes public",
		scgraph.SameStagePrecedence,
		scpb.Status_PUBLIC,
		element(scpb.Status_PUBLIC, (*scpb.IndexName)(nil)),
		element(scpb.Status_PUBLIC, (*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"index unnamed after index no longer public",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_VALIDATED, (*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
		element(scpb.Status_ABSENT, (*scpb.IndexName)(nil)),
		screl.DescID,
		screl.IndexID,
	).register()

	depRule(
		"index unnamed before index no longer exists",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_ABSENT, (*scpb.IndexName)(nil)),
		element(scpb.Status_ABSENT, (*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
		screl.DescID,
		screl.IndexID,
	).register()
}

func init() {
	depRule(
		"column must come into existence before being named",
		scgraph.Precedence,
		scpb.Status_PUBLIC,
		element(scpb.Status_DELETE_ONLY, (*scpb.Column)(nil)),
		element(scpb.Status_PUBLIC, (*scpb.ColumnName)(nil)),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"column named right before column becomes public",
		scgraph.SameStagePrecedence,
		scpb.Status_PUBLIC,
		element(scpb.Status_PUBLIC, (*scpb.ColumnName)(nil)),
		element(scpb.Status_PUBLIC, (*scpb.Column)(nil)),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"column unnamed after column no longer public",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_DELETE_AND_WRITE_ONLY, (*scpb.Column)(nil)),
		element(scpb.Status_ABSENT, (*scpb.ColumnName)(nil)),
		screl.DescID,
		screl.ColumnID,
	).register()

	depRule(
		"column unnamed before column no longer exists",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_ABSENT, (*scpb.ColumnName)(nil)),
		element(scpb.Status_ABSENT, (*scpb.Column)(nil)),
		screl.DescID,
		screl.ColumnID,
	).register()
}

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
	var id, status, targetStatus rel.Var = "id", "status", "target-status"
	register(
		"column depends on indexes",
		scgraph.Precedence,
		indexNode, columnNode,
		screl.MustQuery(
			status.In(scpb.Status_DELETE_AND_WRITE_ONLY, scpb.Status_PUBLIC),
			targetStatus.Eq(scpb.Status_PUBLIC),

			column.Type((*scpb.Column)(nil)),
			index.Type((*scpb.PrimaryIndex)(nil)),

			id.Entities(screl.DescID, column, index),

			rel.Filter("columnInIndex", column, index)(columnInIndex),

			targetStatus.Entities(screl.TargetStatus, columnTarget, indexTarget),
			status.Entities(screl.CurrentStatus, columnNode, indexNode),

			screl.JoinTargetNode(column, columnTarget, columnNode),
			screl.JoinTargetNode(index, indexTarget, indexNode),
		),
	)

	depRule(
		"column existence precedes index existence",
		scgraph.Precedence,
		scpb.Status_PUBLIC,
		element(scpb.Status_DELETE_ONLY, (*scpb.Column)(nil)),
		element(scpb.Status_DELETE_ONLY, (*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
		screl.DescID,
	).withFilter("column-featured-in-index", columnInIndex).register()

}
