// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/errors"
)

func targetNodeVars(el rel.Var) (element, target, node rel.Var) {
	return el, el + "-target", el + "-node"
}

func joinTargetNode(element, target, node rel.Var, targetStatus, status scpb.Status) rel.Clause {
	return rel.And(
		screl.JoinTargetNode(element, target, node),
		target.AttrEq(screl.TargetStatus, targetStatus),
		node.AttrEq(screl.CurrentStatus, status),
	)
}

func init() {
	var (
		parent, parentTarget, parentNode = targetNodeVars("parent")
		other, otherTarget, otherNode    = targetNodeVars("other")
	)
	// Ensures that child objects under a database or schema
	// are dropped before any children are dealt with.
	register(
		"parent dependencies",
		scgraph.Precedence,
		otherNode, parentNode,
		screl.MustQuery(
			parent.Type((*scpb.Database)(nil), (*scpb.Schema)(nil)),
			other.Type(
				(*scpb.Type)(nil), (*scpb.Table)(nil), (*scpb.View)(nil), (*scpb.Sequence)(nil),
				(*scpb.Schema)(nil),
				// TODO(ajwerner): Should sequence be here if the parent is a database?
				// There was code in the old rules to have sequence but then it was
				// actually just duplicating the type case.
			),

			rel.Filter(
				"parentDependsOn",
				parent, other,
			)(func(parent, other scpb.Element) bool {
				var dependentObjects []descpb.ID
				switch parent := parent.(type) {
				case *scpb.Database:
					dependentObjects = parent.DependentObjects
				case *scpb.Schema:
					dependentObjects = parent.DependentObjects
				default:
					panic(errors.AssertionFailedf("invalid parent type %T", parent))
				}
				return idInIDs(dependentObjects, screl.GetDescID(other))
			}),

			screl.JoinTargetNode(parent, parentTarget, parentNode),
			parentTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			parentNode.AttrEq(screl.CurrentStatus, scpb.Status_ABSENT),

			joinTargetNode(other, otherTarget, otherNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
		),
	)
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

	register(
		"index existence depends on column existence",
		scgraph.Precedence,
		columnNode, indexNode,
		screl.MustQuery(
			column.Type((*scpb.Column)(nil)),
			index.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),

			id.Entities(screl.DescID, column, index),

			rel.Filter("columnInIndex", column, index)(columnInIndex),

			joinTargetNode(column, columnTarget, columnNode, scpb.Status_PUBLIC, scpb.Status_DELETE_ONLY),
			joinTargetNode(index, indexTarget, indexNode, scpb.Status_PUBLIC, scpb.Status_DELETE_ONLY),
		),
	)
}

func init() {
	addIdx, addTarget, addNode := targetNodeVars("add-idx")
	dropIdx, dropTarget, dropNode := targetNodeVars("drop-idx")
	var id rel.Var = "id"
	primaryIndexReferenceEachOther := screl.MustQuery(
		addIdx.Type((*scpb.PrimaryIndex)(nil)),
		dropIdx.Type((*scpb.PrimaryIndex)(nil)),
		id.Entities(screl.DescID, addIdx, dropIdx),

		rel.Filter(
			"referenceEachOther", addIdx, dropIdx,
		)(func(add, drop *scpb.PrimaryIndex) bool {
			return add.IndexID != drop.IndexID
		}),

		joinTargetNode(addIdx, addTarget, addNode,
			scpb.Status_PUBLIC, scpb.Status_PUBLIC),
		joinTargetNode(dropIdx, dropTarget, dropNode,
			scpb.Status_ABSENT, scpb.Status_VALIDATED),
	)

	register(
		"primary index add depends on drop",
		scgraph.SameStagePrecedence,
		dropNode, addNode,
		primaryIndexReferenceEachOther,
	)
}

func init() {
	addIdx, addTarget, addNode := targetNodeVars("add-idx")
	partitioning, partitioningTarget, partitioningNode := targetNodeVars("partitioning")
	var id rel.Var = "id"
	var indexID rel.Var = "index-id"

	register(
		"partitioning information needs the basic index as created",
		scgraph.Precedence,
		addNode, partitioningNode,
		screl.MustQuery(
			addIdx.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
			partitioning.Type((*scpb.Partitioning)(nil)),
			id.Entities(screl.DescID, addIdx, partitioning),
			indexID.Entities(screl.IndexID, addIdx, partitioning),

			joinTargetNode(addIdx, addTarget, addNode,
				scpb.Status_PUBLIC, scpb.Status_DELETE_ONLY),
			joinTargetNode(partitioning, partitioningTarget, partitioningNode,
				scpb.Status_PUBLIC, scpb.Status_PUBLIC),
		),
	)
}

func init() {
	addIdx, addTarget, addNode := targetNodeVars("add-idx")
	partitioning, partitioningTarget, partitioningNode := targetNodeVars("partitioning")
	var id rel.Var = "id"

	register(
		"index needs partitioning information to be filled",
		scgraph.Precedence,
		addNode, partitioningNode,
		screl.MustQuery(
			addIdx.Type((*scpb.PrimaryIndex)(nil)),
			partitioning.Type((*scpb.Partitioning)(nil)),
			id.Entities(screl.DescID, addIdx, partitioning),
			id.Entities(screl.IndexID, addIdx, partitioning),

			joinTargetNode(addIdx, addTarget, addNode,
				scpb.Status_PUBLIC, scpb.Status_DELETE_AND_WRITE_ONLY),
			joinTargetNode(partitioning, partitioningTarget, partitioningNode,
				scpb.Status_PUBLIC, scpb.Status_PUBLIC),
		),
	)
}

func init() {
	depNeedsRelationToExitSynthDrop := func(ruleName string, depTypes []interface{}, depDescIDMatch rel.Attr) {
		// Before any parts of a relation/type can be dropped, the relation
		// should exit the synthetic drop state.
		relation, relationTarget, relationNode := targetNodeVars("relation")
		dep, depTarget, depNode := targetNodeVars("dep")
		var id rel.Var = "id"
		register(
			"dependency needs relation/type as non-synthetically dropped",
			scgraph.SameStagePrecedence,
			relationNode, depNode,
			screl.MustQuery(

				relation.Type((*scpb.Table)(nil), (*scpb.View)(nil), (*scpb.Sequence)(nil), (*scpb.Type)(nil)),
				dep.Type(depTypes[0], depTypes[1:]...),

				relation.AttrEqVar(screl.DescID, id),
				dep.AttrEqVar(depDescIDMatch, id),
				joinTargetNode(relation, relationTarget, relationNode, scpb.Status_ABSENT, scpb.Status_DROPPED),
				joinTargetNode(dep, depTarget, depNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
			),
		)
	}
	depNeedsRelationToExitSynthDrop("dependency needs relation/type as non-synthetically dropped",
		[]interface{}{(*scpb.DefaultExpression)(nil), (*scpb.RelationDependedOnBy)(nil),
			(*scpb.SequenceOwnedBy)(nil), (*scpb.ForeignKey)(nil)},
		screl.DescID)

	depNeedsRelationToExitSynthDrop("dependency (ref desc) needs relation/type as non-synthetically dropped",
		[]interface{}{(*scpb.ForeignKeyBackReference)(nil), (*scpb.RelationDependedOnBy)(nil),
			(*scpb.ViewDependsOnType)(nil), (*scpb.DefaultExprTypeReference)(nil),
			(*scpb.OnUpdateExprTypeReference)(nil), (*scpb.ComputedExprTypeReference)(nil),
			(*scpb.ColumnTypeReference)(nil), (*scpb.CheckConstraintTypeReference)(nil)},
		screl.ReferencedDescID)
}

func init() {
	// Ensures that the name is drained first, only when
	// the descriptor is cleaned up.
	ns, nsTarget, nsNode := targetNodeVars("namespace")
	dep, depTarget, depNode := targetNodeVars("dep")
	tabID := rel.Var("desc-id")
	register(
		"namespace needs descriptor to be dropped",
		scgraph.Precedence,
		depNode, nsNode,
		screl.MustQuery(
			ns.Type((*scpb.Namespace)(nil)),
			dep.Type((*scpb.Table)(nil), (*scpb.View)(nil),
				(*scpb.Sequence)(nil), (*scpb.Database)(nil), (*scpb.Schema)(nil),
				(*scpb.Type)(nil)),

			tabID.Entities(screl.DescID, dep, ns),

			joinTargetNode(ns, nsTarget, nsNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
			joinTargetNode(dep, depTarget, depNode, scpb.Status_ABSENT, scpb.Status_DROPPED),
		),
	)

	// Descriptor can only be cleaned up once the namespace has been
	// dropped.
	register(
		"descriptor can only be cleaned up once the name is drained",
		scgraph.Precedence,
		nsNode, depNode,
		screl.MustQuery(
			ns.Type((*scpb.Namespace)(nil)),
			dep.Type((*scpb.Table)(nil), (*scpb.View)(nil),
				(*scpb.Sequence)(nil), (*scpb.Database)(nil), (*scpb.Schema)(nil),
				(*scpb.Type)(nil)),

			tabID.Entities(screl.DescID, dep, ns),

			joinTargetNode(ns, nsTarget, nsNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
			joinTargetNode(dep, depTarget, depNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
		),
	)
}

func init() {
	columnName, columnNameTarget, columnNameNode := targetNodeVars("column-name")
	column, columnTarget, columnNode := targetNodeVars("column")
	tabID := rel.Var("desc-id")
	columnID := rel.Var("column-id")

	register(
		"column named after column existence",
		scgraph.Precedence,
		columnNode, columnNameNode,
		screl.MustQuery(

			columnName.Type((*scpb.ColumnName)(nil)),
			column.Type((*scpb.Column)(nil)),

			tabID.Entities(screl.DescID, column, columnName),
			columnID.Entities(screl.ColumnID, column, columnName),

			joinTargetNode(column, columnTarget, columnNode, scpb.Status_PUBLIC, scpb.Status_DELETE_ONLY),
			joinTargetNode(columnName, columnNameTarget, columnNameNode, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
		),
	)

	register(
		"column named right before column becomes public",
		scgraph.SameStagePrecedence,
		columnNameNode, columnNode,
		screl.MustQuery(

			columnName.Type((*scpb.ColumnName)(nil)),
			column.Type((*scpb.Column)(nil)),

			tabID.Entities(screl.DescID, column, columnName),
			columnID.Entities(screl.ColumnID, column, columnName),

			joinTargetNode(columnName, columnNameTarget, columnNameNode, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			joinTargetNode(column, columnTarget, columnNode, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
		),
	)

	register(
		"column unnamed after column no longer public",
		scgraph.Precedence,
		columnNode, columnNameNode,
		screl.MustQuery(

			columnName.Type((*scpb.ColumnName)(nil)),
			column.Type((*scpb.Column)(nil)),

			tabID.Entities(screl.DescID, column, columnName),
			columnID.Entities(screl.ColumnID, column, columnName),

			joinTargetNode(column, columnTarget, columnNode, scpb.Status_ABSENT, scpb.Status_DELETE_AND_WRITE_ONLY),
			joinTargetNode(columnName, columnNameTarget, columnNameNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
		),
	)

	register(
		"column unnamed before column no longer exists",
		scgraph.Precedence,
		columnNameNode, columnNode,
		screl.MustQuery(

			columnName.Type((*scpb.ColumnName)(nil)),
			column.Type((*scpb.Column)(nil)),

			tabID.Entities(screl.DescID, column, columnName),
			columnID.Entities(screl.ColumnID, column, columnName),

			joinTargetNode(columnName, columnNameTarget, columnNameNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
			joinTargetNode(column, columnTarget, columnNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
		),
	)
}

func init() {
	indexName, indexNameTarget, indexNameNode := targetNodeVars("index-name")
	index, indexTarget, indexNode := targetNodeVars("index")
	tabID := rel.Var("desc-id")
	indexID := rel.Var("index-id")

	register(
		"index named after index existence",
		scgraph.Precedence,
		indexNode, indexNameNode,
		screl.MustQuery(
			indexName.Type((*scpb.IndexName)(nil)),
			index.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),

			tabID.Entities(screl.DescID, index, indexName),
			indexID.Entities(screl.IndexID, index, indexName),

			joinTargetNode(index, indexTarget, indexNode, scpb.Status_PUBLIC, scpb.Status_DELETE_ONLY),
			joinTargetNode(indexName, indexNameTarget, indexNameNode, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
		),
	)

	register(
		"index named right before index becomes public",
		scgraph.SameStagePrecedence,
		indexNameNode, indexNode,
		screl.MustQuery(
			indexName.Type((*scpb.IndexName)(nil)),
			index.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),

			tabID.Entities(screl.DescID, index, indexName),
			indexID.Entities(screl.IndexID, index, indexName),

			joinTargetNode(indexName, indexNameTarget, indexNameNode, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			joinTargetNode(index, indexTarget, indexNode, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
		),
	)

	register(
		"index unnamed after index no longer public",
		scgraph.Precedence,
		indexNode, indexNameNode,
		screl.MustQuery(
			indexName.Type((*scpb.IndexName)(nil)),
			index.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),

			tabID.Entities(screl.DescID, index, indexName),
			indexID.Entities(screl.IndexID, index, indexName),

			screl.JoinTargetNode(index, indexTarget, indexNode),
			indexTarget.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			indexNode.AttrEq(screl.CurrentStatus, scpb.Status_VALIDATED),
			joinTargetNode(indexName, indexNameTarget, indexNameNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
		),
	)

	register(
		"index unnamed before index no longer exists",
		scgraph.Precedence,
		indexNameNode, indexNode,
		screl.MustQuery(
			indexName.Type((*scpb.IndexName)(nil)),
			index.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),

			tabID.Entities(screl.DescID, index, indexName),
			indexID.Entities(screl.IndexID, index, indexName),

			joinTargetNode(indexName, indexNameTarget, indexNameNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
			joinTargetNode(index, indexTarget, indexNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
		),
	)
}

func init() {
	// Intentionally injects unsolvable dependencies on type reference drops if
	// they are invalidated by an add operation.
	typeRefDrop, typeRefDropTarget, typeRefDropNode := targetNodeVars("type-ref-drop")
	typeRefAdd, typeRefAddTarget, typeRefAddNode := targetNodeVars("type-ref-add")
	typeID := rel.Var("type-id")
	tableID := rel.Var("table-id")

	register(
		"type ref drop is no-op if ref is being added",
		scgraph.Precedence,
		typeRefDropNode, typeRefDropNode,
		screl.MustQuery(
			typeRefDrop.Type((*scpb.DefaultExprTypeReference)(nil), (*scpb.ColumnTypeReference)(nil),
				(*scpb.OnUpdateExprTypeReference)(nil), (*scpb.ComputedExprTypeReference)(nil),
				(*scpb.ViewDependsOnType)(nil)),
			typeRefAdd.Type((*scpb.DefaultExprTypeReference)(nil), (*scpb.ColumnTypeReference)(nil),
				(*scpb.OnUpdateExprTypeReference)(nil), (*scpb.ComputedExprTypeReference)(nil),
				(*scpb.ViewDependsOnType)(nil)),

			typeID.Entities(screl.ReferencedDescID, typeRefDrop, typeRefAdd),
			tableID.Entities(screl.DescID, typeRefDrop, typeRefAdd),

			joinTargetNode(typeRefDrop, typeRefDropTarget, typeRefDropNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
			joinTargetNode(typeRefAdd, typeRefAddTarget, typeRefAddNode, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
		),
	)
}

func init() {
	// Ensure table dependencies drop after the table is marked as dropped.
	dep, depTarget, depNode := targetNodeVars("dep-drop")
	tbl, tblTarget, tblNode := targetNodeVars("table-drop")
	tableID := rel.Var("table-id")

	register(
		"table deps removal happens after table marked as dropped",
		scgraph.Precedence,
		tblNode, depNode,
		screl.MustQuery(
			dep.Type((*scpb.Owner)(nil), (*scpb.UserPrivileges)(nil), (*scpb.Locality)(nil)),
			tbl.Type((*scpb.Table)(nil), (*scpb.Sequence)(nil), (*scpb.View)(nil)),

			tableID.Entities(screl.DescID, tbl, dep),

			joinTargetNode(dep, depTarget, depNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
			joinTargetNode(tbl, tblTarget, tblNode, scpb.Status_ABSENT, scpb.Status_DROPPED),
		),
	)
}

func init() {
	schema, schemaTarget, schemaNode := targetNodeVars("schema")
	scEntry, scEntryTarget, scEntryNode := targetNodeVars("schema-entry")
	schemaID := rel.Var("schema-id")

	register(
		"schema can be dropped after schema entry inside the database",
		scgraph.Precedence,
		scEntryNode, schemaNode,
		screl.MustQuery(
			schema.Type((*scpb.Schema)(nil)),
			scEntry.Type((*scpb.DatabaseSchemaEntry)(nil)),

			schemaID.Entities(screl.DescID, schema),
			schemaID.Entities(screl.ReferencedDescID, scEntry),

			joinTargetNode(schema, schemaTarget, schemaNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
			joinTargetNode(scEntry, scEntryTarget, scEntryNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
		),
	)
}

func init() {
	database, databaseTarget, databaseNode := targetNodeVars("database")
	scEntry, scEntryTarget, scEntryNode := targetNodeVars("schema-entry")
	schemaID := rel.Var("schema-id")

	register(
		"schema entry can be dropped after the database has exited synth drop",
		scgraph.Precedence,
		databaseNode, scEntryNode,
		screl.MustQuery(
			database.Type((*scpb.Database)(nil)),
			scEntry.Type((*scpb.DatabaseSchemaEntry)(nil)),

			schemaID.Entities(screl.DescID, database, scEntry),

			joinTargetNode(database, databaseTarget, databaseNode, scpb.Status_ABSENT, scpb.Status_DROPPED),
			joinTargetNode(scEntry, scEntryTarget, scEntryNode, scpb.Status_ABSENT, scpb.Status_ABSENT),
		),
	)
}
