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
	"github.com/cockroachdb/errors"
)

func init() {
	// Ensures that child objects under a database or schema
	// are dropped before any children are dealt with.
	depRule(
		"descriptor must be dropped before draining name",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_ABSENT,
			(*scpb.Schema)(nil),
			(*scpb.Table)(nil),
			(*scpb.View)(nil),
			(*scpb.Sequence)(nil),
			(*scpb.Type)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.Database)(nil),
			(*scpb.Schema)(nil),
		),
	).withFilter("has-parent-child-relationship", func(child, parent scpb.Element) bool {
		var dependentObjects []descpb.ID
		switch p := parent.(type) {
		case *scpb.Database:
			dependentObjects = p.DependentObjects
		case *scpb.Schema:
			dependentObjects = p.DependentObjects
		default:
			panic(errors.AssertionFailedf("invalid parent type %T", parent))
		}
		return idInIDs(dependentObjects, screl.GetDescID(child))
	}).register()
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
				joinTargetNode(relation, relationTarget, scpb.Status_ABSENT, relationNode, scpb.Status_DROPPED),
				joinTargetNode(dep, depTarget, scpb.Status_ABSENT, depNode, scpb.Status_ABSENT),
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

			joinTargetNode(typeRefDrop, typeRefDropTarget, scpb.Status_ABSENT, typeRefDropNode, scpb.Status_ABSENT),
			joinTargetNode(typeRefAdd, typeRefAddTarget, scpb.Status_PUBLIC, typeRefAddNode, scpb.Status_PUBLIC),
		),
	)
}

func init() {
	depRule(
		"descriptor must be dropped before draining name",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_DROPPED,
			(*scpb.Database)(nil),
			(*scpb.Schema)(nil),
			(*scpb.Table)(nil),
			(*scpb.View)(nil),
			(*scpb.Sequence)(nil),
			(*scpb.Type)(nil),
		),
		element(scpb.Status_ABSENT, (*scpb.Namespace)(nil)),
		screl.DescID,
	).register()

	depRule(
		"name must drain before descriptor deletion",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_ABSENT, (*scpb.Namespace)(nil)),
		element(scpb.Status_ABSENT,
			(*scpb.Database)(nil),
			(*scpb.Schema)(nil),
			(*scpb.Table)(nil),
			(*scpb.View)(nil),
			(*scpb.Sequence)(nil),
			(*scpb.Type)(nil),
		),
		screl.DescID,
	).register()

	depRule(
		"relation drop precedes relation deps removal",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_DROPPED, (*scpb.Table)(nil), (*scpb.Sequence)(nil), (*scpb.View)(nil)),
		element(scpb.Status_ABSENT, (*scpb.Owner)(nil), (*scpb.UserPrivileges)(nil), (*scpb.Locality)(nil)),
		screl.DescID,
	).register()

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

			joinTargetNode(schema, schemaTarget, scpb.Status_ABSENT, schemaNode, scpb.Status_ABSENT),
			joinTargetNode(scEntry, scEntryTarget, scpb.Status_ABSENT, scEntryNode, scpb.Status_ABSENT),
		),
	)

	depRule(
		"database drop precedes schema entry removal",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(scpb.Status_DROPPED, (*scpb.Database)(nil)),
		element(scpb.Status_ABSENT, (*scpb.DatabaseSchemaEntry)(nil)),
		screl.DescID,
	).register()
}
