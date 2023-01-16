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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// When dropping a table or a view, skip all removal ops for column elements
// and column-dependent elements (like column names).
// Columns get implicitly removed once the descriptor is marked as dropped.
//
// The column transition to ABSENT needs to remain to clean up back-references
// in types referred to by the column in its type or its computed expression.
//
// We can't skip ops for column-dependent elements which reference other
// descriptors, like for example default expressions, again because of the need
// to clean up back-references.
//
// We also can't skip ops for column-dependent elements which don't like in the
// descriptor, like column comments (which live in a dedicated system table).
func init() {

	relation := mkNodeVars("relation")
	column := mkNodeVars("column")
	dep := mkNodeVars("column-dep")
	relationID, columnID := rel.Var("relation-id"), rel.Var("column-id")
	registerOpRule(
		"skip column removal ops on relation drop",
		column.node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			column.Type(
				(*scpb.Column)(nil),
			),

			joinOnDescID(relation, column, relationID),

			relation.joinTarget(),
			relation.targetStatus(scpb.ToAbsent),
			column.joinTargetNode(),
			column.targetStatus(scpb.ToAbsent),
			column.currentStatus(
				// All but DELETE_ONLY which is the status leading to ABSENT.
				scpb.Status_PUBLIC,
				scpb.Status_WRITE_ONLY,
			),
		),
	)

	registerOpRule(
		"skip column dependents removal ops on relation drop",
		dep.node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			column.Type(
				(*scpb.Column)(nil),
			),
			dep.Type(
				(*scpb.ColumnName)(nil),
			),

			joinOnDescID(relation, column, relationID),
			joinOnColumnID(column, dep, relationID, columnID),

			relation.joinTarget(),
			relation.targetStatus(scpb.ToAbsent),
			column.joinTarget(),
			column.targetStatus(scpb.ToAbsent),
			dep.joinTargetNode(),
			dep.targetStatus(scpb.ToAbsent),
		),
	)
}

// When dropping a table or a view, skip all removal ops for index elements
// as well as elements which depend on them, provided there are no
// back-references that need to be cleaned up. This is similar as for columns.
func init() {
	relation := mkNodeVars("relation")
	index := mkNodeVars("index")
	dep := mkNodeVars("index-dep")
	relationID, indexID := rel.Var("relation-id"), rel.Var("index-id")

	registerOpRule(
		"skip index removal ops on relation drop",
		index.node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			index.Type(
				(*scpb.PrimaryIndex)(nil),
				(*scpb.SecondaryIndex)(nil),
				(*scpb.TemporaryIndex)(nil),
			),

			joinOnDescID(relation, index, relationID),

			relation.joinTarget(),
			relation.targetStatus(scpb.ToAbsent),
			index.joinTargetNode(),
			index.targetStatus(scpb.ToAbsent),
		),
	)

	registerOpRule(
		"skip index dependents removal ops on relation drop",
		dep.node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			index.typeFilter(isIndex),
			dep.Type(
				(*scpb.IndexName)(nil),
				(*scpb.IndexPartitioning)(nil),
				(*scpb.IndexColumn)(nil),
			),

			joinOnDescID(relation, index, relationID),
			joinOnIndexID(index, dep, relationID, indexID),

			relation.joinTarget(),
			relation.targetStatus(scpb.ToAbsent),
			index.joinTarget(),
			index.targetStatus(scpb.ToAbsent),
			dep.joinTargetNode(),
			dep.targetStatus(scpb.ToAbsent),
		),
	)
}

// When dropping a table or a view, skip all removal ops for constraint elements
// as well as elements which depend on them, provided there are no
// back-references that need to be cleaned up. This is similar as for columns
// and indexes.
func init() {
	relation := mkNodeVars("relation")
	constraint := mkNodeVars("constraint")
	dep := mkNodeVars("constraint-dep")
	relationID, constraintID := rel.Var("relation-id"), rel.Var("constraint-id")

	registerOpRule(
		"skip constraint removal ops on relation drop",
		constraint.node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			constraint.Type(
				(*scpb.CheckConstraint)(nil),
				(*scpb.ForeignKeyConstraint)(nil),
				(*scpb.UniqueWithoutIndexConstraint)(nil),
			),

			joinOnDescID(relation, constraint, relationID),

			relation.joinTarget(),
			relation.targetStatus(scpb.ToAbsent),
			constraint.joinTargetNode(),
			constraint.targetStatus(scpb.ToAbsent),
			constraint.currentStatus(
				// Only skip ops in the transition from PUBLIC on relation drop.
				scpb.Status_PUBLIC,
			),
		),
	)

	registerOpRule(
		"skip constraint dependents removal ops on relation drop",
		dep.node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			constraint.Type(
				(*scpb.UniqueWithoutIndexConstraint)(nil),
				(*scpb.CheckConstraint)(nil),
				(*scpb.ForeignKeyConstraint)(nil),
			),
			dep.Type(
				(*scpb.ConstraintWithoutIndexName)(nil),
			),

			joinOnDescID(relation, constraint, relationID),
			joinOnConstraintID(constraint, dep, relationID, constraintID),

			relation.joinTarget(),
			relation.targetStatus(scpb.ToAbsent),
			constraint.joinTarget(),
			constraint.targetStatus(scpb.ToAbsent),
			dep.joinTargetNode(),
			dep.targetStatus(scpb.ToAbsent),
		),
	)
}

// Skip all removal ops on elements which depend on a descriptor element that
// is being removed as well. These elements get implicitly removed once the
// descriptor is marked as dropped.
// This rule excludes:
//   - index and column elements, which were handled above,
//   - elements which don't live inside the descriptor, like comments,
//   - elements which have forward references to other descriptors: back-
//     references need to be cleaned up.
func init() {
	desc := mkNodeVars("desc")
	dep := mkNodeVars("dep")
	descID := rel.Var("desc-id")

	registerOpRule(
		"skip element removal ops on descriptor drop",
		dep.node,
		screl.MustQuery(
			desc.typeFilter(IsDescriptor),
			dep.Type(
				(*scpb.ColumnFamily)(nil),
				(*scpb.Owner)(nil),
				(*scpb.UserPrivileges)(nil),
				(*scpb.EnumTypeValue)(nil),
				(*scpb.TablePartitioning)(nil),
			),

			joinOnDescID(desc, dep, descID),

			desc.joinTarget(),
			desc.targetStatus(scpb.ToAbsent),
			dep.joinTargetNode(),
			dep.targetStatus(scpb.ToAbsent),
		),
	)
}

// Skip all removal ops for table zone configs.
func init() {
	desc := mkNodeVars("desc")
	dep := mkNodeVars("dep")
	descID := rel.Var("desc-id")

	registerOpRule(
		"skip table zone config removal ops on descriptor drop",
		dep.node,
		screl.MustQuery(
			desc.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
				(*scpb.Sequence)(nil),
			),
			dep.Type(
				(*scpb.TableZoneConfig)(nil),
			),

			joinOnDescID(desc, dep, descID),

			desc.joinTarget(),
			desc.targetStatus(scpb.ToAbsent),
			dep.joinTargetNode(),
			dep.targetStatus(scpb.ToAbsent),
		),
	)
}

// TODO(fqazi): For create operations we will need to have the ability
// to have transformations that will combine transitions into a single
// stage for execution. For example, a newly CREATE TABLE will be represented
// by a TABLE, COLUMN, and INDEX elements (among others), all of the operations
// for these elements are executable in a single stage. Having them execute
// across multiple stages would be problematic both from a validation and
// correctness viewpoint.
