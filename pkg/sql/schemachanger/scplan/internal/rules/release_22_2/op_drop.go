// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_22_2

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
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

	relation := MkNodeVars("relation")
	column := MkNodeVars("column")
	dep := MkNodeVars("column-dep")
	relationID, columnID := rel.Var("relation-id"), rel.Var("column-id")
	registerOpRule(
		"skip column removal ops on relation drop",
		column.Node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			column.Type(
				(*scpb.Column)(nil),
			),

			JoinOnDescID(relation, column, relationID),

			relation.JoinTarget(),
			relation.TargetStatus(scpb.ToAbsent),
			column.JoinTargetNode(),
			column.TargetStatus(scpb.ToAbsent),
			column.CurrentStatus(
				// All but DELETE_ONLY which is the status leading to ABSENT.
				scpb.Status_PUBLIC,
				scpb.Status_WRITE_ONLY,
			),
		),
	)

	registerOpRule(
		"skip column dependents removal ops on relation drop",
		dep.Node,
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

			JoinOnDescID(relation, column, relationID),
			JoinOnColumnID(column, dep, relationID, columnID),

			relation.JoinTarget(),
			relation.TargetStatus(scpb.ToAbsent),
			column.JoinTarget(),
			column.TargetStatus(scpb.ToAbsent),
			dep.JoinTargetNode(),
			dep.TargetStatus(scpb.ToAbsent),
		),
	)
}

// When dropping a table or a view, skip all removal ops for index elements
// as well as elements which depend on them, provided there are no
// back-references that need to be cleaned up. This is similar as for columns.
func init() {
	relation := MkNodeVars("relation")
	index := MkNodeVars("index")
	dep := MkNodeVars("index-dep")
	relationID, indexID := rel.Var("relation-id"), rel.Var("index-id")

	registerOpRule(
		"skip index removal ops on relation drop",
		index.Node,
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

			JoinOnDescID(relation, index, relationID),

			relation.JoinTarget(),
			relation.TargetStatus(scpb.ToAbsent),
			index.JoinTargetNode(),
			index.TargetStatus(scpb.ToAbsent),
		),
	)

	registerOpRule(
		"skip index dependents removal ops on relation drop",
		dep.Node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			index.TypeFilter(rulesVersionKey, IsIndex),
			dep.Type(
				(*scpb.IndexName)(nil),
				(*scpb.IndexPartitioning)(nil),
				(*scpb.IndexColumn)(nil),
			),

			JoinOnDescID(relation, index, relationID),
			JoinOnIndexID(index, dep, relationID, indexID),

			relation.JoinTarget(),
			relation.TargetStatus(scpb.ToAbsent),
			index.JoinTarget(),
			index.TargetStatus(scpb.ToAbsent),
			dep.JoinTargetNode(),
			dep.TargetStatus(scpb.ToAbsent),
		),
	)
}

// When dropping a table or a view, skip all removal ops for constraint elements
// as well as elements which depend on them, provided there are no
// back-references that need to be cleaned up. This is similar as for columns
// and indexes.
func init() {
	relation := MkNodeVars("relation")
	constraint := MkNodeVars("constraint")
	dep, constraintID := MkNodeVars("constraint-dep"), rel.Var("constraint-id")
	relationID := rel.Var("relation-id")

	registerOpRule(
		"skip constraint removal ops on relation drop",
		constraint.Node,
		screl.MustQuery(
			relation.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
			),
			constraint.Type(
				(*scpb.UniqueWithoutIndexConstraint)(nil),
			),

			JoinOnDescID(relation, constraint, relationID),

			relation.JoinTarget(),
			relation.TargetStatus(scpb.ToAbsent),
			constraint.JoinTargetNode(),
			constraint.TargetStatus(scpb.ToAbsent),
		),
	)

	registerOpRule(
		"skip constraint dependents removal ops on relation drop",
		dep.Node,
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

			JoinOnDescID(relation, constraint, relationID),
			JoinOnConstraintID(constraint, dep, relationID, constraintID),

			relation.JoinTarget(),
			relation.TargetStatus(scpb.ToAbsent),
			constraint.JoinTarget(),
			constraint.TargetStatus(scpb.ToAbsent),
			dep.JoinTargetNode(),
			dep.TargetStatus(scpb.ToAbsent),
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
	desc := MkNodeVars("desc")
	dep := MkNodeVars("dep")
	descID := rel.Var("desc-id")

	registerOpRule(
		"skip element removal ops on descriptor drop",
		dep.Node,
		screl.MustQuery(
			desc.TypeFilter(rulesVersionKey, isDescriptor),
			dep.Type(
				(*scpb.ColumnFamily)(nil),
				(*scpb.Owner)(nil),
				(*scpb.UserPrivileges)(nil),
				(*scpb.EnumTypeValue)(nil),
			),

			JoinOnDescID(desc, dep, descID),

			desc.JoinTarget(),
			desc.TargetStatus(scpb.ToAbsent),
			dep.JoinTargetNode(),
			dep.TargetStatus(scpb.ToAbsent),
		),
	)
}

// Skip all removal ops for dropping table comments corresponding to elements
// when dropping the table itself.
func init() {
	desc := MkNodeVars("desc")
	dep := MkNodeVars("dep")
	descID := rel.Var("desc-id")

	registerOpRule(
		"skip table comment removal ops on descriptor drop",
		dep.Node,
		screl.MustQuery(
			desc.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
				(*scpb.Sequence)(nil),
			),
			dep.Type(
				(*scpb.ColumnComment)(nil),
				(*scpb.IndexComment)(nil),
				(*scpb.ConstraintComment)(nil),
				(*scpb.TableComment)(nil),
			),

			JoinOnDescID(desc, dep, descID),

			desc.JoinTarget(),
			desc.TargetStatus(scpb.ToAbsent),
			dep.JoinTargetNode(),
			dep.TargetStatus(scpb.ToAbsent),
		),
	)
}

// Skip all removal ops for table zone configs.
func init() {
	desc := MkNodeVars("desc")
	dep := MkNodeVars("dep")
	descID := rel.Var("desc-id")

	registerOpRule(
		"skip table zone config removal ops on descriptor drop",
		dep.Node,
		screl.MustQuery(
			desc.Type(
				(*scpb.Table)(nil),
				(*scpb.View)(nil),
				(*scpb.Sequence)(nil),
			),
			dep.Type(
				(*scpb.TableZoneConfig)(nil),
			),

			JoinOnDescID(desc, dep, descID),

			desc.JoinTarget(),
			desc.TargetStatus(scpb.ToAbsent),
			dep.JoinTargetNode(),
			dep.TargetStatus(scpb.ToAbsent),
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
