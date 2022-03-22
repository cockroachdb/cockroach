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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// This batch of rules ensures that descriptor elements, that is to say elements
// which own a descriptor, reach the DROPPED state in the correct order.
// This normally happens during the pre-commit phase, due to the constraints
// which were placed in these elements' state transition definitions in opgen.
func init() {

	depRule(
		"view drops before the types, views and tables it depends on",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_DROPPED,
			(*scpb.View)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
			(*scpb.View)(nil),
			(*scpb.Table)(nil),
		),
	).withFilter("view-depends-on", func(view *scpb.View, dep scpb.Element) bool {
		depID := screl.GetDescID(dep)
		return idInIDs(view.UsesRelationIDs, depID) || idInIDs(view.UsesTypeIDs, depID)
	}).register()

	depRule(
		"alias type drops before the types it depends on",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
		),
	).withFilter("alias-type-depends-on", func(alias *scpb.AliasType, dep scpb.Element) bool {
		depID := screl.GetDescID(dep)
		return alias.TypeID != depID && idInIDs(alias.ClosedTypeIDs, depID)
	}).register()

	depRule(
		"array type drops right before its element enum type",
		scgraph.SameStagePrecedence,
		scpb.ToAbsent,
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.EnumType)(nil),
		),
	).withFilter("join-array-type-with-element-type", func(arrayType *scpb.AliasType, enumType *scpb.EnumType) bool {
		return arrayType.TypeID == enumType.ArrayTypeID
	}).register()
}

// These rules ensure that non-descriptor elements reach the ABSENT state before
// the descriptor elements they depend on reach the DROPPED state.
// Again, this normally happens during the pre-commit phase.
func init() {

	// This rule implicitly defines a precedence relationship which ensures that
	// a schema reaches DROPPED before its parent database does.
	depRule(
		"schema dropped before parent database",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.SchemaParent)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.Database)(nil),
		),
	).withJoinFromReferencedDescIDWithToDescID().register()

	// This rule implicitly defines a precedence relationship which ensures that
	// an object reaches DROPPED before its parent schema does.
	depRule(
		"object dropped before parent schema",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.ObjectParent)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.Schema)(nil),
		),
	).withJoinFromReferencedDescIDWithToDescID().register()

	depRule(
		"secondary region locality removed before dropping multi-region enum type",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.TableLocalitySecondaryRegion)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.EnumType)(nil),
		),
	).withJoinFromReferencedDescIDWithToDescID().register()

	depRule(
		"check constraint removed before dropping dependent types and sequences",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.CheckConstraint)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
			(*scpb.Sequence)(nil),
		),
	).withFilter("check-constraint-depends-on", func(check *scpb.CheckConstraint, dep scpb.Element) bool {
		depID := screl.GetDescID(dep)
		return idInIDs(check.UsesTypeIDs, depID) || idInIDs(check.UsesSequenceIDs, depID)
	}).register()

	depRule(
		"FK removed before dropping dependent table",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.ForeignKeyConstraint)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.Table)(nil),
		),
	).withJoinFromReferencedDescIDWithToDescID().register()

	depRule(
		"index partial predicate removed before dropping dependent types",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.SecondaryIndexPartial)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
		),
	).withFilter("index-partial-depends-on", func(ip *scpb.SecondaryIndexPartial, dep scpb.Element) bool {
		return idInIDs(ip.UsesTypeIDs, screl.GetDescID(dep))
	}).register()

	depRule(
		"column type removed before dropping dependent types",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.ColumnType)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
		),
	).withFilter("column-type-depends-on", func(cd *scpb.ColumnType, dep scpb.Element) bool {
		depID := screl.GetDescID(dep)
		if ce := cd.ComputeExpr; ce != nil && idInIDs(ce.UsesTypeIDs, depID) {
			return true
		}
		return idInIDs(cd.ClosedTypeIDs, depID)
	}).register()

	depRule(
		"column DEFAULT removed before dropping dependent types and sequences",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.ColumnDefaultExpression)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
			(*scpb.Sequence)(nil),
		),
	).withFilter("column-default-depends-on", func(cd *scpb.ColumnDefaultExpression, dep scpb.Element) bool {
		depID := screl.GetDescID(dep)
		return idInIDs(cd.UsesTypeIDs, depID) || idInIDs(cd.UsesSequenceIDs, depID)
	}).register()

	depRule(
		"column ON UPDATE removed before dropping dependent types and sequences",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.ColumnOnUpdateExpression)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
			(*scpb.Sequence)(nil),
		),
	).withFilter("column-on-update-depends-on", func(cu *scpb.ColumnOnUpdateExpression, dep scpb.Element) bool {
		depID := screl.GetDescID(dep)
		return idInIDs(cu.UsesTypeIDs, depID) || idInIDs(cu.UsesSequenceIDs, depID)
	}).register()

	depRule(
		"sequence ownership removed before dropping sequence",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.SequenceOwner)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.Sequence)(nil),
		),
	).withJoinFromReferencedDescIDWithToDescID().register()

	depRule(
		"database region config removed before dropping multi-region enum type",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.DatabaseRegionConfig)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.EnumType)(nil),
		),
	).withJoinFromReferencedDescIDWithToDescID().register()

}

// These rules ensure that:
// - when a descriptor element reaches the DROPPED state in the pre-commit phase
//   its dependent elements (namespace entry, comments, column names, etc) have
//   already reached the ABSENT state, if they can do so in this phase;
// - for those dependent elements which have to wait post-commit to reach the
//   ABSENT state, we tie them to the same stage as when the descriptor element
//   reaches the ABSENT state, but afterwards in the stage, so as to not
//   interfere with the event logging op which is tied to the descriptor element
//   removal.
func init() {
	depRule(
		"dependent element removal before descriptor drop",
		scgraph.Precedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			// Table elements.
			(*scpb.ColumnFamily)(nil),
			(*scpb.UniqueWithoutIndexConstraint)(nil),
			(*scpb.CheckConstraint)(nil),
			(*scpb.ForeignKeyConstraint)(nil),
			(*scpb.TableComment)(nil),
			// Multi-region elements.
			(*scpb.TableLocalityGlobal)(nil),
			(*scpb.TableLocalityPrimaryRegion)(nil),
			(*scpb.TableLocalitySecondaryRegion)(nil),
			(*scpb.TableLocalityRegionalByRow)(nil),
			// Column elements.
			(*scpb.ColumnName)(nil),
			(*scpb.ColumnDefaultExpression)(nil),
			(*scpb.ColumnOnUpdateExpression)(nil),
			(*scpb.ColumnComment)(nil),
			(*scpb.SequenceOwner)(nil),
			// Index elements.
			(*scpb.IndexName)(nil),
			(*scpb.IndexPartitioning)(nil),
			(*scpb.IndexComment)(nil),
			// Constraint elements.
			(*scpb.ConstraintName)(nil),
			(*scpb.ConstraintComment)(nil),
			// Common elements.
			(*scpb.Namespace)(nil),
			(*scpb.Owner)(nil),
			(*scpb.UserPrivileges)(nil),
			// Database elements.
			(*scpb.DatabaseRoleSetting)(nil),
			(*scpb.DatabaseRegionConfig)(nil),
			(*scpb.DatabaseComment)(nil),
			// Schema elements.
			(*scpb.SchemaParent)(nil),
			(*scpb.SchemaComment)(nil),
			// Object elements.
			(*scpb.ObjectParent)(nil),
		),
		element(scpb.Status_DROPPED,
			(*scpb.Database)(nil),
			(*scpb.Schema)(nil),
			(*scpb.Table)(nil),
			(*scpb.View)(nil),
			(*scpb.Sequence)(nil),
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
		),
		screl.DescID,
	).register()

	depRule(
		"dependent element removal right after descriptor removal",
		scgraph.SameStagePrecedence,
		scpb.ToAbsent,
		element(scpb.Status_ABSENT,
			(*scpb.Table)(nil),
			(*scpb.View)(nil),
		),
		element(scpb.Status_ABSENT,
			(*scpb.Column)(nil),
			(*scpb.PrimaryIndex)(nil),
			(*scpb.SecondaryIndex)(nil),
			(*scpb.RowLevelTTL)(nil),
		),
		screl.DescID,
	).register()
}
