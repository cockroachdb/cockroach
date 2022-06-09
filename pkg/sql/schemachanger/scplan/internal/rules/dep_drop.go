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

// This batch of rules ensures that descriptor elements, that is to say elements
// which own a descriptor, reach the DROPPED state in the correct order.
// This normally happens during the pre-commit phase, due to the constraints
// which were placed in these elements' state transition definitions in opgen.
func init() {

	registerDepRule(
		"view drops before the types, views and tables it depends on",
		scgraph.Precedence,
		"view", "dependents",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.View)(nil)),
				to.Type(
					(*scpb.AliasType)(nil),
					(*scpb.EnumType)(nil),
					(*scpb.View)(nil),
					(*scpb.Table)(nil),
				),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_DROPPED),
				rel.Filter("ViewDependsOn", from, to)(func(
					view *scpb.View, dep scpb.Element,
				) bool {
					depID := screl.GetDescID(dep)
					return idInIDs(view.UsesRelationIDs, depID) || idInIDs(view.UsesTypeIDs, depID)
				}),
			}
		},
	)

	registerDepRule(
		"alias type drops before the types it depends on",
		scgraph.Precedence,
		"alias", "alias-dep",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.AliasType)(nil)),
				to.Type((*scpb.AliasType)(nil), (*scpb.EnumType)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_DROPPED),
				rel.Filter("aliasTypeDependsOn", from, to)(func(
					alias *scpb.AliasType, dep scpb.Element,
				) bool {
					depID := screl.GetDescID(dep)
					return alias.TypeID != depID && idInIDs(alias.ClosedTypeIDs, depID)
				}),
			}
		},
	)

	registerDepRule(
		"array type drops right before its element enum type",
		scgraph.SameStagePrecedence,
		"alias", "enum",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.AliasType)(nil)),
				to.Type((*scpb.EnumType)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_DROPPED),
				rel.Filter("joinArrayTypeWithEnumType", from, to)(func(
					arrayType *scpb.AliasType, enumType *scpb.EnumType,
				) bool {
					return arrayType.TypeID == enumType.ArrayTypeID
				}),
			}
		},
	)
}

// These rules ensure that non-descriptor elements reach the ABSENT state before
// the descriptor elements they depend on reach the DROPPED state.
// Again, this normally happens during the pre-commit phase.
func init() {

	// This rule implicitly defines a precedence relationship which ensures that
	// a schema reaches DROPPED before its parent database does.
	registerDepRule(
		"schema dropped before parent database",
		scgraph.Precedence,
		"schema-parent", "database",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SchemaParent)(nil)),
				to.Type((*scpb.Database)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				joinOn(from, screl.ReferencedDescID, to, screl.DescID, "desc-id"),
			}
		},
	)

	// This rule implicitly defines a precedence relationship which ensures that
	// an object reaches DROPPED before its parent schema does.
	registerDepRule(
		"object dropped before parent schema",
		scgraph.Precedence,
		"object-parent", "schema",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ObjectParent)(nil)),
				to.Type((*scpb.Schema)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				joinOn(from, screl.ReferencedDescID, to, screl.DescID, "desc-id"),
			}
		},
	)

	registerDepRule(
		"secondary region locality removed before dropping multi-region enum type",
		scgraph.Precedence,
		"secondary-region", "enum-type",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TableLocalitySecondaryRegion)(nil)),
				to.Type((*scpb.EnumType)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				joinOn(from, screl.ReferencedDescID, to, screl.DescID, "desc-id"),
			}
		},
	)

	registerDepRule(
		"check constraint removed before dropping dependent types and sequences",
		scgraph.Precedence,
		"check-constraint", "dependent",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.CheckConstraint)(nil)),
				to.Type(
					(*scpb.AliasType)(nil),
					(*scpb.EnumType)(nil),
					(*scpb.Sequence)(nil),
				),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				rel.Filter("checkConstraintDependsOn", from, to)(func(
					check *scpb.CheckConstraint, dep scpb.Element,
				) bool {
					depID := screl.GetDescID(dep)
					return idInIDs(check.UsesTypeIDs, depID) || idInIDs(check.UsesSequenceIDs, depID)
				}),
			}
		},
	)

	registerDepRule(
		"FK removed before dropping dependent table",
		scgraph.Precedence,
		"foreign-key", "table",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ForeignKeyConstraint)(nil)),
				to.Type((*scpb.Table)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				joinOn(from, screl.ReferencedDescID, to, screl.DescID, "desc-id"),
			}
		},
	)

	registerDepRule(
		"index partial predicate removed before dropping dependent types",
		scgraph.Precedence,
		"index-partial", "dependent-type",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndexPartial)(nil)),
				to.Type((*scpb.AliasType)(nil), (*scpb.EnumType)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				rel.Filter("indexPartialDependsOn", from, to)(func(
					ip *scpb.SecondaryIndexPartial, dep scpb.Element,
				) bool {
					return idInIDs(ip.UsesTypeIDs, screl.GetDescID(dep))
				}),
			}
		},
	)

	registerDepRule(
		"column type removed before dropping dependent types",
		scgraph.Precedence,
		"column-type", "dependent-type",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnType)(nil)),
				to.Type((*scpb.AliasType)(nil), (*scpb.EnumType)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				rel.Filter("columnTypeDependsOn", from, to)(func(
					cd *scpb.ColumnType, dep scpb.Element,
				) bool {
					depID := screl.GetDescID(dep)
					if ce := cd.ComputeExpr; ce != nil && idInIDs(ce.UsesTypeIDs, depID) {
						return true
					}
					return idInIDs(cd.ClosedTypeIDs, depID)
				}),
			}
		},
	)

	registerDepRule(
		"column DEFAULT removed before dropping dependent types and sequences",
		scgraph.Precedence,
		"default-expr", "dependent",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnDefaultExpression)(nil)),
				to.Type(
					(*scpb.AliasType)(nil),
					(*scpb.EnumType)(nil),
					(*scpb.Sequence)(nil),
				),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				rel.Filter("columnDefaultDependsOn", from, to)(func(
					cd *scpb.ColumnDefaultExpression, dep scpb.Element,
				) bool {
					depID := screl.GetDescID(dep)
					return idInIDs(cd.UsesTypeIDs, depID) || idInIDs(cd.UsesSequenceIDs, depID)
				}),
			}
		},
	)

	registerDepRule(
		"column ON UPDATE removed before dropping dependent types and sequences",
		scgraph.Precedence,
		"on-update-expr", "dependent",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnOnUpdateExpression)(nil)),
				to.Type(
					(*scpb.AliasType)(nil),
					(*scpb.EnumType)(nil),
					(*scpb.Sequence)(nil),
				),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				rel.Filter("columnOnUpdateDependsOn", from, to)(func(
					cu *scpb.ColumnOnUpdateExpression, dep scpb.Element,
				) bool {
					depID := screl.GetDescID(dep)
					return idInIDs(cu.UsesTypeIDs, depID) || idInIDs(cu.UsesSequenceIDs, depID)
				}),
			}
		},
	)

	registerDepRule(
		"sequence ownership removed before dropping sequence",
		scgraph.Precedence,
		"sequence-owner", "sequence",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SequenceOwner)(nil)),
				to.Type((*scpb.Sequence)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				joinOn(from, screl.ReferencedDescID, to, screl.DescID, "desc-id"),
			}

		},
	)

	registerDepRule(
		"database region config removed before dropping multi-region enum type",
		scgraph.Precedence,
		"region-config", "enum-type",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.DatabaseRegionConfig)(nil)),
				to.Type((*scpb.EnumType)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				joinOn(from, screl.ReferencedDescID, to, screl.DescID, "desc-id"),
			}
		},
	)

	registerDepRule(
		"database region config removed before dropping multi-region enum type",
		scgraph.Precedence,
		"region-config", "enum-type",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.DatabaseRegionConfig)(nil)),
				to.Type((*scpb.EnumType)(nil)),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				joinOn(from, screl.ReferencedDescID, to, screl.DescID, "desc-id"),
			}
		})

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
	registerDepRule(
		"dependent element removal before descriptor drop",
		scgraph.Precedence,
		"element", "relation",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
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
				to.Type(
					(*scpb.Database)(nil),
					(*scpb.Schema)(nil),
					(*scpb.Table)(nil),
					(*scpb.View)(nil),
					(*scpb.Sequence)(nil),
					(*scpb.AliasType)(nil),
					(*scpb.EnumType)(nil),
				),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatus(fromNode, scpb.Status_ABSENT),
				currentStatus(toNode, scpb.Status_DROPPED),
				join(from, to, screl.DescID, "desc-id"),
			}
		})

	registerDepRule(
		"dependent element removal right after descriptor removal",
		scgraph.SameStagePrecedence,
		"relation", "element",
		func(from, fromTarget, fromNode, to, toTarget, toNode rel.Var) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.Table)(nil),
					(*scpb.View)(nil),
				),
				to.Type(
					(*scpb.Column)(nil),
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
					(*scpb.RowLevelTTL)(nil),
				),
				targetStatusEq(fromTarget, toTarget, scpb.ToAbsent),
				currentStatusEq(fromNode, toNode, scpb.Status_ABSENT),
				join(from, to, screl.DescID, "desc-id"),
			}
		},
	)
}
