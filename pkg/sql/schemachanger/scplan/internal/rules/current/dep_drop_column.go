// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package current

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// These rules ensure that column-dependent elements, like a column's name, its
// DEFAULT expression, etc. disappear once the column reaches a suitable state.
func init() {

	// N.B. This rules is superseded by the "column constraint removed right before
	// column reaches write only" rule below for the not null column check.
	registerDepRuleForDrop(
		"column no longer public before dependents",
		scgraph.Precedence,
		"column", "dependent",
		scpb.Status_WRITE_ONLY, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.TypeFilter(rulesVersionKey, isColumnDependent, Not(isColumnNotNull)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)

	registerDepRuleForDrop(
		"dependents removed before column",
		scgraph.Precedence,
		"dependent", "column",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isColumnDependent),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)

	registerDepRule(
		"column type removed before column family",
		scgraph.Precedence,
		"column-type", "column-family",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnType)(nil)),
				to.Type((*scpb.ColumnFamily)(nil)),
				JoinOnColumnFamilyID(from, to, "table-id", "family-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		},
	)
}

// Special cases of the above.
func init() {

	registerDepRule(
		"column type dependents removed right before column type, except if part of a column type alteration ",
		scgraph.SameStagePrecedence,
		"dependent", "column-type",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isColumnTypeDependent),
				to.Type((*scpb.ColumnType)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				IsNotAlterColumnTypeOp("table-id", "col-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		},
	)

	// This rule is similar to the previous one but relaxes SameStagePrecedence,
	// allowing for planning in case the ALTER COLUMN .. TYPE needs to roll back
	// (particularly when altering columns with DEFAULT or ON UPDATE expressions).
	registerDepRule(
		"during a column type alterations, column type dependents removed before column type",
		scgraph.Precedence,
		"dependent", "column-type",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isColumnTypeDependent),
				to.Type((*scpb.ColumnType)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				rel.And(IsAlterColumnTypeOp("table-id", "col-id")...),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		},
	)

	// Special cases for removal of column types, which hold references to other
	// descriptors.
	//
	// When the whole table is dropped, we can (and in fact, should) remove these
	// right away in-txn. However, when only the column is dropped but the table
	// remains, we need to wait until the column is DELETE_ONLY, which happens
	// post-commit because of the need to uphold the 2-version invariant.
	//
	// We distinguish the two cases using a flag in ColumnType which is set iff
	// the parent relation is dropped. This is a dirty hack, ideally we should be
	// able to express the _absence_ of a target element as a query clause.
	//
	// Note that DEFAULT and ON UPDATE expressions are column-dependent elements
	// which also hold references to other descriptors. The rules prior to this one
	// ensures that they transition to ABSENT before scpb.ColumnType does.
	registerDepRule(
		"column type removed right before column when not dropping relation",
		scgraph.SameStagePrecedence,
		"column-type", "column",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnType)(nil)),
				descriptorIsNotBeingDropped(from.El),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		},
	)

	// These rules ensure that computed column expressions are dropped before
	// the columns they depend on. Originally, a single rule applied to all
	// computed columns, but this caused deadlocks during DROP COLUMN CASCADE
	// operations with STORED computed columns. The rules are now split by type:
	//
	// 1. VIRTUAL computed columns need this rule due to an optimizer edge case.
	//    The optimizer can evaluate virtual computed expressions during schema
	//    changes, so the dependent column must remain accessible until the
	//    expression is fully dropped.
	//
	// 2. STORED computed columns only need this rule during ALTER COLUMN TYPE
	//    operations, where temporary expressions map old rows to new column types.
	//    For regular DROP COLUMN operations, applying this rule creates deadlocks
	//    as the optimizer doesn't evaluate stored expressions during drops.
	registerDepRuleForDrop(
		"Virtual computed column expression is dropped before the column it depends on",
		scgraph.Precedence,
		"virtual-column-expr", "column",
		scpb.Status_ABSENT, scpb.Status_WRITE_ONLY,
		func(from, to NodeVars) rel.Clauses {
			colType := MkNodeVars("column-type")
			colID := rel.Var("col-id")
			return rel.Clauses{
				from.Type((*scpb.ColumnComputeExpression)(nil)),
				to.Type((*scpb.Column)(nil)),
				colType.Type((*scpb.ColumnType)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOnColumnID(from, colType, "table-id", "target-col-id"),
				to.El.AttrEqVar(screl.ColumnID, colID),
				from.ReferencedColumnIDsContains(colID),
				FilterElements("columnTypeIsVirtual", colType, from,
					func(columnType *scpb.ColumnType, computeExpression *scpb.ColumnComputeExpression) bool {
						return columnType.IsVirtual
					}),
			}
		},
	)

	// This rule handles STORED computed column expressions specifically during
	// ALTER COLUMN TYPE operations. When altering a column's type such that a
	// backfill is required, a new version of the column is added, and the old
	// version is dropped. A temporary compute expression is set to map the old
	// rows to the new column type. This expression must be dropped *before*
	// dropping the old column.
	//
	// This rule is intentionally limited to ALTER COLUMN TYPE operations only
	// to avoid the deadlock issues that occur with STORED computed columns
	// during regular DROP COLUMN CASCADE operations.
	registerDepRuleForDrop(
		"Stored computed column expression for alter type is dropped before the column it depends on",
		scgraph.Precedence,
		"stored-column-expr", "column",
		scpb.Status_ABSENT, scpb.Status_WRITE_ONLY,
		func(from, to NodeVars) rel.Clauses {
			colType := MkNodeVars("column-type")
			colID := rel.Var("col-id")
			return rel.Clauses{
				from.Type((*scpb.ColumnComputeExpression)(nil)),
				to.Type((*scpb.Column)(nil)),
				colType.Type((*scpb.ColumnType)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOnColumnID(from, colType, "table-id", "target-col-id"),
				rel.And(IsAlterColumnTypeOp("table-id", "target-col-id")...),
				to.El.AttrEqVar(screl.ColumnID, colID),
				from.ReferencedColumnIDsContains(colID),
				FilterElements("columnTypeIsStored", colType, from,
					func(columnType *scpb.ColumnType, computeExpression *scpb.ColumnComputeExpression) bool {
						return !columnType.IsVirtual
					}),
			}
		},
	)

	// Column constraint disappears in the same stage as the column
	// becomes WRITE_ONLY.
	//
	// Column constraint cannot disappear while the column is still publicly writable
	// because we then allow incorrect writes that would violate the constraint.
	//
	// Column constraint cannot still be enforced when the column becomes
	// non-public because an enforced constraint means writes will see and
	// attempt to uphold it but the column is no longer visible to them.
	//
	// N.B. This rule supersedes the above "dependents removed before column" rule.
	// N.B. SameStage is enough; which transition happens first won't matter.
	registerDepRuleForDrop(
		"column constraint removed right before column reaches write only",
		scgraph.Precedence,
		"column-constraint", "column",
		scpb.Status_ABSENT, scpb.Status_WRITE_ONLY,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, isSubjectTo2VersionInvariant),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)

	// This rule enforces that a new primary index moves to the public stage only after all columns stored
	// within the old primary index move to WRITE_ONLY. Without this, the new primary index is at risk of not
	// storing all public columns within the table (as the column being dropped is still considered public
	// before it moves to WRITE_ONLY but the new primary index does not contain it since the schema changer
	// knows it is transitioning to a target status of ABSENT).
	//
	// This rule applies only when the operation is not ALTER COLUMN TYPE. A variant of this rule follows,
	// allowing added and dropped columns to be swapped in the same stage during ALTER COLUMN TYPE.
	registerDepRule(
		"New primary index should go public only after columns being dropped move to WRITE_ONLY",
		scgraph.Precedence,
		"column", "new-primary-index",
		func(from, to NodeVars) rel.Clauses {
			ic := MkNodeVars("index-column")
			relationID, columnID, indexID := rel.Var("table-id"), rel.Var("old-column-id"), rel.Var("index-id")
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				ColumnInSourcePrimaryIndex(ic, to, relationID, columnID, indexID),
				JoinOnColumnID(ic, from, relationID, columnID),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_WRITE_ONLY),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
				IsNotDroppedColumnPartOfAlterColumnTypeOp("table-id", "old-column-id"),
			}
		},
	)

	// This rule is similar to the previous one but applies specifically to ALTER COLUMN ... TYPE operations.
	// It uses SameStagePrecedence to enable the swapping of dropped and added columns within the same stage.
	registerDepRule(
		"New primary index for alter column type should go public in the same stage as dropped column",
		scgraph.SameStagePrecedence,
		"column", "new-primary-index",
		func(from, to NodeVars) rel.Clauses {
			ic := MkNodeVars("index-column")
			relationID, columnID, indexID := rel.Var("table-id"), rel.Var("old-column-id"), rel.Var("index-id")
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				ColumnInSourcePrimaryIndex(ic, to, relationID, columnID, indexID),
				JoinOnColumnID(ic, from, relationID, columnID),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_WRITE_ONLY),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
				rel.And(IsDroppedColumnPartOfAlterColumnTypeOp("table-id", "old-column-id")...),
			}
		},
	)
}

// Special rules partial predicate expressions, which ensure that any columns
// used by them are not cleaned up before the partial index peredicate is
// removed.
func init() {
	registerDepRuleForDrop(
		"secondary index partial no longer public before referenced column",
		scgraph.Precedence,
		"secondary-partial-index", "column",
		scpb.Status_DELETE_ONLY, scpb.Status_WRITE_ONLY,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				JoinOnDescID(from, to, "table-id"),
				descriptorIsNotBeingDropped(from.El),
				FilterElements("secondaryIndexReferencesColumn", from, to,
					func(index *scpb.SecondaryIndex, column *scpb.Column) bool {
						if index.EmbeddedExpr == nil {
							return false
						}
						for _, refColumns := range index.EmbeddedExpr.ReferencedColumnIDs {
							if refColumns == column.ColumnID {
								return true
							}
						}
						return false
					}),
			}
		},
	)
}

// Special rules to ensure that swapping default expressions is done in order.
func init() {
	registerDepRule(
		"handle default column expression swaps",
		scgraph.Precedence,
		"old-column-expression", "new-column-expression",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnDefaultExpression)(nil), (*scpb.ColumnOnUpdateExpression)(nil)),
				to.Type((*scpb.ColumnDefaultExpression)(nil), (*scpb.ColumnOnUpdateExpression)(nil)),
				from.El.AttrEqVar(rel.Type, "same-type"),
				to.El.AttrEqVar(rel.Type, "same-type"),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
