// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/errors"
)

// scanConstraintFields returns the HasConstraint and HasLimit fields for a
// ScanPrivate.
func scanConstraintFields(sp *ScanPrivate) []physical.PlanGramField {
	return []physical.PlanGramField{
		{
			Key: "HasConstraint",
			Val: strconv.FormatBool(
				(sp.Constraint != nil && !sp.Constraint.IsUnconstrained()) ||
					sp.InvertedConstraint != nil,
			),
		},
		{Key: "HasLimit", Val: strconv.FormatBool(sp.HardLimit.IsSet())},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *ScanExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	fields := []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
	}
	return append(fields, scanConstraintFields(&e.ScanPrivate)...)
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *PlaceholderScanExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	fields := []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
	}
	return append(fields, scanConstraintFields(&e.ScanPrivate)...)
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *IndexJoinExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(0).Name())},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *LookupJoinExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
		{Key: "JoinType", Val: joinTypeFieldVal(e.JoinType)},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *InvertedJoinExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
		{Key: "JoinType", Val: joinTypeFieldVal(e.JoinType)},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *ZigzagJoinExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	leftTab := md.Table(e.LeftTable)
	rightTab := md.Table(e.RightTable)
	return []physical.PlanGramField{
		{Key: "LeftTable", Val: string(leftTab.Name())},
		{Key: "RightTable", Val: string(rightTab.Name())},
		{Key: "LeftIndex", Val: string(leftTab.Index(e.LeftIndex).Name())},
		{Key: "RightIndex", Val: string(rightTab.Index(e.RightIndex).Name())},
		{Key: "JoinType", Val: joinTypeFieldVal(opt.InnerJoinOp)},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *VectorSearchExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
		{
			Key: "HasConstraint",
			Val: strconv.FormatBool(
				e.PrefixConstraint != nil && !e.PrefixConstraint.IsUnconstrained(),
			),
		},
		{Key: "HasLimit", Val: "true"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *VectorMutationSearchExpr) PlanGramMatchableFields(
	md *opt.Metadata,
) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *InsertExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "Table", Val: string(md.Table(e.Table).Name())},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *UpdateExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "Table", Val: string(md.Table(e.Table).Name())},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *UpsertExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "Table", Val: string(md.Table(e.Table).Name())},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *DeleteExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "Table", Val: string(md.Table(e.Table).Name())},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *LockExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "Table", Val: string(md.Table(e.Table).Name())},
	}
}

// joinTypeFieldVal returns the short lowercase name for a join operator.
func joinTypeFieldVal(op opt.Operator) string {
	switch op {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return "inner"
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return "left"
	case opt.RightJoinOp:
		return "right"
	case opt.FullJoinOp:
		return "full"
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return "semi"
	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return "anti"
	default:
		panic(errors.AssertionFailedf("unexpected join operator %s", op))
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *MergeJoinExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: joinTypeFieldVal(e.JoinType)},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *InnerJoinExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "inner"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *LeftJoinExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "left"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *RightJoinExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "right"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *FullJoinExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "full"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *SemiJoinExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "semi"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *AntiJoinExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "anti"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *InnerJoinApplyExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "inner"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *LeftJoinApplyExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "left"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *SemiJoinApplyExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "semi"},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *AntiJoinApplyExpr) PlanGramMatchableFields(*opt.Metadata) []physical.PlanGramField {
	return []physical.PlanGramField{
		{Key: "JoinType", Val: "anti"},
	}
}
