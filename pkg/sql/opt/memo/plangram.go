// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *ScanExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *PlaceholderScanExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
	}
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
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *InvertedJoinExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
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
	}
}

// PlanGramMatchableFields implements physical.PlanGramFieldMatchableExpr.
func (e *VectorSearchExpr) PlanGramMatchableFields(md *opt.Metadata) []physical.PlanGramField {
	tab := md.Table(e.Table)
	return []physical.PlanGramField{
		{Key: "Table", Val: string(tab.Name())},
		{Key: "Index", Val: string(tab.Index(e.Index).Name())},
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
