// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package memo

import (
	"bytes"
	"fmt"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// ExprFmtInterceptor is a callback that can be set to a custom formatting
// function. If the function returns true, the normal formatting code is
// bypassed.
var ExprFmtInterceptor func(f *ExprFmtCtx, tp treeprinter.Node, nd opt.Expr) bool

// ExprFmtFlags controls which properties of the expression are shown in
// formatted output.
type ExprFmtFlags int

const (
	// ExprFmtShowAll shows all properties of the expression.
	ExprFmtShowAll ExprFmtFlags = 0

	// ExprFmtHideMiscProps does not show outer columns, row cardinality, provided
	// orderings, or side effects in the output.
	ExprFmtHideMiscProps ExprFmtFlags = 1 << (iota - 1)

	// ExprFmtHideConstraints does not show inferred constraints in the output.
	ExprFmtHideConstraints

	// ExprFmtHideFuncDeps does not show functional dependencies in the output.
	ExprFmtHideFuncDeps

	// ExprFmtHideRuleProps does not show rule-specific properties in the output.
	ExprFmtHideRuleProps

	// ExprFmtHideStats does not show statistics in the output.
	ExprFmtHideStats

	// ExprFmtHideCost does not show expression cost in the output.
	ExprFmtHideCost

	// ExprFmtHideQualifications removes the qualification from column labels
	// (except when a shortened name would be ambiguous).
	ExprFmtHideQualifications

	// ExprFmtHideScalars removes subtrees that contain only scalars and replaces
	// them with the SQL expression (if possible).
	ExprFmtHideScalars

	// ExprFmtHideAll shows only the most basic properties of the expression.
	ExprFmtHideAll ExprFmtFlags = (1 << iota) - 1
)

// HasFlags tests whether the given flags are all set.
func (f ExprFmtFlags) HasFlags(subset ExprFmtFlags) bool {
	return f&subset == subset
}

// FormatExpr returns a string representation of the given expression, formatted
// according to the specified flags.
func FormatExpr(e opt.Expr, flags ExprFmtFlags) string {
	var mem *Memo
	if nd, ok := e.(RelExpr); ok {
		mem = nd.Memo()
	}
	f := MakeExprFmtCtx(flags, mem)
	f.FormatExpr(e)
	return f.Buffer.String()
}

// ExprFmtCtx is passed as context to expression formatting functions, which
// need to know the formatting flags and memo in order to format. In addition,
// a reusable bytes buffer avoids unnecessary allocations.
type ExprFmtCtx struct {
	Buffer *bytes.Buffer

	// Flags controls how the expression is formatted.
	Flags ExprFmtFlags

	// Memo must contain any expression that is formatted.
	Memo *Memo
}

// MakeExprFmtCtx creates an expression formatting context from a new buffer.
func MakeExprFmtCtx(flags ExprFmtFlags, mem *Memo) ExprFmtCtx {
	return ExprFmtCtx{Buffer: &bytes.Buffer{}, Flags: flags, Memo: mem}
}

// MakeExprFmtCtxBuffer creates an expression formatting context from an
// existing buffer.
func MakeExprFmtCtxBuffer(buf *bytes.Buffer, flags ExprFmtFlags, mem *Memo) ExprFmtCtx {
	return ExprFmtCtx{Buffer: buf, Flags: flags, Memo: mem}
}

// HasFlags tests whether the given flags are all set.
func (f *ExprFmtCtx) HasFlags(subset ExprFmtFlags) bool {
	return f.Flags.HasFlags(subset)
}

// FormatExpr constructs a treeprinter view of the given expression for testing
// and debugging, according to the flags in this context.
func (f *ExprFmtCtx) FormatExpr(e opt.Expr) {
	tp := treeprinter.New()
	f.formatExpr(e, tp)
	f.Buffer.Reset()
	f.Buffer.WriteString(tp.String())
}

func (f *ExprFmtCtx) formatExpr(e opt.Expr, tp treeprinter.Node) {
	if ExprFmtInterceptor != nil && ExprFmtInterceptor(f, tp, e) {
		return
	}

	scalar, ok := e.(opt.ScalarExpr)
	if ok {
		f.formatScalar(scalar, tp)
	} else {
		f.formatRelational(e.(RelExpr), tp)
	}
}

func (f *ExprFmtCtx) formatRelational(e RelExpr, tp treeprinter.Node) {
	relational := e.Relational()
	required := e.RequiredPhysical()
	if required == nil {
		// required can be nil before optimization has taken place.
		required = physical.MinRequired
	}

	// Special cases for merge-join and lookup-join: we want the type of the join
	// to show up first.
	f.Buffer.Reset()
	switch t := e.(type) {
	case *MergeJoinExpr:
		fmt.Fprintf(f.Buffer, "%v (merge)", t.JoinType)

	case *LookupJoinExpr:
		fmt.Fprintf(f.Buffer, "%v (lookup", t.JoinType)
		FormatPrivate(f, e.Private(), required)
		f.Buffer.WriteByte(')')

	case *ZigzagJoinExpr:
		fmt.Fprintf(f.Buffer, "%v (zigzag", t.JoinType)
		FormatPrivate(f, e.Private(), required)
		f.Buffer.WriteByte(')')

	case *ScanExpr, *VirtualScanExpr, *IndexJoinExpr, *ShowTraceForSessionExpr:
		fmt.Fprintf(f.Buffer, "%v", e.Op())
		FormatPrivate(f, e.Private(), required)

	default:
		fmt.Fprintf(f.Buffer, "%v", e.Op())
	}

	tp = tp.Child(f.Buffer.String())

	var colList opt.ColList
	// Special handling to improve the columns display for certain ops.
	switch t := e.(type) {
	case *ProjectExpr:
		// We want the synthesized column IDs to map 1-to-1 to the projections,
		// and the pass-through columns at the end.

		// Get the list of columns from the ProjectionsOp, which has the natural
		// order.
		for i := range t.Projections {
			colList = append(colList, t.Projections[i].Col)
		}

		// Add pass-through columns.
		t.Passthrough.ForEach(func(i int) {
			colList = append(colList, opt.ColumnID(i))
		})

	case *ValuesExpr:
		colList = t.Cols

	case *UnionExpr, *IntersectExpr, *ExceptExpr,
		*UnionAllExpr, *IntersectAllExpr, *ExceptAllExpr:
		colList = e.Private().(*SetPrivate).OutCols

	default:
		// Fall back to writing output columns in column id order.
		colList = opt.ColSetToList(e.Relational().OutputCols)
	}

	f.formatColumns(e, tp, colList, required.Presentation)

	switch t := e.(type) {
	// Special-case handling for GroupBy private; print grouping columns
	// and internal ordering in addition to full set of columns.
	case *GroupByExpr, *ScalarGroupByExpr, *DistinctOnExpr:
		private := e.Private().(*GroupingPrivate)
		if !private.GroupingCols.Empty() {
			f.formatColList(e, tp, "grouping columns:", opt.ColSetToList(private.GroupingCols))
		}
		if !private.Ordering.Any() {
			tp.Childf("internal-ordering: %s", private.Ordering)
		}

	case *LimitExpr:
		if !t.Ordering.Any() {
			tp.Childf("internal-ordering: %s", t.Ordering)
		}

	case *OffsetExpr:
		if !t.Ordering.Any() {
			tp.Childf("internal-ordering: %s", t.Ordering)
		}

	// Special-case handling for set operators to show the left and right
	// input columns that correspond to the output columns.
	case *UnionExpr, *IntersectExpr, *ExceptExpr,
		*UnionAllExpr, *IntersectAllExpr, *ExceptAllExpr:
		private := e.Private().(*SetPrivate)
		f.formatColList(e, tp, "left columns:", private.LeftCols)
		f.formatColList(e, tp, "right columns:", private.RightCols)

	case *ScanExpr:
		if t.Constraint != nil {
			tp.Childf("constraint: %s", t.Constraint)
		}
		if t.HardLimit.IsSet() {
			tp.Childf("limit: %s", t.HardLimit)
		}
		if !t.Flags.Empty() {
			if t.Flags.NoIndexJoin {
				tp.Childf("flags: no-index-join")
			} else if t.Flags.ForceIndex {
				idx := f.Memo.Metadata().Table(t.Table).Index(t.Flags.Index)
				tp.Childf("flags: force-index=%s", idx.IdxName())
			}
		}

	case *LookupJoinExpr:
		idxCols := make(opt.ColList, len(t.KeyCols))
		idx := f.Memo.Metadata().Table(t.Table).Index(t.Index)
		for i := range idxCols {
			idxCols[i] = t.Table.ColumnID(idx.Column(i).Ordinal)
		}
		tp.Childf("key columns: %v = %v", t.KeyCols, idxCols)

	case *ZigzagJoinExpr:
		tp.Childf("eq columns: %v = %v", t.LeftEqCols, t.RightEqCols)

	case *MergeJoinExpr:
		tp.Childf("left ordering: %s", t.LeftEq)
		tp.Childf("right ordering: %s", t.RightEq)
	}

	if !f.HasFlags(ExprFmtHideMiscProps) {
		if !relational.OuterCols.Empty() {
			tp.Childf("outer: %s", relational.OuterCols.String())
		}
		if relational.Cardinality != props.AnyCardinality {
			// Suppress cardinality for Scan ops if it's redundant with Limit field.
			if scan, ok := e.(*ScanExpr); !ok || !scan.HardLimit.IsSet() {
				tp.Childf("cardinality: %s", relational.Cardinality)
			}
		}

		if relational.CanHaveSideEffects {
			tp.Child("side-effects")
		}
		if relational.HasPlaceholder {
			tp.Child("has-placeholder")
		}
	}

	if !f.HasFlags(ExprFmtHideStats) {
		tp.Childf("stats: %s", &relational.Stats)
	}

	if !f.HasFlags(ExprFmtHideCost) {
		cost := e.Cost()
		if cost != 0 {
			tp.Childf("cost: %.9g", cost)
		}
	}

	// Format functional dependencies.
	if !f.HasFlags(ExprFmtHideFuncDeps) {
		// Show the key separately from the rest of the FDs.
		if key, ok := relational.FuncDeps.StrictKey(); ok {
			tp.Childf("key: %s", key)
		} else if key, ok := relational.FuncDeps.LaxKey(); ok {
			tp.Childf("lax-key: %s", key)
		}
		if fdStr := relational.FuncDeps.StringOnlyFDs(); fdStr != "" {
			tp.Childf("fd: %s", fdStr)
		}
	}

	if !required.Ordering.Any() {
		if f.HasFlags(ExprFmtHideMiscProps) {
			tp.Childf("ordering: %s", required.Ordering.String())
		} else {
			// Show the provided ordering as well, unless it's exactly the same.
			provided := e.ProvidedPhysical().Ordering
			reqStr := required.Ordering.String()
			provStr := provided.String()
			if provStr == reqStr {
				tp.Childf("ordering: %s", required.Ordering.String())
			} else {
				tp.Childf("ordering: %s [provided: %s]", required.Ordering.String(), provided.String())
			}
		}
	}

	if !f.HasFlags(ExprFmtHideRuleProps) {
		r := &relational.Rule
		if !r.PruneCols.Empty() {
			tp.Childf("prune: %s", r.PruneCols.String())
		}
		if !r.RejectNullCols.Empty() {
			tp.Childf("reject-nulls: %s", r.RejectNullCols.String())
		}
		if len(r.InterestingOrderings) > 0 {
			tp.Childf("interesting orderings: %s", r.InterestingOrderings.String())
		}
	}

	for i, n := 0, e.ChildCount(); i < n; i++ {
		f.formatExpr(e.Child(i), tp)
	}
}

func (f *ExprFmtCtx) formatScalar(scalar opt.ScalarExpr, tp treeprinter.Node) {
	switch scalar.Op() {
	case opt.ProjectionsOp, opt.AggregationsOp:
		// Omit empty Projections and Aggregations expressions.
		if scalar.ChildCount() == 0 {
			return
		}

	case opt.FiltersOp:
		// Show empty Filters expression as "filters (true)".
		if scalar.ChildCount() == 0 {
			tp.Child("filters (true)")
			return
		}
	}

	// Don't show scalar-list, as it's redundant with its parent.
	if scalar.Op() != opt.ScalarListOp {
		f.Buffer.Reset()
		propsExpr := scalar
		switch scalar.Op() {
		case opt.FiltersItemOp, opt.ProjectionsItemOp, opt.AggregationsItemOp, opt.ZipItemOp:
			// Use properties from the item, but otherwise omit it from output.
			scalar = scalar.Child(0).(opt.ScalarExpr)
		}

		fmt.Fprintf(f.Buffer, "%v", scalar.Op())
		f.formatScalarPrivate(scalar)
		f.FormatScalarProps(propsExpr)
		tp = tp.Child(f.Buffer.String())
	}

	for i, n := 0, scalar.ChildCount(); i < n; i++ {
		f.formatExpr(scalar.Child(i), tp)
	}
}

// FormatScalarProps writes out a string representation of the scalar
// properties (with a preceding space); for example:
//  " [type=bool, outer=(1), constraints=(/1: [/1 - /1]; tight)]"
func (f *ExprFmtCtx) FormatScalarProps(scalar opt.ScalarExpr) {
	// Don't panic if scalar properties don't yet exist when printing
	// expression.
	typ := scalar.DataType()
	if typ == nil {
		f.Buffer.WriteString(" [type=undefined]")
	} else {
		first := true
		writeProp := func(format string, args ...interface{}) {
			if first {
				f.Buffer.WriteString(" [")
				first = false
			} else {
				f.Buffer.WriteString(", ")
			}
			fmt.Fprintf(f.Buffer, format, args...)
		}

		if typ != types.Any {
			writeProp("type=%s", typ)
		}

		if propsExpr, ok := scalar.(ScalarPropsExpr); ok && f.Memo != nil {
			scalarProps := propsExpr.ScalarProps(f.Memo)
			if !f.HasFlags(ExprFmtHideMiscProps) {
				if !scalarProps.OuterCols.Empty() {
					writeProp("outer=%s", scalarProps.OuterCols)
				}
				if scalarProps.CanHaveSideEffects {
					writeProp("side-effects")
				}
			}

			if !f.HasFlags(ExprFmtHideConstraints) {
				if scalarProps.Constraints != nil && !scalarProps.Constraints.IsUnconstrained() {
					writeProp("constraints=(%s", scalarProps.Constraints)
					if scalarProps.TightConstraints {
						f.Buffer.WriteString("; tight")
					}
					f.Buffer.WriteString(")")
				}
			}

			if !f.HasFlags(ExprFmtHideFuncDeps) && !scalarProps.FuncDeps.Empty() {
				writeProp("fd=%s", scalarProps.FuncDeps)
			}
		}

		if !first {
			f.Buffer.WriteString("]")
		}
	}
}

func (f *ExprFmtCtx) formatScalarPrivate(scalar opt.ScalarExpr) {
	var private interface{}
	switch t := scalar.(type) {
	case *NullExpr, *TupleExpr:
		// Private is redundant with logical type property.
		private = nil

	case *ProjectionsExpr, *AggregationsExpr:
		// The private data of these ops was already used to print the output
		// columns for their containing op (Project or GroupBy), so no need to
		// print again.
		private = nil

	case *AnyExpr:
		// We don't want to show the OriginalExpr; just show Cmp.
		private = t.Cmp

	case *SubqueryExpr, *ExistsExpr:
		// We don't want to show the OriginalExpr.
		private = nil

	default:
		private = scalar.Private()
	}

	if private != nil {
		f.Buffer.WriteRune(':')
		FormatPrivate(f, private, &physical.Required{})
	}
}

func (f *ExprFmtCtx) formatColumns(
	nd RelExpr, tp treeprinter.Node, cols opt.ColList, presentation physical.Presentation,
) {
	if presentation.Any() {
		f.formatColList(nd, tp, "columns:", cols)
		return
	}

	// When a particular column presentation is required of the expression, then
	// print columns using that information. Include information about columns
	// that are hidden by the presentation separately.
	hidden := cols.ToSet()
	notNullCols := nd.Relational().NotNullCols
	f.Buffer.Reset()
	f.Buffer.WriteString("columns:")
	for _, col := range presentation {
		hidden.Remove(int(col.ID))
		formatCol(f, col.Label, col.ID, notNullCols)
	}
	if !hidden.Empty() {
		f.Buffer.WriteString("  [hidden:")
		for _, col := range cols {
			if hidden.Contains(int(col)) {
				formatCol(f, "", col, notNullCols)
			}
		}
		f.Buffer.WriteString("]")
	}
	tp.Child(f.Buffer.String())
}

// formatColList constructs a new treeprinter child containing the specified
// list of columns formatted using the formatCol method.
func (f *ExprFmtCtx) formatColList(
	nd RelExpr, tp treeprinter.Node, heading string, colList opt.ColList,
) {
	if len(colList) > 0 {
		notNullCols := nd.Relational().NotNullCols
		f.Buffer.Reset()
		f.Buffer.WriteString(heading)
		for _, col := range colList {
			formatCol(f, "", col, notNullCols)
		}
		tp.Child(f.Buffer.String())
	}
}

// formatCol outputs the specified column into the context's buffer using the
// following format:
//   label:index(type)
//
// If the column is not nullable, then this is the format:
//   label:index(type!null)
//
// If a label is given, then it is used. Otherwise, a "best effort" label is
// used from query metadata.
func formatCol(f *ExprFmtCtx, label string, id opt.ColumnID, notNullCols opt.ColSet) {
	md := f.Memo.metadata
	if label == "" {
		fullyQualify := !f.HasFlags(ExprFmtHideQualifications)
		label = md.QualifiedColumnLabel(id, fullyQualify)
	}

	if !isSimpleColumnName(label) {
		// Add quotations around the column name if it is not composed of simple
		// ASCII characters.
		label = "\"" + label + "\""
	}

	typ := md.ColumnType(id)
	f.Buffer.WriteByte(' ')
	f.Buffer.WriteString(label)
	f.Buffer.WriteByte(':')
	fmt.Fprintf(f.Buffer, "%d", id)
	f.Buffer.WriteByte('(')
	f.Buffer.WriteString(typ.String())

	if notNullCols.Contains(int(id)) {
		f.Buffer.WriteString("!null")
	}
	f.Buffer.WriteByte(')')
}

// ScanIsReverseFn is a callback that is used to figure out if a scan needs to
// happen in reverse (the code lives in the ordering package, and depending on
// that directly would be a dependency loop).
var ScanIsReverseFn func(md *opt.Metadata, s *ScanPrivate, required *physical.OrderingChoice) bool

// FormatPrivate outputs a description of the private to f.Buffer.
func FormatPrivate(f *ExprFmtCtx, private interface{}, physProps *physical.Required) {
	if private == nil {
		return
	}
	switch t := private.(type) {
	case *opt.ColumnID:
		fullyQualify := !f.HasFlags(ExprFmtHideQualifications)
		label := f.Memo.metadata.QualifiedColumnLabel(*t, fullyQualify)
		fmt.Fprintf(f.Buffer, " %s", label)

	case *TupleOrdinal:
		fmt.Fprintf(f.Buffer, " %d", *t)

	case *ScanPrivate:
		// Don't output name of index if it's the primary index.
		tab := f.Memo.metadata.Table(t.Table)
		if t.Index == opt.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", tab.Name().TableName)
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", tab.Name().TableName, tab.Index(t.Index).IdxName())
		}
		if ScanIsReverseFn(f.Memo.Metadata(), t, &physProps.Ordering) {
			f.Buffer.WriteString(",rev")
		}

	case *VirtualScanPrivate:
		tab := f.Memo.metadata.Table(t.Table)
		fmt.Fprintf(f.Buffer, " %s", tab.Name())

	case *RowNumberPrivate:
		if !t.Ordering.Any() {
			fmt.Fprintf(f.Buffer, " ordering=%s", t.Ordering)
		}

	case *GroupingPrivate:
		fmt.Fprintf(f.Buffer, " cols=%s", t.GroupingCols.String())
		if !t.Ordering.Any() {
			fmt.Fprintf(f.Buffer, ",ordering=%s", t.Ordering)
		}

	case *IndexJoinPrivate:
		tab := f.Memo.metadata.Table(t.Table)
		fmt.Fprintf(f.Buffer, " %s", tab.Name().TableName)

	case *LookupJoinPrivate:
		tab := f.Memo.metadata.Table(t.Table)
		if t.Index == opt.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", tab.Name().TableName)
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", tab.Name().TableName, tab.Index(t.Index).IdxName())
		}

	case *ZigzagJoinPrivate:
		leftTab := f.Memo.metadata.Table(t.LeftTable)
		rightTab := f.Memo.metadata.Table(t.RightTable)
		if t.LeftIndex == opt.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", leftTab.Name().TableName)
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", leftTab.Name().TableName, leftTab.Index(t.LeftIndex).IdxName())
		}
		if t.RightIndex == opt.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", rightTab.Name().TableName)
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", rightTab.Name().TableName, rightTab.Index(t.RightIndex).IdxName())
		}

	case *MergeJoinPrivate:
		fmt.Fprintf(f.Buffer, " %s,%s,%s", t.JoinType, t.LeftEq, t.RightEq)

	case *FunctionPrivate:
		fmt.Fprintf(f.Buffer, " %s", t.Name)

	case *physical.OrderingChoice:
		if !t.Any() {
			fmt.Fprintf(f.Buffer, " ordering=%s", t)
		}

	case *ExplainPrivate, *opt.ColSet, *opt.ColList, *SetPrivate, types.T:
		// Don't show anything, because it's mostly redundant.

	default:
		fmt.Fprintf(f.Buffer, " %v", private)
	}
}

// isSimpleColumnName returns true if the given label consists of only ASCII
// letters, numbers, underscores, quotation marks, and periods ("."). It is
// used to determine whether to enclose a column name in quotation marks for
// nicer display.
func isSimpleColumnName(label string) bool {
	for i, r := range label {
		if r > unicode.MaxASCII {
			return false
		}

		if i == 0 {
			if r != '"' && !unicode.IsLetter(r) {
				// The first character must be a letter or quotation mark.
				return false
			}
		} else if r != '.' && r != '_' && r != '"' && !unicode.IsNumber(r) && !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}
