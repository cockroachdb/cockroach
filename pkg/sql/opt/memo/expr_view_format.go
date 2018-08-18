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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// ExprFmtInterceptor is a callback that can be set to a custom formatting
// function. If the function returns true, the normal formatting code is bypassed.
var ExprFmtInterceptor func(f *ExprFmtCtx, tp treeprinter.Node, ev ExprView) bool

// ExprFmtFlags controls which properties of the expression are shown in
// formatted output.
type ExprFmtFlags int

const (
	// ExprFmtShowAll shows all properties of the expression.
	ExprFmtShowAll ExprFmtFlags = 0

	// ExprFmtHideMiscProps does not show outer columns, row cardinality, or
	// side effects in the output.
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

// ExprFmtCtx contains data relevant to formatting routines.
type ExprFmtCtx struct {
	Buffer *bytes.Buffer

	// Flags controls how the expression is formatted.
	Flags ExprFmtFlags

	// Memo must contain any expression that is formatted.
	Memo *Memo
}

// MakeExprFmtCtx creates an expression formatting context from an existing
// buffer.
func MakeExprFmtCtx(buf *bytes.Buffer, flags ExprFmtFlags, mem *Memo) ExprFmtCtx {
	return ExprFmtCtx{Buffer: buf, Flags: flags, Memo: mem}
}

// HasFlags tests whether the given flags are all set.
func (f *ExprFmtCtx) HasFlags(subset ExprFmtFlags) bool {
	return f.Flags.HasFlags(subset)
}

// format constructs a treeprinter view of this expression for testing and
// debugging. The given flags control which properties are added.
func (ev ExprView) format(f *ExprFmtCtx, tp treeprinter.Node) {
	if ExprFmtInterceptor != nil && ExprFmtInterceptor(f, tp, ev) {
		return
	}
	if ev.IsScalar() {
		ev.formatScalar(f, tp)
	} else {
		ev.formatRelational(f, tp)
	}
}

func (ev ExprView) formatRelational(f *ExprFmtCtx, tp treeprinter.Node) {
	logProps := ev.Logical()
	var physProps *props.Physical
	if ev.best == normBestOrdinal {
		physProps = &props.Physical{}
	} else {
		physProps = ev.Physical()
	}

	// Special cases for merge-join and lookup-join: we want the type of the join
	// to show up first.
	f.Buffer.Reset()
	switch ev.Operator() {
	case opt.MergeJoinOp:
		def := ev.Child(2).Private().(*MergeOnDef)
		fmt.Fprintf(f.Buffer, "%v (merge)", def.JoinType)

	case opt.LookupJoinOp:
		def := ev.Private().(*LookupJoinDef)
		fmt.Fprintf(f.Buffer, "%v (lookup", def.JoinType)
		formatPrivate(f, def, physProps)
		f.Buffer.WriteByte(')')

	case opt.ScanOp, opt.VirtualScanOp, opt.IndexJoinOp, opt.ShowTraceForSessionOp:
		fmt.Fprintf(f.Buffer, "%v", ev.op)
		formatPrivate(f, ev.Private(), physProps)

	default:
		fmt.Fprintf(f.Buffer, "%v", ev.op)
	}

	tp = tp.Child(f.Buffer.String())

	// If a particular column presentation is required of the expression, then
	// print columns using that information.
	if !physProps.Presentation.Any() {
		ev.formatPresentation(f, tp, physProps.Presentation)
	} else {
		// Special handling to improve the columns display for certain ops.
		switch ev.Operator() {
		case opt.ProjectOp:
			// We want the synthesized column IDs to map 1-to-1 to the projections,
			// and the pass-through columns at the end.

			// Get the list of columns from the ProjectionsOp, which has the natural
			// order.
			def := ev.Child(1).Private().(*ProjectionsOpDef)
			colList := append(opt.ColList(nil), def.SynthesizedCols...)
			// Add pass-through columns.
			def.PassthroughCols.ForEach(func(i int) {
				colList = append(colList, opt.ColumnID(i))
			})

			ev.formatColList(f, tp, "columns:", colList)

		case opt.ValuesOp:
			colList := ev.Private().(opt.ColList)
			ev.formatColList(f, tp, "columns:", colList)

		case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
			opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
			colMap := ev.Private().(*SetOpColMap)
			ev.formatColList(f, tp, "columns:", colMap.Out)

		default:
			// Fall back to writing output columns in column id order, with
			// best guess label.
			ev.formatColSet(f, tp, "columns:", logProps.Relational.OutputCols)
		}
	}

	switch ev.Operator() {
	// Special-case handling for GroupBy private; print grouping columns
	// and internal ordering in addition to full set of columns.
	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		def := ev.Private().(*GroupByDef)
		if !def.GroupingCols.Empty() {
			ev.formatColSet(f, tp, "grouping columns:", def.GroupingCols)
		}
		if !def.Ordering.Any() {
			tp.Childf("internal-ordering: %s", def.Ordering)
		}

	case opt.LimitOp, opt.OffsetOp:
		if ord := ev.Private().(*props.OrderingChoice); !ord.Any() {
			tp.Childf("internal-ordering: %s", ord)
		}

		// Special-case handling for set operators to show the left and right
		// input columns that correspond to the output columns.
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		colMap := ev.Private().(*SetOpColMap)
		ev.formatColList(f, tp, "left columns:", colMap.Left)
		ev.formatColList(f, tp, "right columns:", colMap.Right)

	case opt.ScanOp:
		def := ev.Private().(*ScanOpDef)
		if def.Constraint != nil {
			tp.Childf("constraint: %s", def.Constraint)
		}
		if def.HardLimit.IsSet() {
			tp.Childf("limit: %s", def.HardLimit)
		}
		if !def.Flags.Empty() {
			if def.Flags.NoIndexJoin {
				tp.Childf("flags: no-index-join")
			} else if def.Flags.ForceIndex {
				idx := ev.Metadata().Table(def.Table).Index(def.Flags.Index)
				tp.Childf("flags: force-index=%s", idx.IdxName())
			}
		}

	case opt.LookupJoinOp:
		def := ev.Private().(*LookupJoinDef)
		idxCols := make(opt.ColList, len(def.KeyCols))
		idx := ev.mem.metadata.Table(def.Table).Index(def.Index)
		for i := range idxCols {
			idxCols[i] = def.Table.ColumnID(idx.Column(i).Ordinal)
		}
		tp.Childf("key columns: %v = %v", def.KeyCols, idxCols)
	}

	if !f.HasFlags(ExprFmtHideMiscProps) {
		if !logProps.Relational.OuterCols.Empty() {
			tp.Childf("outer: %s", logProps.Relational.OuterCols.String())
		}
		if logProps.Relational.Cardinality != props.AnyCardinality {
			// Suppress cardinality for Scan ops if it's redundant with Limit field.
			if !(ev.Operator() == opt.ScanOp && ev.Private().(*ScanOpDef).HardLimit.IsSet()) {
				tp.Childf("cardinality: %s", logProps.Relational.Cardinality)
			}
		}
		if logProps.Relational.CanHaveSideEffects {
			tp.Child("side-effects")
		}
	}

	if !f.HasFlags(ExprFmtHideStats) {
		tp.Childf("stats: %s", &logProps.Relational.Stats)
	}

	if !f.HasFlags(ExprFmtHideCost) && ev.best != normBestOrdinal {
		tp.Childf("cost: %.9g", ev.bestExpr().cost)
	}

	// Format functional dependencies.
	if !f.HasFlags(ExprFmtHideFuncDeps) {
		// Show the key separately from the rest of the FDs. Do this by copying
		// the FD to the stack (fast shallow copy), and then calling ClearKey.
		fd := logProps.Relational.FuncDeps
		key, ok := fd.Key()
		if ok {
			tp.Childf("key: %s", key)
		}
		fd.ClearKey()
		if !fd.Empty() {
			tp.Childf("fd: %s", fd)
		}
	}

	if !physProps.Ordering.Any() {
		tp.Childf("ordering: %s", physProps.Ordering.String())
	}

	if !f.HasFlags(ExprFmtHideRuleProps) {
		r := &logProps.Relational.Rule
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

	for i := 0; i < ev.ChildCount(); i++ {
		ev.Child(i).format(f, tp)
	}
}

func (ev ExprView) formatScalar(f *ExprFmtCtx, tp treeprinter.Node) {
	// Omit empty ProjectionsOp and AggregationsOp.
	if (ev.op == opt.ProjectionsOp || ev.op == opt.AggregationsOp) &&
		ev.ChildCount() == 0 {
		return
	}
	if ev.op == opt.MergeOnOp {
		tp = tp.Childf("%v", ev.op)
		def := ev.Private().(*MergeOnDef)
		tp.Childf("left ordering: %s", def.LeftEq)
		tp.Childf("right ordering: %s", def.RightEq)
	} else {
		f.Buffer.Reset()
		fmt.Fprintf(f.Buffer, "%v", ev.op)

		ev.formatScalarPrivate(f, ev.Private())
		ev.FormatScalarProps(f)
		tp = tp.Child(f.Buffer.String())
	}
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		child.format(f, tp)
	}
}

// FormatScalarProps writes out a string representation of the scalar
// properties (with a preceding space); for example:
//  " [type=bool, outer=(1), constraints=(/1: [/1 - /1]; tight)]"
func (ev ExprView) FormatScalarProps(f *ExprFmtCtx) {
	// Don't panic if scalar properties don't yet exist when printing
	// expression.
	scalar := ev.Logical().Scalar
	if scalar == nil {
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

		switch ev.Operator() {
		case opt.ProjectionsOp, opt.AggregationsOp:
			// Don't show the type of these ops because they are simply tuple
			// types of their children's types, and the types of children are
			// already listed.

		default:
			writeProp("type=%s", scalar.Type)
		}

		if !f.HasFlags(ExprFmtHideMiscProps) {
			if !scalar.OuterCols.Empty() {
				writeProp("outer=%s", scalar.OuterCols)
			}
			if scalar.CanHaveSideEffects {
				writeProp("side-effects")
			}
		}

		if !f.HasFlags(ExprFmtHideConstraints) {
			if scalar.Constraints != nil && !scalar.Constraints.IsUnconstrained() {
				writeProp("constraints=(%s", scalar.Constraints)
				if scalar.TightConstraints {
					f.Buffer.WriteString("; tight")
				}
				f.Buffer.WriteString(")")
			}
		}

		if !f.HasFlags(ExprFmtHideFuncDeps) && !scalar.FuncDeps.Empty() {
			writeProp("fd=%s", scalar.FuncDeps)
		}

		if !first {
			f.Buffer.WriteString("]")
		}
	}
}

func (ev ExprView) formatScalarPrivate(f *ExprFmtCtx, private interface{}) {
	switch ev.op {
	case opt.NullOp, opt.TupleOp:
		// Private is redundant with logical type property.
		private = nil

	case opt.ProjectionsOp, opt.AggregationsOp:
		// The private data of these ops was already used to print the output
		// columns for their containing op (Project or GroupBy), so no need to
		// print again.
		private = nil
	}

	if private != nil {
		f.Buffer.WriteRune(':')
		formatPrivate(f, private, &props.Physical{})
	}
}

func (ev ExprView) formatPresentation(
	f *ExprFmtCtx, tp treeprinter.Node, presentation props.Presentation,
) {
	logProps := ev.Logical()

	f.Buffer.Reset()
	f.Buffer.WriteString("columns:")
	for _, col := range presentation {
		formatCol(f, col.Label, col.ID, logProps.Relational.NotNullCols)
	}
	tp.Child(f.Buffer.String())
}

// formatColSet constructs a new treeprinter child containing the specified set
// of columns formatting using the formatCol method.
func (ev ExprView) formatColSet(
	f *ExprFmtCtx, tp treeprinter.Node, heading string, colSet opt.ColSet,
) {
	if !colSet.Empty() {
		notNullCols := ev.Logical().Relational.NotNullCols
		f.Buffer.Reset()
		f.Buffer.WriteString(heading)
		colSet.ForEach(func(i int) {
			formatCol(f, "", opt.ColumnID(i), notNullCols)
		})
		tp.Child(f.Buffer.String())
	}
}

// formatColList constructs a new treeprinter child containing the specified
// list of columns formatted using the formatCol method.
func (ev ExprView) formatColList(
	f *ExprFmtCtx, tp treeprinter.Node, heading string, colList opt.ColList,
) {
	if len(colList) > 0 {
		notNullCols := ev.Logical().Relational.NotNullCols
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

func formatPrivate(f *ExprFmtCtx, private interface{}, physProps *props.Physical) {
	if private == nil {
		return
	}
	switch t := private.(type) {
	case *ScanOpDef:
		// Don't output name of index if it's the primary index.
		tab := f.Memo.metadata.Table(t.Table)
		if t.Index == opt.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", tab.Name().TableName)
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", tab.Name().TableName, tab.Index(t.Index).IdxName())
		}
		if _, reverse := t.CanProvideOrdering(f.Memo.Metadata(), &physProps.Ordering); reverse {
			f.Buffer.WriteString(",rev")
		}

	case *VirtualScanOpDef:
		tab := f.Memo.metadata.Table(t.Table)
		fmt.Fprintf(f.Buffer, " %s", tab.Name())

	case *RowNumberDef:
		if !t.Ordering.Any() {
			fmt.Fprintf(f.Buffer, " ordering=%s", t.Ordering)
		}

	case *GroupByDef:
		fmt.Fprintf(f.Buffer, " cols=%s", t.GroupingCols.String())
		if !t.Ordering.Any() {
			fmt.Fprintf(f.Buffer, ",ordering=%s", t.Ordering)
		}

	case opt.ColumnID:
		fullyQualify := !f.HasFlags(ExprFmtHideQualifications)
		label := f.Memo.metadata.QualifiedColumnLabel(t, fullyQualify)
		fmt.Fprintf(f.Buffer, " %s", label)

	case *IndexJoinDef:
		tab := f.Memo.metadata.Table(t.Table)
		fmt.Fprintf(f.Buffer, " %s", tab.Name().TableName)

	case *LookupJoinDef:
		tab := f.Memo.metadata.Table(t.Table)
		if t.Index == opt.PrimaryIndex {
			fmt.Fprintf(f.Buffer, " %s", tab.Name().TableName)
		} else {
			fmt.Fprintf(f.Buffer, " %s@%s", tab.Name().TableName, tab.Index(t.Index).IdxName())
		}

	case *MergeOnDef:
		fmt.Fprintf(f.Buffer, " %s,%s,%s", t.JoinType, t.LeftEq, t.RightEq)

	case *props.OrderingChoice:
		if !t.Any() {
			fmt.Fprintf(f.Buffer, " ordering=%s", t)
		}

	case *ExplainOpDef, *ProjectionsOpDef, opt.ColSet, opt.ColList, *SetOpColMap, types.T:
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
