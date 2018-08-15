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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// format constructs a treeprinter view of this expression for testing and
// debugging. The given flags control which properties are added.
func (ev ExprView) format(f *opt.ExprFmtCtx, tp treeprinter.Node) {
	if ExprFmtInterceptor != nil && ExprFmtInterceptor(f, tp, ev) {
		return
	}
	if ev.IsScalar() {
		ev.formatScalar(f, tp)
	} else {
		ev.formatRelational(f, tp)
	}
}

func (ev ExprView) formatRelational(f *opt.ExprFmtCtx, tp treeprinter.Node) {
	var buf bytes.Buffer
	formatter := ev.mem.makeExprFormatter(&buf)

	logProps := ev.Logical()
	var physProps *props.Physical
	if ev.best == normBestOrdinal {
		physProps = &props.Physical{}
	} else {
		physProps = ev.Physical()
	}

	// Special cases for merge-join and lookup-join: we want the type of the join
	// to show up first.
	switch ev.Operator() {
	case opt.MergeJoinOp:
		def := ev.Child(2).Private().(*MergeOnDef)
		fmt.Fprintf(&buf, "%v (merge)", def.JoinType)

	case opt.LookupJoinOp:
		def := ev.Private().(*LookupJoinDef)
		fmt.Fprintf(&buf, "%v (lookup", def.JoinType)
		formatter.formatPrivate(def, physProps, formatNormal)
		buf.WriteByte(')')

	case opt.ScanOp, opt.VirtualScanOp, opt.IndexJoinOp, opt.ShowTraceForSessionOp:
		fmt.Fprintf(&buf, "%v", ev.op)
		formatter.formatPrivate(ev.Private(), physProps, formatNormal)

	default:
		fmt.Fprintf(&buf, "%v", ev.op)
	}

	tp = tp.Child(buf.String())

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

			logProps.FormatColList(f, tp, "columns:", colList)

		case opt.ValuesOp:
			colList := ev.Private().(opt.ColList)
			logProps.FormatColList(f, tp, "columns:", colList)

		case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
			opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
			colMap := ev.Private().(*SetOpColMap)
			logProps.FormatColList(f, tp, "columns:", colMap.Out)

		default:
			// Fall back to writing output columns in column id order, with
			// best guess label.
			logProps.FormatColSet(f, tp, "columns:", logProps.Relational.OutputCols)
		}
	}

	switch ev.Operator() {
	// Special-case handling for GroupBy private; print grouping columns
	// and internal ordering in addition to full set of columns.
	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		def := ev.Private().(*GroupByDef)
		if !def.GroupingCols.Empty() {
			logProps.FormatColSet(f, tp, "grouping columns:", def.GroupingCols)
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
		logProps.FormatColList(f, tp, "left columns:", colMap.Left)
		logProps.FormatColList(f, tp, "right columns:", colMap.Right)

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
		buf.Reset()
		idxCols := make(opt.ColList, len(def.KeyCols))
		idx := ev.mem.metadata.Table(def.Table).Index(def.Index)
		for i := range idxCols {
			idxCols[i] = def.Table.ColumnID(idx.Column(i).Ordinal)
		}
		tp.Childf("key columns: %v = %v", def.KeyCols, idxCols)
	}

	if !f.HasFlags(opt.ExprFmtHideMiscProps) {
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

	if !f.HasFlags(opt.ExprFmtHideStats) {
		tp.Childf("stats: %s", &logProps.Relational.Stats)
	}

	if !f.HasFlags(opt.ExprFmtHideCost) && ev.best != normBestOrdinal {
		tp.Childf("cost: %.9g", ev.bestExpr().cost)
	}

	// Format functional dependencies.
	if !f.HasFlags(opt.ExprFmtHideFuncDeps) {
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

	if !f.HasFlags(opt.ExprFmtHideRuleProps) {
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

func (ev ExprView) formatScalar(f *opt.ExprFmtCtx, tp treeprinter.Node) {
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
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%v", ev.op)

		ev.formatScalarPrivate(&buf, ev.Private())
		ev.FormatScalarProps(f, &buf)
		tp = tp.Child(buf.String())
	}
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		child.format(f, tp)
	}
}

// FormatScalarProps writes out a string representation of the scalar
// properties (with a preceding space); for example:
//  " [type=bool, outer=(1), constraints=(/1: [/1 - /1]; tight)]"
func (ev ExprView) FormatScalarProps(f *opt.ExprFmtCtx, buf *bytes.Buffer) {
	// Don't panic if scalar properties don't yet exist when printing
	// expression.
	scalar := ev.Logical().Scalar
	if scalar == nil {
		buf.WriteString(" [type=undefined]")
	} else {
		first := true
		writeProp := func(format string, args ...interface{}) {
			if first {
				buf.WriteString(" [")
				first = false
			} else {
				buf.WriteString(", ")
			}
			fmt.Fprintf(buf, format, args...)
		}

		switch ev.Operator() {
		case opt.ProjectionsOp, opt.AggregationsOp:
			// Don't show the type of these ops because they are simply tuple
			// types of their children's types, and the types of children are
			// already listed.

		default:
			writeProp("type=%s", scalar.Type)
		}

		if !f.HasFlags(opt.ExprFmtHideMiscProps) {
			if !scalar.OuterCols.Empty() {
				writeProp("outer=%s", scalar.OuterCols)
			}
			if scalar.CanHaveSideEffects {
				writeProp("side-effects")
			}
		}

		if !f.HasFlags(opt.ExprFmtHideConstraints) {
			if scalar.Constraints != nil && !scalar.Constraints.IsUnconstrained() {
				writeProp("constraints=(%s", scalar.Constraints)
				if scalar.TightConstraints {
					buf.WriteString("; tight")
				}
				buf.WriteString(")")
			}
		}

		if !f.HasFlags(opt.ExprFmtHideFuncDeps) && !scalar.FuncDeps.Empty() {
			writeProp("fd=%s", scalar.FuncDeps)
		}

		if !first {
			buf.WriteString("]")
		}
	}
}

func (ev ExprView) formatScalarPrivate(buf *bytes.Buffer, private interface{}) {
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
		buf.WriteRune(':')
		formatter := ev.mem.makeExprFormatter(buf)
		formatter.formatPrivate(private, &props.Physical{}, formatNormal)
	}
}

func (ev ExprView) formatPresentation(
	f *opt.ExprFmtCtx, tp treeprinter.Node, presentation props.Presentation,
) {
	logProps := ev.Logical()

	var buf bytes.Buffer
	buf.WriteString("columns:")
	for _, col := range presentation {
		logProps.FormatCol(f, &buf, col.Label, col.ID)
	}
	tp.Child(buf.String())
}
