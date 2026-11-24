// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/errors"
)

// DecompileToPlanGram converts a plan gist's explain.Node tree into a
// PlanGram describing the optimizer plans that match the gist. Exec nodes
// that could map to multiple optimizer operators produce an alternation.
//
// The decompiler always emits a structurally-valid PlanGram, so an error
// indicates a bug. The error is returned rather than panicked so callers can
// skip the pinned plan rather than fail the query; a caller may choose to
// panic on it in test builds.
//
// TODO(drewk): the current decompiler only varies the operator across
// alternates — children, tables, and indexes are shared. PlanGram itself
// supports per-alternate children/fields, recursion, and shared nodes, so
// future refinements could exploit those (e.g., PlaceholderScan without an
// Index field, or a single shared production for a sub-plan referenced from
// multiple places).
func DecompileToPlanGram(node *Node) (physical.PlanGram, error) {
	d := &decompiler{b: &physical.PlanGramBuilder{}}
	g, err := d.build(node)
	if err != nil {
		return physical.PlanGram{}, err
	}
	return g, nil
}

// decompiler emits PlanGramBuilder calls for a node tree. Each node is
// visited exactly once; alternation children are promoted to their own
// productions so the alternate rules share them by reference.
type decompiler struct {
	b       *physical.PlanGramBuilder
	counter int
}

// freshName allocates a unique production name.
func (d *decompiler) freshName() string {
	name := "n" + strconv.Itoa(d.counter)
	d.counter++
	return name
}

// build wraps the root production and finalizes the grammar.
func (d *decompiler) build(node *Node) (physical.PlanGram, error) {
	if err := d.b.EnterProduction("root"); err != nil {
		return physical.PlanGram{}, err
	}
	// emit (not emitInProduction): root must have exactly one rule, so an
	// alternation root needs to be a ref to a sibling production.
	if err := d.emit(node); err != nil {
		return physical.PlanGram{}, err
	}
	if err := d.b.LeaveProduction(); err != nil {
		return physical.PlanGram{}, err
	}
	return d.b.Build()
}

// emit emits a node inside an expression context (i.e., as a child of an
// expression). Single-op nodes inline as an expression; alternation nodes
// ref a freshly-named production at this position and declare it via
// emitAltRules.
func (d *decompiler) emit(node *Node) error {
	if wrappedOp, ok := node.wrapperOp(); ok {
		// Wrap the alternation in its own production so both alternates
		// (with and without the wrappedOp) sit at the current child slot.
		if len(node.children) != 1 {
			return errors.AssertionFailedf("wrapper op has %d children, want 1", len(node.children))
		}
		return d.inFreshProduction(func() error {
			return d.emitWrappedAlt(wrappedOp, node.children[0])
		})
	}
	if node.isTransparent() {
		// The node's sole child is emitted in its place.
		if len(node.children) != 1 {
			return errors.AssertionFailedf("transparent op has %d children, want 1", len(node.children))
		}
		return d.emit(node.children[0])
	}
	ops, fields := node.decompile()
	if ops == nil {
		return errors.AssertionFailedf(
			"decompile: no case for execOperator %d; add an explicit case "+
				"(use opt.UnknownOp if no constraint applies)", node.op)
	}
	children, err := node.decompileChildren()
	if err != nil {
		return err
	}
	if len(ops) == 1 {
		return d.emitExpr(ops[0], fields, children)
	}
	return d.inFreshProduction(func() error {
		return d.emitAltRules(ops, fields, children)
	})
}

// emitInProduction emits a node directly as the rule(s) of the enclosing
// production: single-op contributes one rule; alternation contributes one
// rule per alternate.
func (d *decompiler) emitInProduction(node *Node) error {
	if wrappedOp, ok := node.wrapperOp(); ok {
		if len(node.children) != 1 {
			return errors.AssertionFailedf("wrapper op has %d children, want 1", len(node.children))
		}
		return d.emitWrappedAlt(wrappedOp, node.children[0])
	}
	if node.isTransparent() {
		if len(node.children) != 1 {
			return errors.AssertionFailedf("transparent op has %d children, want 1", len(node.children))
		}
		return d.emitInProduction(node.children[0])
	}
	ops, fields := node.decompile()
	if ops == nil {
		return errors.AssertionFailedf(
			"decompile: no case for execOperator %d; add an explicit case "+
				"(use opt.UnknownOp if no constraint applies)", node.op)
	}
	children, err := node.decompileChildren()
	if err != nil {
		return err
	}
	if len(ops) == 1 {
		return d.emitExpr(ops[0], fields, children)
	}
	return d.emitAltRules(ops, fields, children)
}

// inFreshProduction runs emitRules in a freshly-named production, referenced
// at the current builder position.
func (d *decompiler) inFreshProduction(emitRules func() error) error {
	name := d.freshName()
	if err := d.b.RefProduction(name); err != nil {
		return err
	}
	if err := d.b.EnterProduction(name); err != nil {
		return err
	}
	if err := emitRules(); err != nil {
		return err
	}
	return d.b.LeaveProduction()
}

// emitWrappedAlt emits the two rules
//
//	(wrappedOp childRef) | childRef
//
// into the current production, followed by the declaration of childRef. See
// wrapperOp for when this alternation is needed.
func (d *decompiler) emitWrappedAlt(wrappedOp opt.Operator, child *Node) error {
	childName := d.freshName()
	// Rule 1: (wrappedOp childRef).
	if err := d.b.EnterExpr(wrappedOp); err != nil {
		return err
	}
	if err := d.b.RefProduction(childName); err != nil {
		return err
	}
	if err := d.b.LeaveExpr(); err != nil {
		return err
	}
	// Rule 2: childRef (unwrapped).
	if err := d.b.RefProduction(childName); err != nil {
		return err
	}
	// Declare childRef as its own production.
	if err := d.b.EnterProduction(childName); err != nil {
		return err
	}
	if err := d.emitInProduction(child); err != nil {
		return err
	}
	return d.b.LeaveProduction()
}

// emitAltRules emits one expression rule per alternate op into the current
// production. The alternation's children are promoted to their own
// productions and referenced by name from each rule so the children are
// described exactly once.
func (d *decompiler) emitAltRules(
	ops []opt.Operator, fields []physical.PlanGramField, children []*Node,
) error {
	childNames := make([]string, len(children))
	for i := range children {
		childNames[i] = d.freshName()
	}
	for _, op := range ops {
		if err := d.b.EnterExpr(op); err != nil {
			return err
		}
		for _, f := range fields {
			if err := d.b.AddField(f); err != nil {
				return err
			}
		}
		for _, cn := range childNames {
			if err := d.b.RefProduction(cn); err != nil {
				return err
			}
		}
		if err := d.b.LeaveExpr(); err != nil {
			return err
		}
	}
	for i, child := range children {
		if err := d.b.EnterProduction(childNames[i]); err != nil {
			return err
		}
		if err := d.emitInProduction(child); err != nil {
			return err
		}
		if err := d.b.LeaveProduction(); err != nil {
			return err
		}
	}
	return nil
}

// emitExpr emits a single inline expression at the current builder position.
func (d *decompiler) emitExpr(
	op opt.Operator, fields []physical.PlanGramField, children []*Node,
) error {
	if err := d.b.EnterExpr(op); err != nil {
		return err
	}
	for _, f := range fields {
		if err := d.b.AddField(f); err != nil {
			return err
		}
	}
	for _, c := range children {
		if err := d.emit(c); err != nil {
			return err
		}
	}
	return d.b.LeaveExpr()
}

// decompileChildren returns n.children, swapping the inputs of hash and
// merge joins back to optimizer order when execbuild commuted them
// (RightSemi/RightAnti — see execbuilder.buildHashJoin).
func (n *Node) decompileChildren() ([]*Node, error) {
	var jt descpb.JoinType
	switch n.op {
	case hashJoinOp:
		args, ok := n.args.(*hashJoinArgs)
		if !ok {
			return nil, errors.AssertionFailedf("hashJoinOp node has args of type %T", n.args)
		}
		jt = args.JoinType
	case mergeJoinOp:
		args, ok := n.args.(*mergeJoinArgs)
		if !ok {
			return nil, errors.AssertionFailedf("mergeJoinOp node has args of type %T", n.args)
		}
		jt = args.JoinType
	default:
		return n.children, nil
	}
	if jt == descpb.RightSemiJoin || jt == descpb.RightAntiJoin {
		if len(n.children) != 2 {
			return nil, errors.AssertionFailedf("join has %d children, want 2", len(n.children))
		}
		return []*Node{n.children[1], n.children[0]}, nil
	}
	return n.children, nil
}

// wrapperOp reports whether n is an execbuilder-side wrapper that may or may
// not correspond to an optimizer-side wrappedOp at the same position. If
// true, the decompiler emits "(wrappedOp child) | child" so the matcher can
// try both shapes. Today only simpleProjectOp qualifies: execbuilder uses it
// for both real memo.ProjectExprs and for wrappers (applyPresentation,
// buildGroupBy), and the gist can't tell them apart.
func (n *Node) wrapperOp() (wrappedOp opt.Operator, ok bool) {
	if n.op == simpleProjectOp {
		return opt.ProjectOp, true
	}
	return opt.UnknownOp, false
}

// isTransparent reports whether n has no optimizer-side counterpart and should
// be replaced by its sole child during decompilation. serializingProjectOp is
// an execbuilder wrapper (applyPresentation) with no opt op; simpleProjectOp is
// handled by wrapperOp instead.
func (n *Node) isTransparent() bool {
	return n.op == serializingProjectOp
}

// decompile returns the optimizer operator(s) this exec node could represent
// and any associated field constraints (e.g., table and index names). Multiple
// operators indicate alternation (e.g., a scan could be either ScanOp or
// PlaceholderScanOp).
//
// A nil ops slice means the execOperator has no explicit case, which the caller
// turns into an assertion failure: a new op was added to factory.opt without
// being mapped here (see TestDecompileCoversAllOps). Wrapper and transparent
// ops must not reach decompile — emit/emitInProduction check wrapperOp and
// isTransparent first.
func (n *Node) decompile() (ops []opt.Operator, fields []physical.PlanGramField) {
	switch n.op {
	case scanOp:
		// TODO(drewk): consider encoding ScanParams (constrained columns,
		// hard limit, locking) as fields so the PlanGram can distinguish a
		// constrained scan from a full scan.
		args := n.args.(*scanArgs)
		return []opt.Operator{opt.ScanOp, opt.PlaceholderScanOp},
			tableIndexFields(args.Table, args.Index)

	case indexJoinOp:
		return []opt.Operator{opt.IndexJoinOp},
			tableIndexFields(n.args.(*indexJoinArgs).Table, nil /* index */)

	case lookupJoinOp:
		args := n.args.(*lookupJoinArgs)
		return []opt.Operator{opt.LookupJoinOp, opt.LockOp},
			tableIndexFields(args.Table, args.Index)

	case invertedJoinOp:
		args := n.args.(*invertedJoinArgs)
		return []opt.Operator{opt.InvertedJoinOp},
			tableIndexFields(args.Table, args.Index)

	case hashJoinOp:
		joinOp := joinTypeToOp(n.args.(*hashJoinArgs).JoinType)
		return []opt.Operator{joinOp}, nil

	case mergeJoinOp:
		// TODO(drewk): consider specifying the ordering.
		return []opt.Operator{opt.MergeJoinOp}, nil

	case applyJoinOp:
		joinOp := applyJoinTypeToOp(n.args.(*applyJoinArgs).JoinType)
		return []opt.Operator{joinOp}, nil

	case sortOp:
		// TODO(drewk): consider specifying the ordering.
		return []opt.Operator{opt.SortOp}, nil
	case topKOp:
		return []opt.Operator{opt.TopKOp}, nil
	case filterOp:
		return []opt.Operator{opt.SelectOp}, nil
	case invertedFilterOp:
		return []opt.Operator{opt.InvertedFilterOp}, nil
	case valuesOp, literalValuesOp:
		return []opt.Operator{opt.ValuesOp}, nil
	case groupByOp:
		// TODO(drewk): consider specifying the ordering.
		return []opt.Operator{opt.GroupByOp}, nil
	case scalarGroupByOp:
		return []opt.Operator{opt.ScalarGroupByOp}, nil
	case distinctOp:
		return []opt.Operator{opt.DistinctOnOp}, nil
	case limitOp:
		return []opt.Operator{opt.LimitOp}, nil
	case max1RowOp:
		return []opt.Operator{opt.Max1RowOp}, nil
	case ordinalityOp:
		return []opt.Operator{opt.OrdinalityOp}, nil
	case windowOp:
		return []opt.Operator{opt.WindowOp}, nil
	case projectSetOp:
		return []opt.Operator{opt.ProjectSetOp}, nil
	case zigzagJoinOp:
		args := n.args.(*zigzagJoinArgs)
		return []opt.Operator{opt.ZigzagJoinOp}, zigzagFields(args)
	case recursiveCTEOp:
		return []opt.Operator{opt.RecursiveCTEOp}, nil
	case unionAllOp:
		// UnionAllOp and LocalityOptimizedSearchOp both lower to
		// ConstructUnionAll in execbuilder (see buildSetOp), so the gist
		// alone cannot distinguish them.
		return []opt.Operator{opt.UnionAllOp, opt.LocalityOptimizedSearchOp}, nil

	// Set ops: the gist preserves the ALL bit but drops the set operation type
	// (UNION/INTERSECT/EXCEPT). UnionAll/LocalityOptimizedSearch always lower
	// to unionAllOp, so they don't appear here.
	case hashSetOpOp:
		return setOpAlternates(n.args.(*hashSetOpArgs).All), nil
	case streamingSetOpOp:
		return setOpAlternates(n.args.(*streamingSetOpArgs).All), nil

	// DML operators.
	case insertOp, insertFastPathOp:
		return []opt.Operator{opt.InsertOp}, nil
	case updateOp, updateSwapOp:
		return []opt.Operator{opt.UpdateOp}, nil
	case upsertOp:
		return []opt.Operator{opt.UpsertOp}, nil
	case deleteOp, deleteRangeOp, deleteSwapOp:
		return []opt.Operator{opt.DeleteOp}, nil

	// DDL and misc operators.
	case createTableOp, createTableAsOp:
		return []opt.Operator{opt.CreateTableOp}, nil
	case createViewOp:
		return []opt.Operator{opt.CreateViewOp}, nil
	case sequenceSelectOp:
		return []opt.Operator{opt.SequenceSelectOp}, nil
	case exportOp:
		return []opt.Operator{opt.ExportOp}, nil
	case createStatisticsOp:
		return []opt.Operator{opt.CreateStatisticsOp}, nil
	case showTraceOp:
		return []opt.Operator{opt.ShowTraceForSessionOp}, nil
	case showCompletionsOp:
		return []opt.Operator{opt.ShowCompletionsOp}, nil
	case opaqueOp:
		return []opt.Operator{opt.OpaqueRelOp}, nil
	case callOp:
		return []opt.Operator{opt.CallOp}, nil
	case vectorSearchOp:
		return []opt.Operator{opt.VectorSearchOp}, nil
	case vectorMutationSearchOp:
		return []opt.Operator{opt.VectorMutationSearchOp}, nil
	case createFunctionOp:
		return []opt.Operator{opt.CreateFunctionOp}, nil
	case createTriggerOp:
		return []opt.Operator{opt.CreateTriggerOp}, nil
	case controlJobsOp:
		return []opt.Operator{opt.ControlJobsOp}, nil
	case controlSchedulesOp:
		return []opt.Operator{opt.ControlSchedulesOp}, nil
	case cancelQueriesOp:
		return []opt.Operator{opt.CancelQueriesOp}, nil
	case cancelSessionsOp:
		return []opt.Operator{opt.CancelSessionsOp}, nil
	case alterTableSplitOp:
		return []opt.Operator{opt.AlterTableSplitOp}, nil
	case alterTableUnsplitOp, alterTableUnsplitAllOp:
		return []opt.Operator{opt.AlterTableUnsplitOp}, nil
	case alterTableRelocateOp:
		return []opt.Operator{opt.AlterTableRelocateOp}, nil
	case alterRangeRelocateOp:
		return []opt.Operator{opt.AlterRangeRelocateOp}, nil

	case renderOp:
		// renderOp is always produced from a memo.ProjectExpr (see
		// execbuilder.buildProject), so the optimizer plan always has a
		// ProjectExpr at this position.
		return []opt.Operator{opt.ProjectOp}, nil

	case bufferOp:
		// TODO(drewk): buffer also appears when the execbuilder wraps a
		// mutation for cascade fan-out (mutation.go ConstructBuffer), which
		// has no dedicated opt op. That case will mismatch WithOp.
		return []opt.Operator{opt.WithOp}, nil
	case scanBufferOp:
		// scanBufferOp comes from WithScanExpr and from the recursive
		// function's reference to the working buffer in a RecursiveCTE; both
		// are WithScanOp at the optimizer level.
		//
		// TODO(drewk): the optimizer plan wraps the buffered subtree in a With
		// whose WithScans reference it by label. The decompiler doesn't follow
		// buffer labels yet, so the emitted grammar is missing the (With)
		// parent. Track labels to reconstruct it.
		return []opt.Operator{opt.WithScanOp}, nil

	// Execution-only ops: saveTable is a testing artifact; errorIfRows is
	// pulled into plan.Checks by the gist decoder before the decompiler runs.
	case saveTableOp, errorIfRowsOp:
		return []opt.Operator{opt.UnknownOp}, nil

	// Explain is invisible to PlanGram matching.
	case explainOp, explainOptOp:
		return []opt.Operator{opt.UnknownOp}, nil

	default:
		// A missing case means a new execOperator was added without being
		// mapped here. Returning nil ops signals this to the caller, which
		// turns it into an assertion failure so the pinned plan is skipped
		// rather than silently dropping the operator constraint.
		return nil, nil
	}
}

// tableIndexFields returns PlanGramFields for a table and index.
func tableIndexFields(table cat.Table, index cat.Index) []physical.PlanGramField {
	var fields []physical.PlanGramField
	if table != nil {
		fields = append(fields, physical.PlanGramField{Key: "Table", Val: string(table.Name())})
	}
	if index != nil {
		fields = append(fields, physical.PlanGramField{Key: "Index", Val: string(index.Name())})
	}
	return fields
}

// zigzagFields returns PlanGramFields for a zigzag join's left and right
// tables and indexes, matching memo.ZigzagJoinExpr.PlanGramMatchableFields.
func zigzagFields(args *zigzagJoinArgs) []physical.PlanGramField {
	var fields []physical.PlanGramField
	if args.LeftTable != nil {
		fields = append(fields, physical.PlanGramField{Key: "LeftTable", Val: string(args.LeftTable.Name())})
	}
	if args.RightTable != nil {
		fields = append(fields, physical.PlanGramField{Key: "RightTable", Val: string(args.RightTable.Name())})
	}
	if args.LeftIndex != nil {
		fields = append(fields, physical.PlanGramField{Key: "LeftIndex", Val: string(args.LeftIndex.Name())})
	}
	if args.RightIndex != nil {
		fields = append(fields, physical.PlanGramField{Key: "RightIndex", Val: string(args.RightIndex.Name())})
	}
	return fields
}

// setOpAlternates returns the optimizer set-op operators that could have
// produced a hashSetOp / streamingSetOp gist node with the given ALL bit.
// UnionAllOp and LocalityOptimizedSearchOp are excluded: both lower to
// unionAllOp in the gist.
func setOpAlternates(all bool) []opt.Operator {
	if all {
		return []opt.Operator{opt.IntersectAllOp, opt.ExceptAllOp}
	}
	return []opt.Operator{opt.UnionOp, opt.IntersectOp, opt.ExceptOp}
}

// joinTypeToOp maps a descpb.JoinType to its optimizer operator. RIGHT_SEMI
// and RIGHT_ANTI map to SemiJoinOp/AntiJoinOp: the execbuilder produces the
// right variants by swapping inputs (see execbuilder.buildHashJoin).
func joinTypeToOp(jt descpb.JoinType) opt.Operator {
	switch jt {
	case descpb.InnerJoin:
		return opt.InnerJoinOp
	case descpb.LeftOuterJoin:
		return opt.LeftJoinOp
	case descpb.RightOuterJoin:
		return opt.RightJoinOp
	case descpb.FullOuterJoin:
		return opt.FullJoinOp
	case descpb.LeftSemiJoin, descpb.RightSemiJoin:
		return opt.SemiJoinOp
	case descpb.LeftAntiJoin, descpb.RightAntiJoin:
		return opt.AntiJoinOp
	default:
		return opt.UnknownOp
	}
}

// applyJoinTypeToOp maps a descpb.JoinType to the corresponding apply join
// optimizer operator.
func applyJoinTypeToOp(jt descpb.JoinType) opt.Operator {
	switch jt {
	case descpb.InnerJoin:
		return opt.InnerJoinApplyOp
	case descpb.LeftOuterJoin:
		return opt.LeftJoinApplyOp
	case descpb.LeftSemiJoin:
		return opt.SemiJoinApplyOp
	case descpb.LeftAntiJoin:
		return opt.AntiJoinApplyOp
	default:
		return opt.UnknownOp
	}
}
