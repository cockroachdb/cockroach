// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type execPlan struct {
	root exec.Node

	// outputCols is a map from opt.ColumnID to exec.ColumnOrdinal. It maps
	// columns in the output set of a relational expression to indices in the
	// result columns of the exec.Node.
	//
	// The reason we need to keep track of this (instead of using just the
	// relational properties) is that the relational properties don't force a
	// single "schema": any ordering of the output columns is possible. We choose
	// the schema that is most convenient: for scans, we use the table's column
	// ordering. Consider:
	//   SELECT a, b FROM t WHERE a = b
	// and the following two cases:
	//   1. The table is defined as (k INT PRIMARY KEY, a INT, b INT). The scan will
	//      return (k, a, b).
	//   2. The table is defined as (k INT PRIMARY KEY, b INT, a INT). The scan will
	//      return (k, b, a).
	// In these two cases, the relational properties are effectively the same.
	//
	// An alternative to this would be to always use a "canonical" schema, for
	// example the output columns in increasing index order. However, this would
	// require a lot of otherwise unnecessary projections.
	//
	// Note: conceptually, this could be a ColList; however, the map is more
	// convenient when converting VariableOps to IndexedVars.
	outputCols opt.ColMap
}

// numOutputCols returns the number of columns emitted by the execPlan's Node.
// This will typically be equal to ep.outputCols.Len(), but might be different
// if the node outputs the same optimizer ColumnID multiple times.
// TODO(justin): we should keep track of this instead of computing it each time.
func (ep *execPlan) numOutputCols() int {
	return numOutputColsInMap(ep.outputCols)
}

// numOutputColsInMap returns the number of slots required to fill in all of
// the columns referred to by this ColMap.
func numOutputColsInMap(m opt.ColMap) int {
	max, ok := m.MaxValue()
	if !ok {
		return 0
	}
	return max + 1
}

// makeBuildScalarCtx returns a buildScalarCtx that can be used with expressions
// that refer the output columns of this plan.
func (ep *execPlan) makeBuildScalarCtx() buildScalarCtx {
	return buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, ep.numOutputCols()),
		ivarMap: ep.outputCols,
	}
}

// getColumnOrdinal takes a column that is known to be produced by the execPlan
// and returns the ordinal index of that column in the result columns of the
// node.
func (ep *execPlan) getColumnOrdinal(col opt.ColumnID) exec.ColumnOrdinal {
	ord, ok := ep.outputCols.Get(int(col))
	if !ok {
		panic(errors.AssertionFailedf("column %d not in input", log.Safe(col)))
	}
	return exec.ColumnOrdinal(ord)
}

func (ep *execPlan) getColumnOrdinalSet(cols opt.ColSet) exec.ColumnOrdinalSet {
	var res exec.ColumnOrdinalSet
	cols.ForEach(func(colID opt.ColumnID) {
		res.Add(int(ep.getColumnOrdinal(colID)))
	})
	return res
}

// reqOrdering converts the provided ordering of a relational expression to an
// OutputOrdering (according to the outputCols map).
func (ep *execPlan) reqOrdering(expr memo.RelExpr) exec.OutputOrdering {
	return exec.OutputOrdering(ep.sqlOrdering(expr.ProvidedPhysical().Ordering))
}

// sqlOrdering converts an Ordering to a ColumnOrdering (according to the
// outputCols map).
func (ep *execPlan) sqlOrdering(ordering opt.Ordering) sqlbase.ColumnOrdering {
	if ordering.Empty() {
		return nil
	}
	colOrder := make(sqlbase.ColumnOrdering, len(ordering))
	for i := range ordering {
		colOrder[i].ColIdx = int(ep.getColumnOrdinal(ordering[i].ID()))
		if ordering[i].Descending() {
			colOrder[i].Direction = encoding.Descending
		} else {
			colOrder[i].Direction = encoding.Ascending
		}
	}

	return colOrder
}

func (b *Builder) buildRelational(e memo.RelExpr) (execPlan, error) {
	var ep execPlan
	var err error

	// This will set the system DB trigger for transactions containing
	// schema-modifying statements that have no effect, such as
	// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
	// where the table already exists. This will generate some false schema
	// cache refreshes, but that's expected to be quite rare in practice.
	isDDL := opt.IsDDLOp(e)
	if isDDL {
		if err := b.evalCtx.Txn.SetSystemConfigTrigger(); err != nil {
			return execPlan{}, errors.WithSecondaryError(
				unimplemented.NewWithIssuef(26508,
					"schema change statement cannot follow a statement that has written in the same transaction"),
				err)
		}
	}

	// Raise error if mutation op is part of a read-only transaction.
	if opt.IsMutationOp(e) {
		if b.evalCtx.TxnReadOnly {
			return execPlan{}, pgerror.Newf(pgcode.ReadOnlySQLTransaction,
				"cannot execute %s in a read-only transaction", e.Op().SyntaxTag())
		}
	}

	// Collect usage telemetry for relational node, if appropriate.
	if !b.disableTelemetry {
		if c := opt.OpTelemetryCounters[e.Op()]; c != nil {
			telemetry.Inc(c)
		}
	}

	var saveTableName string
	if b.nameGen != nil {
		// This function must be called in a pre-order traversal of the tree.
		saveTableName = b.nameGen.GenerateName(e.Op())
	}

	// Handle read-only operators which never write data or modify schema.
	switch t := e.(type) {
	case *memo.ValuesExpr:
		ep, err = b.buildValues(t)

	case *memo.ScanExpr:
		ep, err = b.buildScan(t)

	case *memo.VirtualScanExpr:
		ep, err = b.buildVirtualScan(t)

	case *memo.SelectExpr:
		ep, err = b.buildSelect(t)

	case *memo.ProjectExpr:
		ep, err = b.buildProject(t)

	case *memo.GroupByExpr, *memo.ScalarGroupByExpr:
		ep, err = b.buildGroupBy(e)

	case *memo.DistinctOnExpr:
		ep, err = b.buildDistinct(t)

	case *memo.LimitExpr, *memo.OffsetExpr:
		ep, err = b.buildLimitOffset(e)

	case *memo.SortExpr:
		ep, err = b.buildSort(t)

	case *memo.IndexJoinExpr:
		ep, err = b.buildIndexJoin(t)

	case *memo.LookupJoinExpr:
		ep, err = b.buildLookupJoin(t)

	case *memo.ZigzagJoinExpr:
		ep, err = b.buildZigzagJoin(t)

	case *memo.OrdinalityExpr:
		ep, err = b.buildOrdinality(t)

	case *memo.MergeJoinExpr:
		ep, err = b.buildMergeJoin(t)

	case *memo.Max1RowExpr:
		ep, err = b.buildMax1Row(t)

	case *memo.ProjectSetExpr:
		ep, err = b.buildProjectSet(t)

	case *memo.WindowExpr:
		ep, err = b.buildWindow(t)

	case *memo.SequenceSelectExpr:
		ep, err = b.buildSequenceSelect(t)

	case *memo.InsertExpr:
		ep, err = b.buildInsert(t)

	case *memo.UpdateExpr:
		ep, err = b.buildUpdate(t)

	case *memo.UpsertExpr:
		ep, err = b.buildUpsert(t)

	case *memo.DeleteExpr:
		ep, err = b.buildDelete(t)

	case *memo.CreateTableExpr:
		ep, err = b.buildCreateTable(t)

	case *memo.WithExpr:
		ep, err = b.buildWith(t)

	case *memo.WithScanExpr:
		ep, err = b.buildWithScan(t)

	case *memo.ExplainExpr:
		ep, err = b.buildExplain(t)

	case *memo.ShowTraceForSessionExpr:
		ep, err = b.buildShowTrace(t)

	case *memo.OpaqueRelExpr:
		ep, err = b.buildOpaque(t)

	case *memo.AlterTableSplitExpr:
		ep, err = b.buildAlterTableSplit(t)

	case *memo.AlterTableUnsplitExpr:
		ep, err = b.buildAlterTableUnsplit(t)

	case *memo.AlterTableUnsplitAllExpr:
		ep, err = b.buildAlterTableUnsplitAll(t)

	case *memo.AlterTableRelocateExpr:
		ep, err = b.buildAlterTableRelocate(t)

	case *memo.ControlJobsExpr:
		ep, err = b.buildControlJobs(t)

	case *memo.CancelQueriesExpr:
		ep, err = b.buildCancelQueries(t)

	case *memo.CancelSessionsExpr:
		ep, err = b.buildCancelSessions(t)

	default:
		if opt.IsSetOp(e) {
			ep, err = b.buildSetOp(e)
			break
		}
		if opt.IsJoinNonApplyOp(e) {
			ep, err = b.buildHashJoin(e)
			break
		}
		if opt.IsJoinApplyOp(e) {
			ep, err = b.buildApplyJoin(e)
			break
		}
		return execPlan{}, errors.AssertionFailedf("no execbuild for %T", t)
	}
	if err != nil {
		return execPlan{}, err
	}

	// In race builds, assert that the exec plan output columns match the opt
	// plan output columns.
	if util.RaceEnabled {
		optCols := e.Relational().OutputCols
		var execCols opt.ColSet
		ep.outputCols.ForEach(func(key, val int) {
			execCols.Add(opt.ColumnID(key))
		})
		if !execCols.Equals(optCols) {
			return execPlan{}, errors.AssertionFailedf(
				"exec columns do not match opt columns: expected %v, got %v", optCols, execCols)
		}
	}

	if saveTableName != "" {
		ep, err = b.applySaveTable(ep, e, saveTableName)
		if err != nil {
			return execPlan{}, err
		}
	}

	// Wrap the expression in a render expression if presentation requires it.
	if p := e.RequiredPhysical(); !p.Presentation.Any() {
		ep, err = b.applyPresentation(ep, p)
	}
	return ep, err
}

func (b *Builder) buildValues(values *memo.ValuesExpr) (execPlan, error) {
	numCols := len(values.Cols)

	rows := make([][]tree.TypedExpr, len(values.Rows))
	rowBuf := make([]tree.TypedExpr, len(rows)*numCols)
	scalarCtx := buildScalarCtx{}
	for i := range rows {
		tup := values.Rows[i].(*memo.TupleExpr)
		if len(tup.Elems) != numCols {
			return execPlan{}, fmt.Errorf("inconsistent row length %d vs %d", len(tup.Elems), numCols)
		}
		// Chop off prefix of rowBuf and limit its capacity.
		rows[i] = rowBuf[:numCols:numCols]
		rowBuf = rowBuf[numCols:]
		var err error
		for j := 0; j < numCols; j++ {
			rows[i][j], err = b.buildScalar(&scalarCtx, tup.Elems[j])
			if err != nil {
				return execPlan{}, err
			}
		}
	}
	return b.constructValues(rows, values.Cols)
}

func (b *Builder) constructValues(rows [][]tree.TypedExpr, cols opt.ColList) (execPlan, error) {
	md := b.mem.Metadata()
	resultCols := make(sqlbase.ResultColumns, len(cols))
	for i, col := range cols {
		colMeta := md.ColumnMeta(col)
		resultCols[i].Name = colMeta.Alias
		resultCols[i].Typ = colMeta.Type
	}
	node, err := b.factory.ConstructValues(rows, resultCols)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range cols {
		ep.outputCols.Set(int(col), i)
	}

	return ep, nil
}

// getColumns returns the set of column ordinals in the table for the set of
// column IDs, along with a mapping from the column IDs to output ordinals
// (starting with outputOrdinalStart).
func (b *Builder) getColumns(
	cols opt.ColSet, tableID opt.TableID,
) (exec.ColumnOrdinalSet, opt.ColMap) {
	needed := exec.ColumnOrdinalSet{}
	output := opt.ColMap{}

	columnCount := b.mem.Metadata().Table(tableID).DeletableColumnCount()
	n := 0
	for i := 0; i < columnCount; i++ {
		colID := tableID.ColumnID(i)
		if cols.Contains(colID) {
			needed.Add(i)
			output.Set(int(colID), n)
			n++
		}
	}

	return needed, output
}

// indexConstraintMaxResults returns the maximum number of results for a scan;
// the scan is guaranteed never to return more results than this. Iff this hint
// is invalid, 0 is returned.
func (b *Builder) indexConstraintMaxResults(scan *memo.ScanExpr) uint64 {
	c := scan.Constraint
	if c == nil || c.IsContradiction() || c.IsUnconstrained() {
		return 0
	}

	numCols := c.Columns.Count()
	var indexCols opt.ColSet
	for i := 0; i < numCols; i++ {
		indexCols.Add(c.Columns.Get(i).ID())
	}
	rel := scan.Relational()
	if !rel.FuncDeps.ColsAreLaxKey(indexCols) {
		return 0
	}

	return c.CalculateMaxResults(b.evalCtx, indexCols, rel.NotNullCols)
}

func (b *Builder) buildScan(scan *memo.ScanExpr) (execPlan, error) {
	md := b.mem.Metadata()
	tab := md.Table(scan.Table)

	// Check if we tried to force a specific index but there was no Scan with that
	// index in the memo.
	if scan.Flags.ForceIndex && scan.Flags.Index != scan.Index {
		idx := tab.Index(scan.Flags.Index)
		var err error
		if idx.IsInverted() {
			err = fmt.Errorf("index \"%s\" is inverted and cannot be used for this query", idx.Name())
		} else {
			// This should never happen.
			err = fmt.Errorf("index \"%s\" cannot be used for this query", idx.Name())
		}
		return execPlan{}, err
	}

	needed, output := b.getColumns(scan.Cols, scan.Table)
	res := execPlan{outputCols: output}

	root, err := b.factory.ConstructScan(
		tab,
		tab.Index(scan.Index),
		needed,
		scan.Constraint,
		scan.HardLimit.RowCount(),
		// HardLimit.Reverse() is taken into account by ScanIsReverse.
		ordering.ScanIsReverse(scan, &scan.RequiredPhysical().Ordering),
		b.indexConstraintMaxResults(scan),
		res.reqOrdering(scan),
	)
	if err != nil {
		return execPlan{}, err
	}
	res.root = root
	return res, nil
}

func (b *Builder) buildVirtualScan(scan *memo.VirtualScanExpr) (execPlan, error) {
	md := b.mem.Metadata()
	tab := md.Table(scan.Table)

	_, output := b.getColumns(scan.Cols, scan.Table)
	res := execPlan{outputCols: output}

	root, err := b.factory.ConstructVirtualScan(tab)
	if err != nil {
		return execPlan{}, err
	}
	res.root = root
	return res, nil
}

func (b *Builder) buildSelect(sel *memo.SelectExpr) (execPlan, error) {
	input, err := b.buildRelational(sel.Input)
	if err != nil {
		return execPlan{}, err
	}
	ctx := input.makeBuildScalarCtx()
	filter, err := b.buildScalar(&ctx, &sel.Filters)
	if err != nil {
		return execPlan{}, err
	}
	// A filtering node does not modify the schema.
	res := execPlan{outputCols: input.outputCols}
	reqOrder := res.reqOrdering(sel)
	res.root, err = b.factory.ConstructFilter(input.root, filter, reqOrder)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

// applySimpleProject adds a simple projection on top of an existing plan.
func (b *Builder) applySimpleProject(
	input execPlan, cols opt.ColSet, providedOrd opt.Ordering,
) (execPlan, error) {
	// We have only pass-through columns.
	colList := make([]exec.ColumnOrdinal, 0, cols.Len())
	var res execPlan
	cols.ForEach(func(i opt.ColumnID) {
		res.outputCols.Set(int(i), len(colList))
		colList = append(colList, input.getColumnOrdinal(i))
	})
	var err error
	res.root, err = b.factory.ConstructSimpleProject(
		input.root, colList, nil /* colNames */, exec.OutputOrdering(res.sqlOrdering(providedOrd)),
	)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

func (b *Builder) buildProject(prj *memo.ProjectExpr) (execPlan, error) {
	md := b.mem.Metadata()
	input, err := b.buildRelational(prj.Input)
	if err != nil {
		return execPlan{}, err
	}

	projections := prj.Projections
	if len(projections) == 0 {
		// We have only pass-through columns.
		return b.applySimpleProject(input, prj.Passthrough, prj.ProvidedPhysical().Ordering)
	}

	var res execPlan
	exprs := make(tree.TypedExprs, 0, len(projections)+prj.Passthrough.Len())
	colNames := make([]string, 0, len(exprs))
	ctx := input.makeBuildScalarCtx()
	for i := range projections {
		item := &projections[i]
		expr, err := b.buildScalar(&ctx, item.Element)
		if err != nil {
			return execPlan{}, err
		}
		res.outputCols.Set(int(item.Col), i)
		exprs = append(exprs, expr)
		colNames = append(colNames, md.ColumnMeta(item.Col).Alias)
	}
	prj.Passthrough.ForEach(func(colID opt.ColumnID) {
		res.outputCols.Set(int(colID), len(exprs))
		exprs = append(exprs, b.indexedVar(&ctx, md, colID))
		colNames = append(colNames, md.ColumnMeta(colID).Alias)
	})
	reqOrdering := res.reqOrdering(prj)
	res.root, err = b.factory.ConstructRender(input.root, exprs, colNames, reqOrdering)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

// ReplaceVars replaces the VariableExprs within a RelExpr with constant Datums
// provided by the vars map, which maps opt column id for each VariableExpr to
// replace to the Datum that should replace it. The memo within the input
// norm.Factory will be replaced with the result.
// requiredPhysical is the set of physical properties that are required of the
// root of the new expression.
func ReplaceVars(
	f *norm.Factory,
	applyInput memo.RelExpr,
	requiredPhysical *physical.Required,
	vars map[opt.ColumnID]tree.Datum,
) {
	var replaceFn norm.ReplaceFunc
	replaceFn = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.VariableExpr:
			if d, ok := vars[t.Col]; ok {
				return f.ConstructConstVal(d, t.Typ)
			}
		}
		return f.CopyAndReplaceDefault(e, replaceFn)
	}
	f.CopyAndReplace(applyInput, requiredPhysical, replaceFn)
}

func (b *Builder) buildApplyJoin(join memo.RelExpr) (execPlan, error) {
	switch join.Op() {
	case opt.InnerJoinApplyOp, opt.LeftJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
	default:
		return execPlan{}, fmt.Errorf("couldn't execute correlated subquery with op %s", join.Op())
	}
	joinType := joinOpToJoinType(join.Op())
	leftExpr := join.Child(0).(memo.RelExpr)
	rightExpr := join.Child(1).(memo.RelExpr)
	filters := join.Child(2).(*memo.FiltersExpr)

	// Create a fake version of the right-side plan that contains NULL for all
	// outer columns, so that we can figure out the output columns and various
	// other attributes.
	var f norm.Factory
	f.Init(b.evalCtx)
	fakeBindings := make(map[opt.ColumnID]tree.Datum)
	rightExpr.Relational().OuterCols.ForEach(func(k opt.ColumnID) {
		fakeBindings[k] = tree.DNull
	})
	ReplaceVars(&f, rightExpr, rightExpr.RequiredPhysical(), fakeBindings)

	// We increment nullifyMissingVarExprs here to instruct the scalar builder to
	// replace the outer column VariableExprs with null for the current scope.
	b.nullifyMissingVarExprs++
	fakeRight, err := b.buildRelational(rightExpr)
	b.nullifyMissingVarExprs--
	if err != nil {
		return execPlan{}, err
	}

	// Make a copy of the required props.
	requiredProps := *rightExpr.RequiredPhysical()
	requiredProps.Presentation = make(physical.Presentation, fakeRight.outputCols.Len())
	fakeRight.outputCols.ForEach(func(k, v int) {
		requiredProps.Presentation[opt.ColumnID(v)] = opt.AliasedColumn{
			ID:    opt.ColumnID(k),
			Alias: join.Memo().Metadata().ColumnMeta(opt.ColumnID(k)).Alias,
		}
	})

	left, err := b.buildRelational(leftExpr)
	if err != nil {
		return execPlan{}, err
	}

	// leftBoundCols is the set of columns that this apply join binds.
	leftBoundCols := leftExpr.Relational().OutputCols.Intersection(rightExpr.Relational().OuterCols)
	// leftBoundColMap is a map from opt.ColumnID to opt.ColumnOrdinal that maps
	// a column bound by the left side of this apply join to the column ordinal
	// in the left side that contains the binding.
	var leftBoundColMap opt.ColMap
	for k, ok := leftBoundCols.Next(0); ok; k, ok = leftBoundCols.Next(k + 1) {
		v, ok := left.outputCols.Get(int(k))
		if !ok {
			return execPlan{}, fmt.Errorf("couldn't find binding column %d in output columns", k)
		}
		leftBoundColMap.Set(int(k), v)
	}

	allCols := joinOutputMap(left.outputCols, fakeRight.outputCols)

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, numOutputColsInMap(allCols)),
		ivarMap: allCols,
	}

	var onExpr tree.TypedExpr
	if len(*filters) != 0 {
		onExpr, err = b.buildScalar(&ctx, filters)
		if err != nil {
			return execPlan{}, nil
		}
	}

	var outputCols opt.ColMap
	if joinType == sqlbase.LeftSemiJoin || joinType == sqlbase.LeftAntiJoin {
		// For semi and anti join, only the left columns are output.
		outputCols = left.outputCols
	} else {
		outputCols = allCols
	}

	ep := execPlan{outputCols: outputCols}

	ep.root, err = b.factory.ConstructApplyJoin(
		joinType,
		left.root,
		leftBoundColMap,
		b.mem,
		&requiredProps,
		fakeRight.root,
		rightExpr,
		onExpr,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func (b *Builder) buildHashJoin(join memo.RelExpr) (execPlan, error) {
	if f := join.Private().(*memo.JoinPrivate).Flags; f.DisallowHashJoin {
		hint := tree.AstLookup
		if !f.DisallowMergeJoin {
			hint = tree.AstMerge
		}

		return execPlan{}, errors.Errorf(
			"could not produce a query plan conforming to the %s JOIN hint", hint,
		)
	}

	joinType := joinOpToJoinType(join.Op())
	leftExpr := join.Child(0).(memo.RelExpr)
	rightExpr := join.Child(1).(memo.RelExpr)
	filters := join.Child(2).(*memo.FiltersExpr)

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(
		leftExpr.Relational().OutputCols,
		rightExpr.Relational().OutputCols,
		*filters,
	)

	left, right, onExpr, outputCols, err := b.initJoinBuild(
		leftExpr,
		rightExpr,
		memo.ExtractRemainingJoinFilters(*filters, leftEq, rightEq),
		joinType,
	)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{outputCols: outputCols}

	// Convert leftEq/rightEq to ordinals.
	eqColsBuf := make([]exec.ColumnOrdinal, 2*len(leftEq))
	leftEqOrdinals := eqColsBuf[:len(leftEq):len(leftEq)]
	rightEqOrdinals := eqColsBuf[len(leftEq):]
	for i := range leftEq {
		leftEqOrdinals[i] = left.getColumnOrdinal(leftEq[i])
		rightEqOrdinals[i] = right.getColumnOrdinal(rightEq[i])
	}

	leftEqColsAreKey := leftExpr.Relational().FuncDeps.ColsAreStrictKey(leftEq.ToSet())
	rightEqColsAreKey := rightExpr.Relational().FuncDeps.ColsAreStrictKey(rightEq.ToSet())

	ep.root, err = b.factory.ConstructHashJoin(
		joinType,
		left.root, right.root,
		leftEqOrdinals, rightEqOrdinals,
		leftEqColsAreKey, rightEqColsAreKey,
		onExpr,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func (b *Builder) buildMergeJoin(join *memo.MergeJoinExpr) (execPlan, error) {
	joinType := joinOpToJoinType(join.JoinType)

	left, right, onExpr, outputCols, err := b.initJoinBuild(
		join.Left, join.Right, join.On, joinType,
	)
	if err != nil {
		return execPlan{}, err
	}
	leftOrd := left.sqlOrdering(join.LeftEq)
	rightOrd := right.sqlOrdering(join.RightEq)
	ep := execPlan{outputCols: outputCols}
	reqOrd := ep.reqOrdering(join)
	ep.root, err = b.factory.ConstructMergeJoin(
		joinType, left.root, right.root, onExpr, leftOrd, rightOrd, reqOrd,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

// initJoinBuild builds the inputs to the join as well as the ON expression.
func (b *Builder) initJoinBuild(
	leftChild memo.RelExpr,
	rightChild memo.RelExpr,
	filters memo.FiltersExpr,
	joinType sqlbase.JoinType,
) (leftPlan, rightPlan execPlan, onExpr tree.TypedExpr, outputCols opt.ColMap, _ error) {
	leftPlan, err := b.buildRelational(leftChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
	}
	rightPlan, err = b.buildRelational(rightChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
	}

	allCols := joinOutputMap(leftPlan.outputCols, rightPlan.outputCols)

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, numOutputColsInMap(allCols)),
		ivarMap: allCols,
	}

	if len(filters) != 0 {
		onExpr, err = b.buildScalar(&ctx, &filters)
		if err != nil {
			return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
		}
	}

	if joinType == sqlbase.LeftSemiJoin || joinType == sqlbase.LeftAntiJoin {
		// For semi and anti join, only the left columns are output.
		return leftPlan, rightPlan, onExpr, leftPlan.outputCols, nil
	}
	return leftPlan, rightPlan, onExpr, allCols, nil
}

// joinOutputMap determines the outputCols map for a (non-semi/anti) join, given
// the outputCols maps for its inputs.
func joinOutputMap(left, right opt.ColMap) opt.ColMap {
	numLeftCols := numOutputColsInMap(left)

	res := left.Copy()
	right.ForEach(func(colIdx, rightIdx int) {
		res.Set(colIdx, rightIdx+numLeftCols)
	})
	return res
}

func joinOpToJoinType(op opt.Operator) sqlbase.JoinType {
	switch op {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return sqlbase.InnerJoin

	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return sqlbase.LeftOuterJoin

	case opt.RightJoinOp, opt.RightJoinApplyOp:
		return sqlbase.RightOuterJoin

	case opt.FullJoinOp, opt.FullJoinApplyOp:
		return sqlbase.FullOuterJoin

	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return sqlbase.LeftSemiJoin

	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return sqlbase.LeftAntiJoin

	default:
		panic(errors.AssertionFailedf("not a join op %s", log.Safe(op)))
	}
}

func (b *Builder) buildGroupBy(groupBy memo.RelExpr) (execPlan, error) {
	input, err := b.buildGroupByInput(groupBy)
	if err != nil {
		return execPlan{}, err
	}

	var ep execPlan
	groupingCols := groupBy.Private().(*memo.GroupingPrivate).GroupingCols
	groupingColIdx := make([]exec.ColumnOrdinal, 0, groupingCols.Len())
	for i, ok := groupingCols.Next(0); ok; i, ok = groupingCols.Next(i + 1) {
		ep.outputCols.Set(int(i), len(groupingColIdx))
		groupingColIdx = append(groupingColIdx, input.getColumnOrdinal(opt.ColumnID(i)))
	}

	aggregations := *groupBy.Child(1).(*memo.AggregationsExpr)
	aggInfos := make([]exec.AggInfo, len(aggregations))
	for i := range aggregations {
		item := &aggregations[i]
		name, overload := memo.FindAggregateOverload(item.Agg)

		distinct := false
		var argIdx []exec.ColumnOrdinal
		var filterOrd exec.ColumnOrdinal = -1

		if item.Agg.ChildCount() > 0 {
			child := item.Agg.Child(0)

			if aggFilter, ok := child.(*memo.AggFilterExpr); ok {
				filter, ok := aggFilter.Filter.(*memo.VariableExpr)
				if !ok {
					return execPlan{}, errors.Errorf("only VariableOp args supported")
				}
				filterOrd = input.getColumnOrdinal(filter.Col)
				child = aggFilter.Input
			}

			if aggDistinct, ok := child.(*memo.AggDistinctExpr); ok {
				distinct = true
				child = aggDistinct.Input
			}
			v, ok := child.(*memo.VariableExpr)
			if !ok {
				return execPlan{}, errors.Errorf("only VariableOp args supported")
			}
			argIdx = []exec.ColumnOrdinal{input.getColumnOrdinal(v.Col)}
		}

		constArgs := b.extractAggregateConstArgs(item.Agg)

		aggInfos[i] = exec.AggInfo{
			FuncName:   name,
			Builtin:    overload,
			Distinct:   distinct,
			ResultType: item.Agg.DataType(),
			ArgCols:    argIdx,
			ConstArgs:  constArgs,
			Filter:     filterOrd,
		}
		ep.outputCols.Set(int(item.Col), len(groupingColIdx)+i)
	}

	if groupBy.Op() == opt.ScalarGroupByOp {
		ep.root, err = b.factory.ConstructScalarGroupBy(input.root, aggInfos)
	} else {
		groupBy := groupBy.(*memo.GroupByExpr)
		groupingColOrder := input.sqlOrdering(ordering.StreamingGroupingColOrdering(
			&groupBy.GroupingPrivate, &groupBy.RequiredPhysical().Ordering,
		))
		reqOrdering := ep.reqOrdering(groupBy)
		ep.root, err = b.factory.ConstructGroupBy(
			input.root, groupingColIdx, groupingColOrder, aggInfos, reqOrdering,
		)
	}
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

// extractAggregateConstArgs returns the list of constant arguments associated with a given aggregate
// expression.
func (b *Builder) extractAggregateConstArgs(agg opt.ScalarExpr) tree.Datums {
	switch agg.Op() {
	case opt.StringAggOp:
		return tree.Datums{memo.ExtractConstDatum(agg.Child(1))}
	default:
		return nil
	}
}

func (b *Builder) buildDistinct(distinct *memo.DistinctOnExpr) (execPlan, error) {
	if distinct.GroupingCols.Empty() {
		// A DistinctOn with no grouping columns should have been converted to a
		// LIMIT 1 by normalization rules.
		return execPlan{}, fmt.Errorf("cannot execute distinct on no columns")
	}
	input, err := b.buildGroupByInput(distinct)
	if err != nil {
		return execPlan{}, err
	}

	distinctCols := input.getColumnOrdinalSet(distinct.GroupingCols)
	var orderedCols exec.ColumnOrdinalSet
	ordering := ordering.StreamingGroupingColOrdering(
		&distinct.GroupingPrivate, &distinct.RequiredPhysical().Ordering,
	)
	for i := range ordering {
		orderedCols.Add(int(input.getColumnOrdinal(ordering[i].ID())))
	}
	node, err := b.factory.ConstructDistinct(input.root, distinctCols, orderedCols)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node, outputCols: input.outputCols}

	// buildGroupByInput can add extra sort column(s), so discard those if they
	// are present by using an additional projection.
	outCols := distinct.Relational().OutputCols
	if input.outputCols.Len() == outCols.Len() {
		return ep, nil
	}
	return b.ensureColumns(
		ep, opt.ColSetToList(outCols), nil /* colNames */, distinct.ProvidedPhysical().Ordering,
	)
}

func (b *Builder) buildGroupByInput(groupBy memo.RelExpr) (execPlan, error) {
	groupByInput := groupBy.Child(0).(memo.RelExpr)
	input, err := b.buildRelational(groupByInput)
	if err != nil {
		return execPlan{}, err
	}

	// TODO(radu): this is a one-off fix for an otherwise bigger gap: we should
	// have a more general mechanism (through physical properties or otherwise) to
	// figure out unneeded columns and project them away as necessary. The
	// optimizer doesn't guarantee that it adds ProjectOps everywhere.
	//
	// We address just the GroupBy case for now because there is a particularly
	// important case with COUNT(*) where we can remove all input columns, which
	// leads to significant speedup.
	private := groupBy.Private().(*memo.GroupingPrivate)
	neededCols := private.GroupingCols.Copy()
	aggs := *groupBy.Child(1).(*memo.AggregationsExpr)
	for i := range aggs {
		neededCols.UnionWith(memo.ExtractAggInputColumns(aggs[i].Agg))
	}

	// In rare cases, we might need a column only for its ordering, for example:
	//   SELECT concat_agg(s) FROM (SELECT s FROM kv ORDER BY k)
	// In this case we can't project the column away as it is still needed by
	// distsql to maintain the desired ordering.
	for _, c := range groupByInput.ProvidedPhysical().Ordering {
		neededCols.Add(c.ID())
	}

	if neededCols.Equals(groupByInput.Relational().OutputCols) {
		// All columns produced by the input are used.
		return input, nil
	}

	// The input is producing columns that are not useful; set up a projection.
	cols := make([]exec.ColumnOrdinal, 0, neededCols.Len())
	var newOutputCols opt.ColMap
	for colID, ok := neededCols.Next(0); ok; colID, ok = neededCols.Next(colID + 1) {
		ordinal, ordOk := input.outputCols.Get(int(colID))
		if !ordOk {
			panic(errors.AssertionFailedf("needed column not produced by group-by input"))
		}
		newOutputCols.Set(int(colID), len(cols))
		cols = append(cols, exec.ColumnOrdinal(ordinal))
	}

	input.outputCols = newOutputCols
	reqOrdering := input.reqOrdering(groupByInput)
	input.root, err = b.factory.ConstructSimpleProject(
		input.root, cols, nil /* colNames */, reqOrdering,
	)
	if err != nil {
		return execPlan{}, err
	}
	return input, nil
}

func (b *Builder) buildSetOp(set memo.RelExpr) (execPlan, error) {
	leftExpr := set.Child(0).(memo.RelExpr)
	left, err := b.buildRelational(leftExpr)
	if err != nil {
		return execPlan{}, err
	}
	rightExpr := set.Child(1).(memo.RelExpr)
	right, err := b.buildRelational(rightExpr)
	if err != nil {
		return execPlan{}, err
	}

	private := set.Private().(*memo.SetPrivate)

	// We need to make sure that the two sides render the columns in the same
	// order; otherwise we add projections.
	//
	// In most cases the projection is needed only to reorder the columns, but not
	// always. For example:
	//  (SELECT a, a, b FROM ab) UNION (SELECT x, y, z FROM xyz)
	// The left input could be just a scan that produces two columns.
	//
	// TODO(radu): we don't have to respect the exact order in the two ColLists;
	// if one side has the right columns but in a different permutation, we could
	// set up a matching projection on the other side. For example:
	//   (SELECT b, c, a FROM abc) UNION (SELECT z, y, x FROM xyz)
	// The expression for this could be a UnionOp on top of two ScanOps (any
	// internal projections could be removed by normalization rules).
	// The scans produce columns `a, b, c` and `x, y, z` respectively. We could
	// leave `b, c, a` as is and project the other side to `x, z, y`.
	// Note that (unless this is part of a larger query) the presentation property
	// will ensure that the columns are presented correctly in the output (i.e. in
	// the order `b, c, a`).
	left, err = b.ensureColumns(
		left, private.LeftCols, nil /* colNames */, leftExpr.ProvidedPhysical().Ordering,
	)
	if err != nil {
		return execPlan{}, err
	}
	right, err = b.ensureColumns(
		right, private.RightCols, nil /* colNames */, rightExpr.ProvidedPhysical().Ordering,
	)
	if err != nil {
		return execPlan{}, err
	}

	var typ tree.UnionType
	var all bool
	switch set.Op() {
	case opt.UnionOp:
		typ, all = tree.UnionOp, false
	case opt.UnionAllOp:
		typ, all = tree.UnionOp, true
	case opt.IntersectOp:
		typ, all = tree.IntersectOp, false
	case opt.IntersectAllOp:
		typ, all = tree.IntersectOp, true
	case opt.ExceptOp:
		typ, all = tree.ExceptOp, false
	case opt.ExceptAllOp:
		typ, all = tree.ExceptOp, true
	default:
		panic(errors.AssertionFailedf("invalid operator %s", log.Safe(set.Op())))
	}

	node, err := b.factory.ConstructSetOp(typ, all, left.root, right.root)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range private.OutCols {
		ep.outputCols.Set(int(col), i)
	}
	return ep, nil
}

// buildLimitOffset builds a plan for a LimitOp or OffsetOp
func (b *Builder) buildLimitOffset(e memo.RelExpr) (execPlan, error) {
	input, err := b.buildRelational(e.Child(0).(memo.RelExpr))
	if err != nil {
		return execPlan{}, err
	}
	// LIMIT/OFFSET expression should never need buildScalarContext, because it
	// can't refer to the input expression.
	expr, err := b.buildScalar(nil, e.Child(1).(opt.ScalarExpr))
	if err != nil {
		return execPlan{}, err
	}
	var node exec.Node
	if e.Op() == opt.LimitOp {
		node, err = b.factory.ConstructLimit(input.root, expr, nil)
	} else {
		node, err = b.factory.ConstructLimit(input.root, nil, expr)
	}
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildSort(sort *memo.SortExpr) (execPlan, error) {
	input, err := b.buildRelational(sort.Input)
	if err != nil {
		return execPlan{}, err
	}
	return b.buildSortedInput(input, sort.ProvidedPhysical().Ordering)
}

func (b *Builder) buildOrdinality(ord *memo.OrdinalityExpr) (execPlan, error) {
	input, err := b.buildRelational(ord.Input)
	if err != nil {
		return execPlan{}, err
	}

	colName := b.mem.Metadata().ColumnMeta(ord.ColID).Alias

	node, err := b.factory.ConstructOrdinality(input.root, colName)
	if err != nil {
		return execPlan{}, err
	}

	// We have one additional ordinality column, which is ordered at the end of
	// the list.
	outputCols := input.outputCols.Copy()
	outputCols.Set(int(ord.ColID), outputCols.Len())

	return execPlan{root: node, outputCols: outputCols}, nil
}

func (b *Builder) buildIndexJoin(join *memo.IndexJoinExpr) (execPlan, error) {
	var err error
	// If the index join child is a sort operator then flip the order so that the
	// sort is on top of the index join.
	// TODO(radu): Remove this code once we have support for a more general
	// lookup join execution path.
	var ordering opt.Ordering
	child := join.Input
	if child.Op() == opt.SortOp {
		ordering = child.ProvidedPhysical().Ordering
		child = child.Child(0).(memo.RelExpr)
	}

	input, err := b.buildRelational(child)
	if err != nil {
		return execPlan{}, err
	}

	md := b.mem.Metadata()

	cols := join.Cols
	needed, output := b.getColumns(cols, join.Table)
	res := execPlan{outputCols: output}

	// Get sort *result column* ordinals. Don't confuse these with *table column*
	// ordinals, which are used by the needed set. The sort columns should already
	// be in the needed set, so no need to add anything further to that.
	var reqOrdering exec.OutputOrdering
	if ordering == nil {
		reqOrdering = res.reqOrdering(join)
	}

	res.root, err = b.factory.ConstructIndexJoin(
		input.root, md.Table(join.Table), needed, reqOrdering,
	)
	if err != nil {
		return execPlan{}, err
	}
	if ordering != nil {
		res, err = b.buildSortedInput(res, ordering)
		if err != nil {
			return execPlan{}, err
		}
	}

	return res, nil
}

func (b *Builder) buildLookupJoin(join *memo.LookupJoinExpr) (execPlan, error) {
	input, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, err
	}

	md := b.mem.Metadata()

	keyCols := make([]exec.ColumnOrdinal, len(join.KeyCols))
	for i, c := range join.KeyCols {
		keyCols[i] = input.getColumnOrdinal(c)
	}

	inputCols := join.Input.Relational().OutputCols
	lookupCols := join.Cols.Difference(inputCols)

	lookupOrdinals, lookupColMap := b.getColumns(lookupCols, join.Table)
	allCols := joinOutputMap(input.outputCols, lookupColMap)

	res := execPlan{outputCols: allCols}

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, allCols.Len()),
		ivarMap: allCols,
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, err
	}

	tab := md.Table(join.Table)
	idx := tab.Index(join.Index)
	var eqCols opt.ColSet
	for i := range join.KeyCols {
		eqCols.Add(join.Table.ColumnID(idx.Column(i).Ordinal))
	}
	tableFDs := memo.MakeTableFuncDep(md, join.Table)
	eqColsAreKey := tableFDs.ColsAreStrictKey(eqCols)

	res.root, err = b.factory.ConstructLookupJoin(
		joinOpToJoinType(join.JoinType),
		input.root,
		tab,
		idx,
		keyCols,
		eqColsAreKey,
		lookupOrdinals,
		onExpr,
		res.reqOrdering(join),
	)
	if err != nil {
		return execPlan{}, err
	}

	// Apply a post-projection if Cols doesn't contain all input columns.
	if !inputCols.SubsetOf(join.Cols) {
		return b.applySimpleProject(res, join.Cols, join.ProvidedPhysical().Ordering)
	}
	return res, nil
}

func (b *Builder) buildZigzagJoin(join *memo.ZigzagJoinExpr) (execPlan, error) {
	md := b.mem.Metadata()

	leftTable := md.Table(join.LeftTable)
	rightTable := md.Table(join.RightTable)
	leftIndex := leftTable.Index(join.LeftIndex)
	rightIndex := rightTable.Index(join.RightIndex)

	leftEqCols := make([]exec.ColumnOrdinal, len(join.LeftEqCols))
	rightEqCols := make([]exec.ColumnOrdinal, len(join.RightEqCols))
	for i := range join.LeftEqCols {
		leftEqCols[i] = exec.ColumnOrdinal(join.LeftTable.ColumnOrdinal(join.LeftEqCols[i]))
		rightEqCols[i] = exec.ColumnOrdinal(join.RightTable.ColumnOrdinal(join.RightEqCols[i]))
	}
	leftCols := md.TableMeta(join.LeftTable).IndexColumns(join.LeftIndex).Intersection(join.Cols)
	rightCols := md.TableMeta(join.RightTable).IndexColumns(join.RightIndex).Intersection(join.Cols)
	// Remove duplicate columns, if any.
	rightCols.DifferenceWith(leftCols)

	leftOrdinals, leftColMap := b.getColumns(leftCols, join.LeftTable)
	rightOrdinals, rightColMap := b.getColumns(rightCols, join.RightTable)

	allCols := joinOutputMap(leftColMap, rightColMap)

	res := execPlan{outputCols: allCols}

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, leftColMap.Len()+rightColMap.Len()),
		ivarMap: allCols,
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, err
	}

	// Build the fixed value scalars. These are represented as one value node
	// per side of the join, containing one row/tuple with fixed values for
	// a prefix of that index's columns.
	fixedVals := make([]exec.Node, 2)
	fixedCols := []opt.ColList{join.LeftFixedCols, join.RightFixedCols}
	for i := range join.FixedVals {
		tup := join.FixedVals[i].(*memo.TupleExpr)
		valExprs := make([]tree.TypedExpr, len(tup.Elems))
		for j := range tup.Elems {
			valExprs[j], err = b.buildScalar(&ctx, tup.Elems[j])
			if err != nil {
				return execPlan{}, err
			}
		}
		valuesPlan, err := b.constructValues([][]tree.TypedExpr{valExprs}, fixedCols[i])
		if err != nil {
			return execPlan{}, err
		}
		fixedVals[i] = valuesPlan.root
	}

	res.root, err = b.factory.ConstructZigzagJoin(
		leftTable,
		leftIndex,
		rightTable,
		rightIndex,
		leftEqCols,
		rightEqCols,
		leftOrdinals,
		rightOrdinals,
		onExpr,
		fixedVals,
		res.reqOrdering(join),
	)
	if err != nil {
		return execPlan{}, err
	}

	return res, nil
}

func (b *Builder) buildMax1Row(max1Row *memo.Max1RowExpr) (execPlan, error) {
	input, err := b.buildRelational(max1Row.Input)
	if err != nil {
		return execPlan{}, err
	}

	node, err := b.factory.ConstructMax1Row(input.root)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil

}

func (b *Builder) buildWith(with *memo.WithExpr) (execPlan, error) {
	value, err := b.buildRelational(with.Binding)
	if err != nil {
		return execPlan{}, err
	}

	var label bytes.Buffer
	fmt.Fprintf(&label, "buffer %d", with.ID)
	if with.Name != "" {
		fmt.Fprintf(&label, " (%s)", with.Name)
	}

	buffer, err := b.factory.ConstructBuffer(value.root, label.String())
	if err != nil {
		return execPlan{}, err
	}

	// TODO(justin): if the binding here has a spoolNode at its root, we can
	// remove it, since subquery execution also guarantees complete execution.

	// Add the buffer as a subquery so it gets executed ahead of time, and is
	// available to be referenced by other queries.
	b.subqueries = append(b.subqueries, exec.Subquery{
		ExprNode: &tree.Subquery{
			Select: with.OriginalExpr.Select,
		},
		// TODO(justin): this is wasteful: both the subquery and the bufferNode
		// will buffer up all the results.  This should be fixed by either making
		// the buffer point directly to the subquery results or adding a new
		// subquery mode that reads and discards all rows. This could possibly also
		// be fixed by ensuring that bufferNode exhausts its input (and forcing it
		// to behave like a spoolNode) and using the EXISTS mode.
		Mode: exec.SubqueryAllRows,
		Root: buffer,
	})

	b.addBuiltWithExpr(with.ID, value.outputCols, buffer)

	return b.buildRelational(with.Input)
}

func (b *Builder) buildWithScan(withScan *memo.WithScanExpr) (execPlan, error) {
	id := withScan.ID
	var e *builtWithExpr
	for i := range b.withExprs {
		if b.withExprs[i].id == id {
			e = &b.withExprs[i]
			break
		}
	}
	if e == nil {
		panic(errors.AssertionFailedf("couldn't find With expression with ID %d", id))
	}

	var label bytes.Buffer
	fmt.Fprintf(&label, "buffer %d", withScan.ID)
	if withScan.Name != "" {
		fmt.Fprintf(&label, " (%s)", withScan.Name)
	}

	node, err := b.factory.ConstructScanBuffer(e.bufferNode, label.String())
	if err != nil {
		return execPlan{}, err
	}
	res := execPlan{root: node}

	if maxVal, _ := e.outputCols.MaxValue(); len(withScan.InCols) == maxVal+1 {
		// We are outputting all columns. Just set up the map.

		// The ColumnIDs from the With expression need to get remapped according to
		// the mapping in the withScan to get the actual colMap for this expression.
		for i := range withScan.InCols {
			idx, _ := e.outputCols.Get(int(withScan.InCols[i]))
			res.outputCols.Set(int(withScan.OutCols[i]), idx)
		}
	} else {
		// We need a projection.
		cols := make([]exec.ColumnOrdinal, len(withScan.InCols))
		for i := range withScan.InCols {
			col, ok := e.outputCols.Get(int(withScan.InCols[i]))
			if !ok {
				panic(errors.AssertionFailedf("column %d not in input", log.Safe(withScan.InCols[i])))
			}
			cols[i] = exec.ColumnOrdinal(col)
			res.outputCols.Set(int(withScan.OutCols[i]), i)
		}
		res.root, err = b.factory.ConstructSimpleProject(
			res.root, cols, nil, /* colNames */
			exec.OutputOrdering(res.sqlOrdering(withScan.ProvidedPhysical().Ordering)),
		)
		if err != nil {
			return execPlan{}, err
		}
	}
	return res, nil

}

func (b *Builder) buildProjectSet(projectSet *memo.ProjectSetExpr) (execPlan, error) {
	input, err := b.buildRelational(projectSet.Input)
	if err != nil {
		return execPlan{}, err
	}

	zip := projectSet.Zip
	md := b.mem.Metadata()
	scalarCtx := input.makeBuildScalarCtx()

	exprs := make(tree.TypedExprs, len(zip))
	zipCols := make(sqlbase.ResultColumns, 0, len(zip))
	numColsPerGen := make([]int, len(zip))

	ep := execPlan{outputCols: input.outputCols}
	n := ep.numOutputCols()

	for i := range zip {
		item := &zip[i]
		exprs[i], err = b.buildScalar(&scalarCtx, item.Func)
		if err != nil {
			return execPlan{}, err
		}

		for _, col := range item.Cols {
			colMeta := md.ColumnMeta(col)
			zipCols = append(zipCols, sqlbase.ResultColumn{Name: colMeta.Alias, Typ: colMeta.Type})

			ep.outputCols.Set(int(col), n)
			n++
		}

		numColsPerGen[i] = len(item.Cols)
	}

	ep.root, err = b.factory.ConstructProjectSet(input.root, exprs, zipCols, numColsPerGen)
	if err != nil {
		return execPlan{}, err
	}

	return ep, nil
}

func (b *Builder) resultColumn(id opt.ColumnID) sqlbase.ResultColumn {
	colMeta := b.mem.Metadata().ColumnMeta(id)
	return sqlbase.ResultColumn{
		Name: colMeta.Alias,
		Typ:  colMeta.Type,
	}
}

// extractFromOffset extracts the start bound expression of a window function
// that uses the OFFSET windowing mode for its start bound.
func (b *Builder) extractFromOffset(e opt.ScalarExpr) (_ opt.ScalarExpr, ok bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if modifier, ok := e.(*memo.WindowFromOffsetExpr); ok {
		return modifier.Offset, true
	}
	return b.extractFromOffset(e.Child(0).(opt.ScalarExpr))
}

// extractToOffset extracts the end bound expression of a window function
// that uses the OFFSET windowing mode for its end bound.
func (b *Builder) extractToOffset(e opt.ScalarExpr) (_ opt.ScalarExpr, ok bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if modifier, ok := e.(*memo.WindowToOffsetExpr); ok {
		return modifier.Offset, true
	}
	return b.extractToOffset(e.Child(0).(opt.ScalarExpr))
}

// extractFilter extracts a FILTER expression from a window function tower.
// Returns the expression and true if there was a filter, and false otherwise.
func (b *Builder) extractFilter(e opt.ScalarExpr) (opt.ScalarExpr, bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if filter, ok := e.(*memo.AggFilterExpr); ok {
		return filter.Filter, true
	}
	return b.extractFilter(e.Child(0).(opt.ScalarExpr))
}

// extractWindowFunction extracts the window function being computed from a
// potential tower of modifiers attached to the Function field of a
// WindowsItem.
func (b *Builder) extractWindowFunction(e opt.ScalarExpr) opt.ScalarExpr {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return e
	}
	return b.extractWindowFunction(e.Child(0).(opt.ScalarExpr))
}

func (b *Builder) isOffsetMode(boundType tree.WindowFrameBoundType) bool {
	return boundType == tree.OffsetPreceding || boundType == tree.OffsetFollowing
}

func (b *Builder) buildFrame(input execPlan, w *memo.WindowsItem) (*tree.WindowFrame, error) {
	scalarCtx := input.makeBuildScalarCtx()
	newDef := &tree.WindowFrame{
		Mode: w.Frame.Mode,
		Bounds: tree.WindowFrameBounds{
			StartBound: &tree.WindowFrameBound{
				BoundType: w.Frame.StartBoundType,
			},
			EndBound: &tree.WindowFrameBound{
				BoundType: w.Frame.EndBoundType,
			},
		},
	}
	if boundExpr, ok := b.extractFromOffset(w.Function); ok {
		if !b.isOffsetMode(w.Frame.StartBoundType) {
			panic(errors.AssertionFailedf("expected offset to only be present in offset mode"))
		}
		offset, err := b.buildScalar(&scalarCtx, boundExpr)
		if err != nil {
			return nil, err
		}
		newDef.Bounds.StartBound.OffsetExpr = offset
	}

	if boundExpr, ok := b.extractToOffset(w.Function); ok {
		if !b.isOffsetMode(newDef.Bounds.EndBound.BoundType) {
			panic(errors.AssertionFailedf("expected offset to only be present in offset mode"))
		}
		offset, err := b.buildScalar(&scalarCtx, boundExpr)
		if err != nil {
			return nil, err
		}
		newDef.Bounds.EndBound.OffsetExpr = offset
	}
	return newDef, nil
}

func (b *Builder) buildWindow(w *memo.WindowExpr) (execPlan, error) {
	input, err := b.buildRelational(w.Input)
	if err != nil {
		return execPlan{}, err
	}

	// Rearrange the input so that the input has all the passthrough columns
	// followed by all the argument columns.

	passthrough := w.Input.Relational().OutputCols

	desiredCols := opt.ColList{}
	passthrough.ForEach(func(i opt.ColumnID) {
		desiredCols = append(desiredCols, i)
	})

	// TODO(justin): this call to ensureColumns is kind of unfortunate because it
	// can result in an extra render beneath each window function. Figure out a
	// way to alleviate this.
	input, err = b.ensureColumns(input, desiredCols, nil, opt.Ordering{})
	if err != nil {
		return execPlan{}, err
	}

	ctx := input.makeBuildScalarCtx()

	ord := w.Ordering.ToOrdering()

	orderingExprs := make(tree.OrderBy, len(ord))
	for i, c := range ord {
		direction := tree.Ascending
		if c.Descending() {
			direction = tree.Descending
		}
		orderingExprs[i] = &tree.Order{
			Expr:      b.indexedVar(&ctx, b.mem.Metadata(), c.ID()),
			Direction: direction,
		}
	}

	partitionIdxs := make([]exec.ColumnOrdinal, w.Partition.Len())
	partitionExprs := make(tree.Exprs, w.Partition.Len())

	i := 0
	w.Partition.ForEach(func(col opt.ColumnID) {
		ordinal, _ := input.outputCols.Get(int(col))
		partitionIdxs[i] = exec.ColumnOrdinal(ordinal)
		partitionExprs[i] = b.indexedVar(&ctx, b.mem.Metadata(), col)
		i++
	})

	argIdxs := make([][]exec.ColumnOrdinal, len(w.Windows))
	filterIdxs := make([]int, len(w.Windows))
	exprs := make([]*tree.FuncExpr, len(w.Windows))

	for i := range w.Windows {
		item := &w.Windows[i]
		fn := b.extractWindowFunction(item.Function)
		name, overload := memo.FindWindowOverload(fn)
		props, _ := builtins.GetBuiltinProperties(name)

		args := make([]tree.TypedExpr, fn.ChildCount())
		argIdxs[i] = make([]exec.ColumnOrdinal, fn.ChildCount())
		for j, n := 0, fn.ChildCount(); j < n; j++ {
			col := fn.Child(j).(*memo.VariableExpr).Col
			args[j] = b.indexedVar(&ctx, b.mem.Metadata(), col)
			idx, _ := input.outputCols.Get(int(col))
			argIdxs[i][j] = exec.ColumnOrdinal(idx)
		}

		frame, err := b.buildFrame(input, item)
		if err != nil {
			return execPlan{}, err
		}

		var builtFilter tree.TypedExpr
		filter, ok := b.extractFilter(item.Function)
		if ok {
			f, ok := filter.(*memo.VariableExpr)
			if !ok {
				panic(errors.AssertionFailedf("expected FILTER expression to be a VariableExpr"))
			}
			filterIdxs[i], _ = input.outputCols.Get(int(f.Col))

			builtFilter, err = b.buildScalar(&ctx, filter)
			if err != nil {
				return execPlan{}, err
			}
		} else {
			filterIdxs[i] = -1
		}

		exprs[i] = tree.NewTypedFuncExpr(
			tree.WrapFunction(name),
			0,
			args,
			builtFilter,
			&tree.WindowDef{
				Partitions: partitionExprs,
				OrderBy:    orderingExprs,
				Frame:      frame,
			},
			overload.FixedReturnType(),
			props,
			overload,
		)
	}

	resultCols := make(sqlbase.ResultColumns, w.Relational().OutputCols.Len())

	// All the passthrough cols will keep their ordinal index.
	passthrough.ForEach(func(col opt.ColumnID) {
		ordinal, _ := input.outputCols.Get(int(col))
		resultCols[ordinal] = b.resultColumn(col)
	})

	var outputCols opt.ColMap
	input.outputCols.ForEach(func(key, val int) {
		if passthrough.Contains(opt.ColumnID(key)) {
			outputCols.Set(key, val)
		}
	})

	outputIdxs := make([]int, len(w.Windows))

	// Because of the way we arranged the input columns, we will be outputting
	// the window columns at the end (which is exactly what the execution engine
	// will do as well).
	windowStart := passthrough.Len()
	for i := range w.Windows {
		resultCols[windowStart+i] = b.resultColumn(w.Windows[i].Col)
		outputCols.Set(int(w.Windows[i].Col), windowStart+i)
		outputIdxs[i] = windowStart + i
	}

	node, err := b.factory.ConstructWindow(input.root, exec.WindowInfo{
		Cols:       resultCols,
		Exprs:      exprs,
		OutputIdxs: outputIdxs,
		ArgIdxs:    argIdxs,
		FilterIdxs: filterIdxs,
		Partition:  partitionIdxs,
		Ordering:   input.sqlOrdering(ord),
	})
	if err != nil {
		return execPlan{}, err
	}

	return execPlan{
		root:       node,
		outputCols: outputCols,
	}, nil
}

func (b *Builder) buildSequenceSelect(seqSel *memo.SequenceSelectExpr) (execPlan, error) {
	seq := b.mem.Metadata().Sequence(seqSel.Sequence)
	node, err := b.factory.ConstructSequenceSelect(seq)
	if err != nil {
		return execPlan{}, err
	}

	ep := execPlan{root: node}
	for i, c := range seqSel.Cols {
		ep.outputCols.Set(int(c), i)
	}

	return ep, nil
}

func (b *Builder) applySaveTable(
	input execPlan, e memo.RelExpr, saveTableName string,
) (execPlan, error) {
	name := tree.NewTableName(tree.Name(opt.SaveTablesDatabase), tree.Name(saveTableName))

	// Ensure that the column names are unique and match the names used by the
	// opttester.
	outputCols := e.Relational().OutputCols
	colNames := make([]string, outputCols.Len())
	colNameGen := memo.NewColumnNameGenerator(e)
	for col, ok := outputCols.Next(0); ok; col, ok = outputCols.Next(col + 1) {
		ord, _ := input.outputCols.Get(int(col))
		colNames[ord] = colNameGen.GenerateName(col)
	}

	var err error
	input.root, err = b.factory.ConstructSaveTable(input.root, name, colNames)
	if err != nil {
		return execPlan{}, err
	}
	return input, err
}

func (b *Builder) buildOpaque(opaque *memo.OpaqueRelExpr) (execPlan, error) {
	node, err := b.factory.ConstructOpaque(opaque.Metadata)
	if err != nil {
		return execPlan{}, err
	}

	ep := execPlan{root: node}
	for i, c := range opaque.Columns {
		ep.outputCols.Set(int(c), i)
	}

	return ep, nil
}

// needProjection figures out what projection is needed on top of the input plan
// to produce the given list of columns. If the input plan already produces
// the columns (in the same order), returns needProj=false.
func (b *Builder) needProjection(
	input execPlan, colList opt.ColList,
) (_ []exec.ColumnOrdinal, needProj bool) {
	if input.numOutputCols() == len(colList) {
		identity := true
		for i, col := range colList {
			if ord, ok := input.outputCols.Get(int(col)); !ok || ord != i {
				identity = false
				break
			}
		}
		if identity {
			return nil, false
		}
	}
	cols := make([]exec.ColumnOrdinal, 0, len(colList))
	for _, col := range colList {
		if col != 0 {
			cols = append(cols, input.getColumnOrdinal(col))
		}
	}
	return cols, true
}

// ensureColumns applies a projection as necessary to make the output match the
// given list of columns; colNames is optional.
func (b *Builder) ensureColumns(
	input execPlan, colList opt.ColList, colNames []string, provided opt.Ordering,
) (execPlan, error) {
	cols, needProj := b.needProjection(input, colList)
	if !needProj {
		// No projection necessary.
		if colNames != nil {
			var err error
			input.root, err = b.factory.RenameColumns(input.root, colNames)
			if err != nil {
				return execPlan{}, err
			}
		}
		return input, nil
	}
	var res execPlan
	for i, col := range colList {
		res.outputCols.Set(int(col), i)
	}
	reqOrdering := exec.OutputOrdering(res.sqlOrdering(provided))
	var err error
	res.root, err = b.factory.ConstructSimpleProject(input.root, cols, colNames, reqOrdering)
	return res, err
}

// applyPresentation adds a projection to a plan to satisfy a required
// Presentation property.
func (b *Builder) applyPresentation(input execPlan, p *physical.Required) (execPlan, error) {
	pres := p.Presentation
	colList := make(opt.ColList, len(pres))
	colNames := make([]string, len(pres))
	for i := range pres {
		colList[i] = pres[i].ID
		colNames[i] = pres[i].Alias
	}
	// The ordering is not useful for a top-level projection (it is used by the
	// distsql planner for internal nodes); we might not even be able to represent
	// it because it can refer to columns not in the presentation.
	return b.ensureColumns(input, colList, colNames, nil /* provided */)
}

// getEnvData consolidates the information that must be presented in
// EXPLAIN (opt, env).
func (b *Builder) getEnvData() exec.ExplainEnvData {
	envOpts := exec.ExplainEnvData{ShowEnv: true}
	// Catalog objects can show up multiple times in these lists, so
	// deduplicate them.
	seen := make(map[tree.TableName]bool)
	addDS := func(list []tree.TableName, ds cat.DataSource) []tree.TableName {
		tn, err := b.catalog.FullyQualifiedName(context.TODO(), ds)
		if err != nil {
			panic(err)
		}
		if !seen[tn] {
			seen[tn] = true
			list = append(list, tn)
		}
		return list
	}
	for _, t := range b.mem.Metadata().AllTables() {
		envOpts.Tables = addDS(envOpts.Tables, t.Table)
	}
	for _, s := range b.mem.Metadata().AllSequences() {
		envOpts.Sequences = addDS(envOpts.Sequences, s)
	}
	for _, v := range b.mem.Metadata().AllViews() {
		envOpts.Views = addDS(envOpts.Views, v)
	}

	return envOpts
}

// buildSortedInput is a helper method that can be reused to sort any input plan
// by the given ordering.
func (b *Builder) buildSortedInput(input execPlan, ordering opt.Ordering) (execPlan, error) {
	node, err := b.factory.ConstructSort(input.root, input.sqlOrdering(ordering))
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}
