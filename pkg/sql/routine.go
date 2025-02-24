// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// A callNode executes a procedure.
type callNode struct {
	zeroInputPlanNode
	proc *tree.RoutineExpr
	r    tree.Datums
}

var _ planNode = &callNode{}

// startExec implements the planNode interface.
func (d *callNode) startExec(params runParams) error {
	res, err := eval.Expr(params.ctx, params.EvalContext(), d.proc)
	if err != nil {
		return err
	}
	if params.p.storedProcTxnState.getTxnOp() != tree.StoredProcTxnNoOp {
		// The stored procedure has paused execution to COMMIT or ROLLBACK the
		// current transaction. The current iteration should have no result.
		return nil
	}
	if d.proc.Typ.Family() == types.VoidFamily {
		// With VOID return type we expect no rows to be produced, so we should
		// get NULL value from the expression evaluation.
		if res != tree.DNull {
			return errors.AssertionFailedf("expected NULL, got %T", res)
		}
		return nil
	}
	if d.proc.Typ.Family() != types.TupleFamily {
		return errors.AssertionFailedf("expected VOID or RECORD type for procedures, got %s", d.proc.Typ.SQLStringForError())
	}
	if res == tree.DNull {
		return pgerror.New(pgcode.Internal, "procedure returned null record")
	}
	tuple, ok := tree.AsDTuple(res)
	if !ok {
		return errors.AssertionFailedf("expected a tuple, got %T", res)
	}
	d.r = tuple.D
	return nil
}

// Next implements the planNode interface.
func (d *callNode) Next(params runParams) (bool, error) {
	return d.r != nil, nil
}

// Values implements the planNode interface.
func (d *callNode) Values() tree.Datums {
	r := d.r
	d.r = nil
	return r
}

// Close implements the planNode interface.
func (d *callNode) Close(ctx context.Context) {}

func (d *callNode) getResultColumns() colinfo.ResultColumns {
	// The schema of the result row is specified in the tuple contents except
	// when the return type is VOID (in which case the callNode returns
	// nothing).
	if d.proc.Typ.Family() == types.VoidFamily {
		return colinfo.ResultColumns{}
	}
	names, typs := d.proc.Typ.TupleLabels(), d.proc.Typ.TupleContents()
	cols := make(colinfo.ResultColumns, len(names))
	for i := range cols {
		cols[i] = colinfo.ResultColumn{
			Name: names[i],
			Typ:  typs[i],
		}
	}
	return cols
}

// EvalRoutineExpr returns the result of evaluating the routine. It calls the
// routine's ForEachPlan closure to generate a plan for each statement in the
// routine, then runs the plans. The resulting value of the last statement in
// the routine is returned.
func (p *planner) EvalRoutineExpr(
	ctx context.Context, expr *tree.RoutineExpr, args tree.Datums,
) (result tree.Datum, err error) {
	// Strict routines (CalledOnNullInput=false) should not be invoked and they
	// should immediately return NULL if any of their arguments are NULL.
	if !expr.CalledOnNullInput {
		for i := range args {
			if args[i] == tree.DNull {
				return tree.DNull, nil
			}
		}
	}

	// Return the cached result if it exists.
	if expr.CachedResult != nil {
		return expr.CachedResult, nil
	}

	if tailCallOptimizationEnabled && expr.TailCall && !expr.Generator {
		// This is a nested routine in tail-call position.
		sender := p.EvalContext().RoutineSender
		if sender != nil && sender.CanOptimizeTailCall(expr) {
			// Tail-call optimizations are enabled. Send the information needed to
			// evaluate this routine to the parent routine, then return. It is safe to
			// return NULL here because the parent is guaranteed not to perform any
			// processing on the result of the child.
			p.EvalContext().RoutineSender.SendDeferredRoutine(expr, args)
			return tree.DNull, nil
		}
	}

	if expr.TriggerFunc {
		// In cyclical reference situations, the number of nested trigger actions
		// can be arbitrarily large. To avoid OOM, we enforce a limit on the depth
		// of nested triggers. This is also a safeguard in case we have a bug that
		// results in an infinite trigger loop.
		var triggerDepth int
		if triggerDepthValue := ctx.Value(triggerDepthKey{}); triggerDepthValue != nil {
			triggerDepth = triggerDepthValue.(int)
		}
		if limit := int(p.SessionData().RecursionDepthLimit); triggerDepth > limit {
			telemetry.Inc(sqltelemetry.RecursionDepthLimitReached)
			err = pgerror.Newf(pgcode.TriggeredActionException,
				"trigger reached recursion depth limit: %d", limit)
			return nil, err
		}
		ctx = context.WithValue(ctx, triggerDepthKey{}, triggerDepth+1)
	}

	var g routineGenerator
	g.init(p, expr, args)
	defer g.Close(ctx)
	err = g.Start(ctx, p.Txn())
	if err != nil {
		return nil, err
	}

	hasNext, err := g.Next(ctx)
	if err != nil {
		return nil, err
	}

	var res tree.Datum
	if !hasNext {
		// The result is NULL if no rows were returned by the last statement in
		// the routine.
		res = tree.DNull
	} else {
		// The result is the first and only column in the row returned by the
		// last statement in the routine.
		row, err := g.Values()
		if err != nil {
			return nil, err
		}
		res = row[0]
	}
	if len(args) == 0 && !expr.EnableStepping {
		// Cache the result if there are zero arguments and stepping is
		// disabled.
		expr.CachedResult = res
	}
	return res, nil
}

type triggerDepthKey struct{}

// RoutineExprGenerator returns an eval.ValueGenerator that produces the results
// of a routine.
func (p *planner) RoutineExprGenerator(
	ctx context.Context, expr *tree.RoutineExpr, args tree.Datums,
) eval.ValueGenerator {
	var g routineGenerator
	g.init(p, expr, args)
	return &g
}

// routineGenerator is an eval.ValueGenerator that produces the result of a
// routine.
type routineGenerator struct {
	p        *planner
	expr     *tree.RoutineExpr
	args     tree.Datums
	rch      rowContainerHelper
	rci      *rowContainerIterator
	currVals tree.Datums
	// deferredRoutine encapsulates the information needed to execute a nested
	// routine that has deferred its execution.
	deferredRoutine struct {
		expr *tree.RoutineExpr
		args tree.Datums
	}
}

var _ eval.ValueGenerator = &routineGenerator{}
var _ eval.DeferredRoutineSender = &routineGenerator{}

// init initializes a routineGenerator.
func (g *routineGenerator) init(p *planner, expr *tree.RoutineExpr, args tree.Datums) {
	*g = routineGenerator{
		p:    p,
		expr: expr,
		args: args,
	}
}

// reset closes and re-initializes a routineGenerator for reuse.
// TODO(drewk): we should hold on to memory for the row container.
func (g *routineGenerator) reset(
	ctx context.Context, p *planner, expr *tree.RoutineExpr, args tree.Datums,
) {
	g.Close(ctx)
	g.init(p, expr, args)
}

// ResolvedType is part of the eval.ValueGenerator interface.
func (g *routineGenerator) ResolvedType() *types.T {
	return g.expr.ResolvedType()
}

// Start is part of the eval.ValueGenerator interface.
func (g *routineGenerator) Start(ctx context.Context, txn *kv.Txn) (err error) {
	enabledStepping := false
	var prevSteppingMode kv.SteppingMode
	var prevSeqNum enginepb.TxnSeq
	for {
		if g.expr.EnableStepping && !enabledStepping {
			prevSteppingMode = txn.ConfigureStepping(ctx, kv.SteppingEnabled)
			prevSeqNum = txn.GetReadSeqNum()
			enabledStepping = true
		}
		err = g.startInternal(ctx, txn)
		if err != nil {
			return err
		}
		if g.deferredRoutine.expr == nil {
			// No tail-call optimization.
			if enabledStepping {
				_ = txn.ConfigureStepping(ctx, prevSteppingMode)
				return txn.SetReadSeqNum(prevSeqNum)
			}
			return nil
		}
		// A nested routine in tail-call position deferred its execution until now.
		// Since it's in tail-call position, evaluating it will give the result of
		// this routine as well.
		g.reset(ctx, g.p, g.deferredRoutine.expr, g.deferredRoutine.args)
	}
}

// startInternal implements logic for a single execution of a routine.
// TODO(mgartner): We can cache results for future invocations of the routine by
// creating a new iterator over an existing row container helper if the routine
// is cache-able (i.e., there are no arguments to the routine and stepping is
// disabled).
func (g *routineGenerator) startInternal(ctx context.Context, txn *kv.Txn) (err error) {
	rt := g.expr.ResolvedType()
	var retTypes []*types.T
	if g.expr.MultiColOutput {
		// A routine with multiple output column should have its types in a tuple.
		if rt.Family() != types.TupleFamily {
			return errors.AssertionFailedf("routine expected to return multiple columns")
		}
		retTypes = rt.TupleContents()
	} else {
		retTypes = []*types.T{g.expr.ResolvedType()}
	}
	g.rch.Init(ctx, retTypes, g.p.ExtendedEvalContext(), "routine" /* opName */)

	// If this is the start of a PLpgSQL block with an exception handler, create a
	// savepoint.
	err = g.maybeInitBlockState(ctx)
	if err != nil {
		return err
	}

	// Execute each statement in the routine sequentially.
	stmtIdx := 0
	ef := newExecFactory(ctx, g.p)
	rrw := NewRowResultWriter(&g.rch)
	var cursorHelper *plpgsqlCursorHelper
	err = g.expr.ForEachPlan(ctx, ef, g.args, func(plan tree.RoutinePlan, stmtForDistSQLDiagram string, isFinalPlan bool) error {
		stmtIdx++
		opName := "udf-stmt-" + g.expr.Name + "-" + strconv.Itoa(stmtIdx)
		ctx, sp := tracing.ChildSpan(ctx, opName)
		defer sp.Finish()

		var w rowResultWriter
		openCursor := stmtIdx == 1 && g.expr.CursorDeclaration != nil
		if isFinalPlan {
			// The result of this statement is the routine's output.
			w = rrw
		} else if openCursor {
			// The result of the first statement will be used to open a SQL cursor.
			cursorHelper, err = g.newCursorHelper(plan.(*planComponents))
			if err != nil {
				return err
			}
			w = NewRowResultWriter(&cursorHelper.container)
		} else {
			// The result of this statement is not needed. Use a rowResultWriter that
			// drops all rows added to it.
			w = &droppingResultWriter{}
		}

		// Place a sequence point before each statement in the routine for volatile
		// functions. Unlike Postgres, we don't allow the txn's external read
		// snapshot to advance, because we do not support restoring the txn's prior
		// external read snapshot after returning from the volatile function.
		if g.expr.EnableStepping {
			if err := txn.Step(ctx, false /* allowReadTimestampStep */); err != nil {
				return err
			}
		}

		// Run the plan.
		params := runParams{ctx, g.p.ExtendedEvalContext(), g.p}
		err = runPlanInsidePlan(ctx, params, plan.(*planComponents), w, g, stmtForDistSQLDiagram)
		if err != nil {
			return err
		}
		if openCursor {
			return cursorHelper.createCursor(g.p)
		}
		return nil
	})
	if err != nil {
		if cursorHelper != nil && !cursorHelper.addedCursor {
			// The cursor wasn't successfully added to the list, so we clean it up
			// here.
			err = errors.CombineErrors(err, cursorHelper.Close())
		}
		return g.handleException(ctx, err)
	}
	g.rci = newRowContainerIterator(ctx, g.rch)
	return nil
}

// handleException attempts to match the code of the given error to an exception
// handler for the routine. If the error finds a match, the corresponding branch
// for the exception handler is executed as a routine.
//
// handleException uses the current set of arguments for the routine to ensure
// that the exception handler sees up-to-date PLpgSQL variables.
//
// If an error is matched, handleException will roll back the database state
// using the current PLpgSQL block's savepoint. Otherwise, it will do nothing,
// in which case the savepoint will be rolled back either by a parent PLpgSQL
// block (if the error is eventually caught), or when the transaction aborts.
func (g *routineGenerator) handleException(ctx context.Context, err error) error {
	caughtCode := pgerror.GetPGCode(err)
	if caughtCode == pgcode.Uncategorized {
		// It is not safe to catch an uncategorized error.
		return err
	}
	// Attempt to catch the error, starting with the exception handler for the
	// current block, and propagating the error up to ancestor exception handlers
	// if necessary.
	for blockState := g.expr.BlockState; blockState != nil; blockState = blockState.Parent {
		if blockState.ExceptionHandler == nil {
			// This block has no exception handler.
			continue
		}
		if !g.p.Txn().CanUseSavepoint(ctx, blockState.SavepointTok.(kv.SavepointToken)) {
			// The current transaction state does not allow roll-back.
			// TODO(111446): some retryable errors allow the transaction to be rolled
			// back partially (e.g. for read committed). We should be able to take
			// advantage of that mechanism here as well.
			return err
		}
		// Unset the exception handler to indicate that it has already encountered an
		// error.
		exceptionHandler := blockState.ExceptionHandler
		blockState.ExceptionHandler = nil
		var branch *tree.RoutineExpr
		for i, code := range exceptionHandler.Codes {
			caughtException := code == caughtCode
			if code.String() == "OTHERS" {
				// The special OTHERS condition matches any error code apart from
				// query_canceled and assert_failure (though they can still be caught
				// explicitly).
				caughtException = caughtCode != pgcode.QueryCanceled && caughtCode != pgcode.AssertFailure
			}
			if caughtException {
				branch = exceptionHandler.Actions[i]
				break
			}
		}
		if branch != nil {
			cursErr := g.closeCursors(blockState)
			if cursErr != nil {
				// This error is unexpected, so return immediately.
				return errors.CombineErrors(err, errors.WithAssertionFailure(cursErr))
			}
			spErr := g.p.Txn().RollbackToSavepoint(ctx, blockState.SavepointTok.(kv.SavepointToken))
			if spErr != nil {
				// This error is unexpected, so return immediately.
				return errors.CombineErrors(err, errors.WithAssertionFailure(spErr))
			}
			// Truncate the arguments using the number of variables in scope for the
			// current block. This is necessary because the error may originate from
			// a child block, but propagate up to a parent block. See the BlockState
			// comments for further details.
			args := g.args[:blockState.VariableCount]
			g.reset(ctx, g.p, branch, args)

			// Configure stepping for volatile routines so that mutations made by the
			// invoking statement are visible to the routine.
			var prevSteppingMode kv.SteppingMode
			var prevSeqNum enginepb.TxnSeq
			txn := g.p.Txn()
			if g.expr.EnableStepping {
				prevSteppingMode = txn.ConfigureStepping(ctx, kv.SteppingEnabled)
				prevSeqNum = txn.GetReadSeqNum()
			}

			// If handling the exception results in another error, that error can in
			// turn be caught by a parent exception handler. Otherwise, the exception
			// was handled, so just return.
			err = g.startInternal(ctx, txn)
			if err == nil {
				if g.expr.EnableStepping {
					_ = txn.ConfigureStepping(ctx, prevSteppingMode)
					return txn.SetReadSeqNum(prevSeqNum)
				}
				return nil
			}
		}
	}
	// We reached the end of the exception handlers without handling this error.
	return err
}

// closeCursors closes any cursors that were opened within the scope of the
// current block. It is used for PLpgSQL exception handling.
func (g *routineGenerator) closeCursors(blockState *tree.BlockState) error {
	if blockState == nil || blockState.CursorTimestamp == nil {
		return nil
	}
	blockStart := *blockState.CursorTimestamp
	blockState.CursorTimestamp = nil
	var err error
	for name, cursor := range g.p.sqlCursors.list() {
		if cursor.created.After(blockStart) {
			if curErr := g.p.sqlCursors.closeCursor(name); curErr != nil {
				// Try to close all cursors in the scope, even if one throws an error.
				err = errors.CombineErrors(err, curErr)
			}
		}
	}
	return err
}

// maybeInitBlockState creates a savepoint for a routine that marks a transition
// into a PL/pgSQL block with an exception handler. It also tracks the current
// timestamp in order to correctly roll back cursors opened within the block.
//
// Note that it is not necessary to explicitly release the savepoint at any
// point, because it does not add any overhead.
func (g *routineGenerator) maybeInitBlockState(ctx context.Context) error {
	blockState := g.expr.BlockState
	if blockState == nil || !g.expr.BlockStart {
		return nil
	}
	if blockState.ExceptionHandler != nil {
		// Drop down a savepoint for the current scope.
		var err error
		if blockState.SavepointTok, err = g.p.Txn().CreateSavepoint(ctx); err != nil {
			return err
		}
		// Save the current timestamp, so that cursors opened from now on can be
		// rolled back by the exception handler.
		curTime := timeutil.Now()
		blockState.CursorTimestamp = &curTime
	}
	return nil
}

// Next is part of the eval.ValueGenerator interface.
func (g *routineGenerator) Next(ctx context.Context) (bool, error) {
	var err error
	g.currVals, err = g.rci.Next()
	if err != nil {
		return false, err
	}
	return g.currVals != nil, nil
}

// Values is part of the eval.ValueGenerator interface.
func (g *routineGenerator) Values() (tree.Datums, error) {
	return g.currVals, nil
}

// Close is part of the eval.ValueGenerator interface.
func (g *routineGenerator) Close(ctx context.Context) {
	if g.rci != nil {
		g.rci.Close()
	}
	g.rch.Close(ctx)
	*g = routineGenerator{}
}

var tailCallOptimizationEnabled = metamorphic.ConstantWithTestBool(
	"tail-call-optimization-enabled",
	true,
)

func (g *routineGenerator) CanOptimizeTailCall(nestedRoutine *tree.RoutineExpr) bool {
	// Tail-call optimization is allowed only if the current routine will not
	// perform any work after its body statements finish executing.
	//
	// Note: cursors are opened after the first body statement, and there is
	// always more than one body statement if a cursor is opened. This is enforced
	// during exec-building. For this reason, we only have to check for an
	// exception handler.
	if g.expr.BlockState != nil {
		// If the current routine has an exception handler (which is the case when
		// BlockState is non-nil), the nested routine must either be part of the
		// same PL/pgSQL block, or a child block. Otherwise, enabling TCO could
		// cause execution to skip the exception handler.
		childBlock := nestedRoutine.BlockState
		if childBlock == nil {
			return false
		}
		return childBlock == g.expr.BlockState || childBlock.Parent == g.expr.BlockState
	}
	return true
}

func (g *routineGenerator) SendDeferredRoutine(nestedRoutine *tree.RoutineExpr, args tree.Datums) {
	g.deferredRoutine.expr = nestedRoutine
	g.deferredRoutine.args = args
}

func (g *routineGenerator) newCursorHelper(plan *planComponents) (*plpgsqlCursorHelper, error) {
	open := g.expr.CursorDeclaration
	if open.NameArgIdx < 0 || open.NameArgIdx >= len(g.args) {
		panic(errors.AssertionFailedf("unexpected name argument index: %d", open.NameArgIdx))
	}
	if g.args[open.NameArgIdx] == tree.DNull {
		return nil, errors.AssertionFailedf("expected non-null cursor name")
	}
	cursorName := tree.Name(tree.MustBeDString(g.args[open.NameArgIdx]))
	if cursorName == "" {
		// Specifying the empty string as a cursor name conflicts with the
		// "unnamed" portal, which always exists.
		return nil, pgerror.Newf(pgcode.DuplicateCursor, "cursor \"\" already in use")
	}
	// The CloseCursorsAtCommit setting provides oracle-compatible behavior, where
	// cursors are holdable by default.
	withHold := !g.p.SessionData().CloseCursorsAtCommit
	planCols := plan.main.planColumns()
	cursorHelper := &plpgsqlCursorHelper{
		cursorName: cursorName,
		cursorSql:  open.CursorSQL,
		withHold:   withHold,
	}
	// Use context.Background(), since the cursor can outlive the context in which
	// it was created.
	cursorHelper.ctx = context.Background()
	cursorHelper.resultCols = make(colinfo.ResultColumns, len(planCols))
	copy(cursorHelper.resultCols, planCols)
	mon := g.p.Mon()
	if withHold {
		mon = g.p.sessionMonitor
		if mon == nil {
			return nil, errors.AssertionFailedf("cannot open cursor WITH HOLD without an active session")
		}
	}
	cursorHelper.container.InitWithParentMon(
		cursorHelper.ctx,
		getTypesFromResultColumns(planCols),
		mon,
		g.p.ExtendedEvalContextCopy(),
		"routine_open_cursor", /* opName */
	)
	return cursorHelper, nil
}

// plpgsqlCursorHelper wraps a row container in order to feed the results of
// executing a SQL statement to a SQL cursor. Note that the SQL statement is not
// lazily executed; its entire result is written to the container.
//
// TODO(#111479): while the row container can spill to disk, we should default
// to lazy execution for cursors for performance reasons.
type plpgsqlCursorHelper struct {
	persistedCursorHelper

	cursorName  tree.Name
	cursorSql   string
	addedCursor bool
	withHold    bool
}

var _ isql.Rows = &plpgsqlCursorHelper{}

func (h *plpgsqlCursorHelper) createCursor(p *planner) error {
	h.iter = newRowContainerIterator(h.ctx, h.container)
	cursor := &sqlCursor{
		Rows:       h,
		readSeqNum: p.txn.GetReadSeqNum(),
		txn:        p.txn,
		statement:  h.cursorSql,
		created:    timeutil.Now(),
		withHold:   h.withHold,
		persisted:  true,
	}
	if err := p.checkIfCursorExists(h.cursorName); err != nil {
		return err
	}
	if err := p.sqlCursors.addCursor(h.cursorName, cursor); err != nil {
		return err
	}
	h.addedCursor = true
	return nil
}

// storedProcTxnStateAccessor provides a method for stored procedures to request
// that the current transaction be committed or aborted and supply a
// continuation stored procedure to resume execution in the new transaction.
type storedProcTxnStateAccessor struct {
	ex *connExecutor
}

func (a *storedProcTxnStateAccessor) setStoredProcTxnState(
	txnOp tree.StoredProcTxnOp, txnModes *tree.TransactionModes, resumeProc *memo.Memo,
) {
	if a.ex == nil {
		panic(errors.AssertionFailedf("setStoredProcTxnState is not supported without connExecutor"))
	}
	a.ex.extraTxnState.storedProcTxnState.txnOp = txnOp
	a.ex.extraTxnState.storedProcTxnState.txnModes = txnModes
	a.ex.extraTxnState.storedProcTxnState.resumeProc = resumeProc
}

func (a *storedProcTxnStateAccessor) getTxnOp() tree.StoredProcTxnOp {
	if a.ex == nil {
		return tree.StoredProcTxnNoOp
	}
	return a.ex.extraTxnState.storedProcTxnState.txnOp
}

func (a *storedProcTxnStateAccessor) getResumeProc() *memo.Memo {
	if a.ex == nil {
		return nil
	}
	return a.ex.extraTxnState.storedProcTxnState.resumeProc
}

func (a *storedProcTxnStateAccessor) getTxnModes() *tree.TransactionModes {
	if a.ex == nil {
		return nil
	}
	return a.ex.extraTxnState.storedProcTxnState.txnModes
}

// EvalTxnControlExpr produces the side effects of a COMMIT or ROLLBACK
// statement within a PL/pgSQL stored procedure. It directs the connExecutor to
// either commit or abort the current transaction, and provides a plan for a
// "continuation" stored procedure which will resume execution in the new
// transaction.
//
// EvalTxnControlExpr returns NULL, but this result is simply discarded without
// being examined or modified when the transaction finishes.
func (p *planner) EvalTxnControlExpr(
	ctx context.Context, expr *tree.TxnControlExpr, args tree.Datums,
) (tree.Datum, error) {
	if !p.EvalContext().TxnImplicit {
		// Transaction control statements are not allowed in explicit transactions.
		return nil, errors.WithDetail(
			pgerror.Newf(pgcode.InvalidTransactionTermination, "invalid transaction termination"),
			"PL/pgSQL COMMIT/ROLLBACK is not allowed in an explicit transaction",
		)
	}
	resumeProc, err := expr.Gen(ctx, args)
	if err != nil {
		return nil, err
	}
	p.storedProcTxnState.setStoredProcTxnState(expr.Op, &expr.Modes, resumeProc.(*memo.Memo))
	return tree.DNull, nil
}
