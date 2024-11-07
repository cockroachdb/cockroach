// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// RoutinePlanGenerator generates a plan for the execution of each statement
// within a routine. The given RoutinePlanGeneratedFunc is called for each plan
// generated.
//
// A RoutinePlanGenerator must return an error if the RoutinePlanGeneratedFunc
// returns an error.
type RoutinePlanGenerator func(
	_ context.Context, _ RoutineExecFactory, args Datums, fn RoutinePlanGeneratedFunc,
) error

// RoutinePlanGeneratedFunc is the function type that is called for each plan
// enumerated by a RoutinePlanGenerator.
// - stmtForDistSQLDiagram, if set, will be used when generating DistSQL diagram
// to specify the SQL stmt corresponding to the plan.
// - isFinalPlan is true if no more plans will be generated after the current
// plan.
type RoutinePlanGeneratedFunc func(plan RoutinePlan, stmtForDistSQLDiagram string, isFinalPlan bool) error

// RoutinePlan represents a plan for a statement in a routine. It currently maps
// to exec.Plan. We use the empty interface here rather than exec.Plan to avoid
// import cycles.
type RoutinePlan interface{}

// RoutineExecFactory is a factory used to build optimizer expressions into
// execution plans for statements within a RoutineExpr. It currently maps to
// exec.Factory. We use the empty interface here rather than exec.Factory to
// avoid import cycles.
type RoutineExecFactory interface{}

// RoutineExpr represents sequential execution of multiple statements. For
// example, it is used to represent execution of statements in the body of a
// user-defined function. It is only created by execbuilder - it is never
// constructed during parsing.
type RoutineExpr struct {
	// Name is a string name that describes the RoutineExpr, e.g., the name of
	// the UDF that the RoutineExpr is built from.
	Name string

	// Args contains the argument expressions to the routine.
	Args TypedExprs

	// ForEachPlan generates a plan for each statement in the routine.
	ForEachPlan RoutinePlanGenerator

	// Typ is the type of the routine's result.
	Typ *types.T

	// EnableStepping configures step-wise execution for the statements in the
	// routine. When true, statements within the routine will see mutations made
	// by the statement invoking the routine. They will also see changes made by
	// previous statements in the routine. When false, statements within the
	// routine will see a snapshot of the data as of the start of the statement
	// invoking the routine.
	EnableStepping bool

	// CachedResult stores the datum that the routine evaluates to, if the
	// routine is nullary (i.e., it has zero arguments) and stepping is
	// disabled. It is populated during the first execution of the routine.
	// Subsequent invocations of the routine return Result directly, rather than
	// being executed.
	//
	// The cache is never cleared - it lives for the entire lifetime of the
	// routine. Therefore, to "invalidate" the cache, a new routine must be
	// created. For example, consider:
	//
	//   CREATE TABLE t (i INT);
	//   CREATE FUNCTION f() RETURNS INT VOLATILE LANGUAGE SQL AS $$
	//     SELECT i FROM (VALUES (1), (2)) v(i) WHERE i = (SELECT max(i) FROM t)
	//   $$;
	//   SELECT f() FROM (VALUES (3), (4));
	//
	// The optimizer query plan for the SELECT contains two expressions that are
	// built as routines, the UDF and the subquery within it. Each invocation of
	// the UDF's routine will re-plan the body of the function, creating a new
	// routine for the subquery. This effectively "invalidates" the cached
	// result in the subquery routines each time the UDF is invoked. Within a
	// single invocation of the UDF, however, the subquery will only be executed
	// once and the cached result will be returned if the subquery is evaluated
	// multiple times (e.g., for each value of i in v).
	CachedResult Datum

	// CalledOnNullInput is true if the function should be called when any of
	// its inputs are NULL. If false, the function will not be evaluated in the
	// presence of null inputs, and will instead evaluate directly to NULL.
	//
	// NOTE: This boolean only affects evaluation of Routines within project-set
	// operators. This can apply to scalar routines if they are used as data
	// source (e.g. SELECT * FROM scalar_udf()), and always applies to
	// set-returning routines.
	// Strict non-set-returning routines are not invoked when their arguments
	// are NULL because optbuilder wraps them in a CASE expressions.
	CalledOnNullInput bool

	// MultiColOutput is true if the function may return multiple columns.
	MultiColOutput bool

	// Generator is true if the function may output a set of rows.
	Generator bool

	// TailCall is true if the routine is in a tail-call position in a parent
	// routine. This means that once execution reaches this routine, the parent
	// routine will return the result of evaluating this routine with no further
	// changes. For routines in a tail-call position we implement an optimization
	// to avoid nesting execution. This is necessary for performant PLpgSQL loops.
	TailCall bool

	// Procedure is true if the routine is a procedure being invoked by CALL.
	Procedure bool

	// TriggerFunc is true if this routine is a trigger function. Note that it is
	// only set for the outermost routine, and not any sub-routines used to
	// implement the PL/pgSQL body.
	TriggerFunc bool

	// BlockStart is true if this routine marks the start of a PL/pgSQL block with
	// an exception handler. It determines when to initialize the state shared
	// between sub-routines for the block.
	BlockStart bool

	// BlockState holds the information needed to coordinate error-handling
	// between the sub-routines that make up a PLpgSQL exception block.
	BlockState *BlockState

	// CursorDeclaration contains the information needed to open a SQL cursor with
	// the result of the *first* body statement. It may be unset.
	CursorDeclaration *RoutineOpenCursor
}

// NewTypedRoutineExpr returns a new RoutineExpr that is well-typed.
func NewTypedRoutineExpr(
	name string,
	args TypedExprs,
	gen RoutinePlanGenerator,
	typ *types.T,
	enableStepping bool,
	calledOnNullInput bool,
	multiColOutput bool,
	generator bool,
	tailCall bool,
	procedure bool,
	triggerFunc bool,
	blockStart bool,
	blockState *BlockState,
	cursorDeclaration *RoutineOpenCursor,
) *RoutineExpr {
	return &RoutineExpr{
		Args:              args,
		ForEachPlan:       gen,
		Typ:               typ,
		EnableStepping:    enableStepping,
		Name:              name,
		CalledOnNullInput: calledOnNullInput,
		MultiColOutput:    multiColOutput,
		Generator:         generator,
		TailCall:          tailCall,
		Procedure:         procedure,
		TriggerFunc:       triggerFunc,
		BlockStart:        blockStart,
		BlockState:        blockState,
		CursorDeclaration: cursorDeclaration,
	}
}

// TypeCheck is part of the Expr interface.
func (node *RoutineExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return node, nil
}

// ResolvedType is part of the TypedExpr interface.
func (node *RoutineExpr) ResolvedType() *types.T {
	return node.Typ
}

// Format is part of the Expr interface.
func (node *RoutineExpr) Format(ctx *FmtCtx) {
	ctx.FormatName(node.Name)
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Args)
	ctx.WriteByte(')')
}

// Walk is part of the Expr interface.
func (node *RoutineExpr) Walk(v Visitor) Expr {
	// Cannot walk into a routine, so this is a no-op.
	return node
}

// RoutineExceptionHandler encapsulates the information needed to match and
// handle errors for the exception block of a routine defined with PLpgSQL.
type RoutineExceptionHandler struct {
	// Codes is a list of pgcode strings used to match exceptions. Note that as a
	// special case, the code may be "OTHERS", which matches most error codes.
	Codes []pgcode.Code

	// Actions contains a routine to handle each error code.
	Actions []*RoutineExpr
}

// RoutineOpenCursor stores the information needed to correctly open a cursor
// with the output of a routine.
type RoutineOpenCursor struct {
	// NameArgIdx is the index of the routine argument that contains the name of
	// the cursor that will be created.
	NameArgIdx int

	// Scroll is the scroll option for the cursor, if one was specified. The other
	// cursor options are not valid in PLpgSQL.
	Scroll CursorScrollOption

	// CursorSQL is a formatted string used to associate the original SQL
	// statement with the cursor.
	CursorSQL string
}

// BlockState is shared state between all routines that make up a PLpgSQL block.
// It allows for coordination between the routines for exception handling.
type BlockState struct {
	// Parent is a pointer to this block's parent, if any. Note that this refers
	// to a parent within the same routine; nested routine calls currently do not
	// interact with one another directly (e.g. through TCO, see #119956).
	Parent *BlockState

	// VariableCount tracks the number of variables that are in scope for this
	// block, so that the correct arguments can be supplied to an exception
	// handler when an error originates from a "descendant" block. Example:
	//
	// DECLARE
	//   x INT := 0;
	// BEGIN
	//   DECLARE
	//     y INT := 1;
	//   BEGIN
	//     y = 1 // 0;
	//   END;
	// EXCEPTION WHEN division_by_zero THEN
	//   RETURN 0;
	// END
	//
	// In this example, the error is thrown from the inner block, where variables
	// "x" and "y" are both in scope. Therefore, we will have access to values for
	// both variables. However, the error will be caught by the outer block, for
	// which only "x" is in scope. Therefore, the outer block must truncate the
	// values before supplying them to its exception handler as arguments.
	//
	// NOTE: the list of variables in an outer block *always* form a prefix of the
	// variables in a nested block.
	VariableCount int

	// ExceptionHandler is the exception handler for the current block, if any.
	ExceptionHandler *RoutineExceptionHandler

	// SavepointTok allows the exception handler to roll-back changes to database
	// state if an error occurs during its execution. It currently maps to
	// kv.SavepointToken. We use the empty interface here rather than
	// kv.SavepointToken to avoid import cycles.
	SavepointTok interface{}

	// CursorTimestamp is the timestamp at which control transitioned into this
	// PL/pgSQL block. It is used to close (only) cursors which were opened within
	// the scope of the block when an exception is caught.
	CursorTimestamp *time.Time
}

// StoredProcTxnOp indicates whether a stored procedure has requested that the
// current transaction be committed or aborted.
type StoredProcTxnOp uint8

const (
	StoredProcTxnNoOp StoredProcTxnOp = iota
	StoredProcTxnCommit
	StoredProcTxnRollback
)

// String returns a string representation of the transaction control statement.
func (txnOp StoredProcTxnOp) String() string {
	switch txnOp {
	case StoredProcTxnNoOp:
		return "NO-OP"
	case StoredProcTxnCommit:
		return "COMMIT"
	case StoredProcTxnRollback:
		return "ROLLBACK"
	default:
		panic(errors.AssertionFailedf("unknown txn control op: %d", txnOp))
	}
}

// StoredProcContinuation represents the plan for a CALL statement that resumes
// execution of a stored procedure that paused in order to execute a COMMIT or
// ROLLBACK statement. Currently maps to *memo.Memo.
type StoredProcContinuation interface{}

// TxnControlPlanGenerator builds the plan for a StoredProcContinuation.
type TxnControlPlanGenerator func(
	ctx context.Context, args Datums,
) (StoredProcContinuation, error)

// TxnControlExpr implements PL/pgSQL COMMIT and ROLLBACK statements. It directs
// the session to end the current transaction, and provides a plan to resume
// execution in a new transaction in the form of StoredProcContinuation.
type TxnControlExpr struct {
	Op    StoredProcTxnOp
	Modes TransactionModes
	Args  TypedExprs
	Gen   TxnControlPlanGenerator

	Name string
	Typ  *types.T
}

var _ Expr = &TxnControlExpr{}

// NewTxnControlExpr returns a new TxnControlExpr that is well-typed.
func NewTxnControlExpr(
	opType StoredProcTxnOp,
	txnModes TransactionModes,
	args TypedExprs,
	gen TxnControlPlanGenerator,
	name string,
	typ *types.T,
) *TxnControlExpr {
	return &TxnControlExpr{
		Op:    opType,
		Modes: txnModes,
		Args:  args,
		Gen:   gen,
		Name:  name,
		Typ:   typ,
	}
}

// TypeCheck is part of the Expr interface.
func (node *TxnControlExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return node, nil
}

// ResolvedType is part of the TypedExpr interface.
func (node *TxnControlExpr) ResolvedType() *types.T {
	return node.Typ
}

// Format is part of the Expr interface.
func (node *TxnControlExpr) Format(ctx *FmtCtx) {
	if buildutil.CrdbTestBuild {
		if node.Op == StoredProcTxnNoOp {
			panic(errors.AssertionFailedf("called Format for no-op txn control expr"))
		}
	}
	ctx.Printf("%s; CALL %s(", node.Op, node.Name)
	ctx.FormatNode(&node.Args)
	ctx.WriteByte(')')
}

// Walk is part of the Expr interface.
func (node *TxnControlExpr) Walk(v Visitor) Expr {
	// Cannot walk into a TxnOp, so this is a no-op.
	return node
}
