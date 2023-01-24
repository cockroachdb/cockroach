// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// RoutinePlanFn creates a plan for the execution of one statement within a
// routine.
type RoutinePlanFn func(
	_ context.Context, _ RoutineExecFactory, stmtIdx int, args Datums,
) (RoutinePlan, error)

// RoutinePlan represents a plan for a statement in a routine. It currently maps
// to exec.Plan. We use the empty interface here rather then exec.Plan to avoid
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

	// PlanFn returns an exec plan for a given statement in the routine.
	PlanFn RoutinePlanFn

	// NumStmts is the number of statements in the routine.
	NumStmts int

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
}

// NewTypedRoutineExpr returns a new RoutineExpr that is well-typed.
func NewTypedRoutineExpr(
	name string,
	args TypedExprs,
	planFn RoutinePlanFn,
	numStmts int,
	typ *types.T,
	enableStepping bool,
) *RoutineExpr {
	return &RoutineExpr{
		Args:           args,
		PlanFn:         planFn,
		NumStmts:       numStmts,
		Typ:            typ,
		EnableStepping: enableStepping,
		Name:           name,
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
	ctx.Printf("%s(", node.Name)
	for i := range node.Args {
		if i > 0 {
			ctx.WriteString(", ")
		}
		node.Args[i].Format(ctx)
	}
	ctx.WriteByte(')')
}

// Walk is part of the Expr interface.
func (node *RoutineExpr) Walk(v Visitor) Expr {
	// Cannot walk into a routine, so this is a no-op.
	return node
}
