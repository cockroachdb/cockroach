// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/errors"
)

// DispatcherExpr is used to execute logically nested routine calls without
// having to nest the execution context. Whenever a nested routine would be
// evaluated as specified by the logical plan, control instead returns to the
// dispatcher, which executes the routine in its own top-level context.
// This is useful for two reasons:
//
//  1. Execution of deeply nested routines only consumes the resources for
//     one routine at a time, since the previous routine is cleaned up whenever
//     control returns to the DispatcherExpr.
//  2. This will provide a single location to implement explicit transaction
//     control for PL/pgSQL routines (tracked in #115294).
//
// A DispatcherExpr is evaluated according to the following steps:
//
//  1. If this is the first iteration, the Input expression is evaluated.
//     Otherwise, the branch indicated by the last iteration is evaluated.
//  2. If a DispatchExpr was evaluated in the last step, it will have passed the
//     next branch to execute, and its arguments. Return to step (1).
//  3. If evaluation never reached a DispatchExpr, execution has finished. The
//     result of evaluating the expression in step (1) is the final result.
//
// These steps are implemented in eval/expr.go.
type DispatcherExpr struct {
	// Input is the initial expression to be evaluated. It may refer to the
	// dispatcher's branches, but is not itself a branch. Generally, it will
	// contain a DispatchExpr that will indicate a branch to be executed after
	// execution of the Input expressions finishes to obtain the final result.
	Input TypedExpr

	// Branches is an ordered set of routines that may be invoked by DispatchExprs
	// within Input and the branches themselves.
	Branches []*RoutineExpr

	// Receiver allows the DispatcherExpr to receive the next branch to execute
	// and its arguments from a descendant DispatchExpr.
	Receiver *DispatchChannel

	// Typ is the return type of the DispatcherExpr.
	Typ *types.T
}

var _ TypedExpr = &DispatcherExpr{}

// NewDispatcherExpr returns a new DispatcherExpr with all fields initialized.
func NewDispatcherExpr(
	input TypedExpr, branches []*RoutineExpr, dispatchChannel *DispatchChannel, typ *types.T,
) *DispatcherExpr {
	return &DispatcherExpr{
		Input:    input,
		Branches: branches,
		Receiver: dispatchChannel,
		Typ:      typ,
	}
}

// TypeCheck is part of the Expr interface.
func (node *DispatcherExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return node, nil
}

// ResolvedType is part of the TypedExpr interface.
func (node *DispatcherExpr) ResolvedType() *types.T {
	return node.Typ
}

// Format is part of the Expr interface.
func (node *DispatcherExpr) Format(ctx *FmtCtx) {
	ctx.Printf("dispatcher")
}

// Walk is part of the Expr interface.
func (node *DispatcherExpr) Walk(v Visitor) Expr {
	// Cannot walk into a DispatcherExpr, so this is a no-op.
	return node
}

// DispatchExpr represents a nested routine call. It avoids nesting *execution*
// of the nested call by deferring the execution until control reaches the
// top-level DispatcherExpr. See the DispatcherExpr comment for further details.
type DispatchExpr struct {
	// Args is the set of arguments with which the nested routine must be
	// evaluated.
	Args TypedExprs

	// Branch is the index of the next branch of the parent DispatcherExpr that
	// should be executed with the Args.
	BranchIdx int

	// Sender is used to directly pass the index of the next branch to be
	// evaluated and its arguments to the parent DispatcherExpr.
	Sender *DispatchChannel

	// Type is the return type of the DispatchExpr.
	Typ *types.T
}

// NewDispatchExpr returns a new DispatchExpr with all fields initialized.
func NewDispatchExpr(
	args TypedExprs, branchIdx int, dispatcherChannel *DispatchChannel, typ *types.T,
) *DispatchExpr {
	return &DispatchExpr{
		Args:      args,
		BranchIdx: branchIdx,
		Sender:    dispatcherChannel,
		Typ:       typ,
	}
}

var _ TypedExpr = &DispatchExpr{}

// TypeCheck is part of the Expr interface.
func (node *DispatchExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return node, nil
}

// ResolvedType is part of the TypedExpr interface.
func (node *DispatchExpr) ResolvedType() *types.T {
	return node.Typ
}

// Format is part of the Expr interface.
func (node *DispatchExpr) Format(ctx *FmtCtx) {
	ctx.Printf("dispatch: %d(", node.BranchIdx)
	ctx.FormatNode(&node.Args)
	ctx.WriteByte(')')
}

// Walk is part of the Expr interface.
func (node *DispatchExpr) Walk(v Visitor) Expr {
	// Cannot walk into a DispatchExpr, so this is a no-op.
	return node
}

// DispatchChannel provides a method for DispatchExpr instances to communicate
// with their parent DispatcherExpr.
type DispatchChannel struct {
	branchIdx int
	args      Datums
	hasNext   bool
}

// Send is called by a DispatchExpr to invoke a branch of a DispatcherExpr.
func (ch *DispatchChannel) Send(branchIdx int, args Datums) {
	if ch.hasNext {
		panic(errors.AssertionFailedf("invoked Send twice"))
	}
	ch.branchIdx = branchIdx
	ch.args = args
	ch.hasNext = true
}

// HasNext indicates the presence of a request to execute a dispatcher branch.
func (ch *DispatchChannel) HasNext() bool {
	return ch.hasNext
}

// Next retrieves the index of the next dispatcher branch that should execute,
// as well as arguments to invoke it with. It can only be called is HasNext()
// returns true.
func (ch *DispatchChannel) Next() (branch int, args Datums) {
	if !ch.hasNext {
		panic(errors.AssertionFailedf("cannot call Next without a previous call to Send"))
	}
	branch, args = ch.branchIdx, ch.args
	ch.hasNext = false
	return branch, args
}
