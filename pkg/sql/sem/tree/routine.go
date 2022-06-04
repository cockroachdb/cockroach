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

// Routine represents a routine. It is never constructed during parsing. All
// functions are parsed as functions. Routines, such as a user-defined function,
// are converted from functions in the optbuilder.
//
// TODO(mgartner): We'll need to keep track of more information here, like
// arguments, volatility, etc.
type Routine struct {
	Args       TypedExprs
	ArgNames   []string
	Statements []string
	Typ        *types.T
}

func (node *Routine) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return node, nil
}

func (node *Routine) ResolvedType() *types.T {
	return types.Int
}

// Format implements the NodeFormatter interface.
func (node *Routine) Format(ctx *FmtCtx) {
	ctx.Printf("Routine")
}

func (node *Routine) String() string {
	return "Routine"
}

func (node *Routine) Walk(v Visitor) Expr {
	// Cannot walk into a routine, so this is a no-op.
	return node
}

type RoutineArgs struct {
	Names  []string
	Values Datums
}

func (ra *RoutineArgs) FindArgWithName(n Name) (int, bool) {
	for i := range ra.Names {
		if string(n) == ra.Names[i] {
			return i, true
		}
	}
	return 0, false
}
