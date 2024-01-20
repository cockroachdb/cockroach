// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// Call represents a CALL statement to invoke a procedure.
type Call struct {
	// Proc contains the procedure reference and the arguments of the procedure.
	Proc *FuncExpr

	// AsOf is an optional AS OF SYSTEM TIME clause.
	AsOf AsOfClause
}

// Format implements the NodeFormatter interface.
func (node *Call) Format(ctx *FmtCtx) {
	ctx.WriteString("CALL ")
	ctx.FormatNode(node.Proc)
	if node.AsOf.Expr != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.AsOf)
	}
}

var _ Statement = &Call{}
