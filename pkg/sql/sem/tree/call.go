// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Call represents a CALL statement to invoke a procedure.
type Call struct {
	// Proc contains the procedure reference and the arguments of the procedure.
	Proc *FuncExpr
}

// Format implements the NodeFormatter interface.
func (node *Call) Format(ctx *FmtCtx) {
	ctx.WriteString("CALL ")
	ctx.FormatNode(node.Proc)
}

var _ Statement = &Call{}
