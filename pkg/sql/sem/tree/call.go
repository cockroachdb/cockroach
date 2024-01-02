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
}

// Format implements the NodeFormatter interface.
func (node *Call) Format(ctx *FmtCtx) {
	ctx.WriteString("CALL ")
	ctx.FormatNode(node.Proc)
}

var _ Statement = &Call{}
