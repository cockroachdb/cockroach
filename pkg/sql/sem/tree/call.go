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
	// Name is the name of the procedure.
	Name *UnresolvedObjectName
	// Exprs contains the arguments to pass to the procedure.
	Exprs Exprs
}

// Format implements the NodeFormatter interface.
func (node *Call) Format(ctx *FmtCtx) {
	ctx.WriteString("CALL ")
	ctx.FormatNode(node.Name)
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
}

var _ Statement = &Call{}
