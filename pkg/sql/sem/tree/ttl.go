// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// TTL represents a table's TTL metadata.
type TTL struct {
	// IntervalExpr is an unchecked expression which contains the TTL duration.
	IntervalExpr Expr
}

// Format implements the NodeFormatter interface.
func (node *TTL) Format(ctx *FmtCtx) {
	ctx.WriteString("TTL ")
	node.IntervalExpr.Format(ctx)
}
