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

// Listen represents a NOTIFY statement.
type Notify struct {
	ChannelName *UnresolvedObjectName
	// Payload is a constant string expression and it may be empty/nil.
	Payload Expr
}

var _ Statement = &Notify{}

// Format implements the NodeFormatter interface.
func (node *Notify) Format(ctx *FmtCtx) {
	ctx.WriteString("NOTIFY ")
	ctx.FormatNode(node.ChannelName)
}

// String implements the Statement interface.
func (node *Notify) String() string {
	return AsString(node)
}
