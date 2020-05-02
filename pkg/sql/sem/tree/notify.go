// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// Notify represents a NOTIFY statement.
type Notify struct {
	ChanName Name
	Message  *StrVal
}

// Format implements the NodeFormatter interface.
func (node *Notify) Format(ctx *FmtCtx) {
	ctx.WriteString("NOTIFY ")
	node.ChanName.Format(ctx)
	if node.Message != nil && len(node.Message.s) > 0 {
		ctx.WriteString(", ")
		node.Message.Format(ctx)
	}
}

// Listen represents either a LISTEN or UNLISTEN statement.
type Listen struct {
	ChanName Name
	// Active is true if this is a LISTEN statement, and false if this is an
	// UNLISTEN statement.
	Unlisten bool
	// UnlistenAll is true if this is an UNLISTEN * statement.
	UnlistenAll bool
}

// Format implements the NodeFormatter interface.
func (node *Listen) Format(ctx *FmtCtx) {
	if node.Unlisten {
		ctx.WriteString("UNLISTEN ")
	} else {
		ctx.WriteString("LISTEN ")
	}
	if node.UnlistenAll {
		ctx.WriteString("*")
	} else {
		node.ChanName.Format(ctx)
	}
}
