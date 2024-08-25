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

// Notify represents a NOTIFY statement.
type Notify struct {
	ChannelName Name
	// Payload is a constant string expression and it may be empty/nil.
	Payload *StrVal
}

var _ Statement = &Notify{}

// Format implements the NodeFormatter interface.
func (node *Notify) Format(ctx *FmtCtx) {
	ctx.WriteString("NOTIFY ")
	ctx.WriteString(node.ChannelName.Normalize())
	if node.Payload != nil {
		ctx.WriteString(", ")
		ctx.FormatNode(node.Payload)
	}
}

// String implements the Statement interface.
func (node *Notify) String() string {
	return AsString(node)
}
