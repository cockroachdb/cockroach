// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// CreateChangefeed represents a CREATE CHANGEFEED statement.
type CreateChangefeed struct {
	Targets TargetList
	SinkURI Expr
	Options KVOptions
}

var _ Statement = &CreateChangefeed{}

// Format implements the NodeFormatter interface.
func (node *CreateChangefeed) Format(ctx *FmtCtx) {
	if node.SinkURI != nil {
		ctx.WriteString("CREATE ")
	} else {
		// Sinkless feeds don't really CREATE anything, so the syntax omits the
		// prefix. They're also still EXPERIMENTAL, so they get marked as such.
		ctx.WriteString("EXPERIMENTAL ")
	}
	ctx.WriteString("CHANGEFEED FOR ")
	ctx.FormatNode(&node.Targets)
	if node.SinkURI != nil {
		ctx.WriteString(" INTO ")
		ctx.FormatNode(node.SinkURI)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}
