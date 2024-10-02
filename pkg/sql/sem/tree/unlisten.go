// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Unlisten represents a UNLISTEN statement.
type Unlisten struct {
	ChannelName *UnresolvedObjectName
	Star        bool
}

var _ Statement = &Unlisten{}

// Format implements the NodeFormatter interface.
func (node *Unlisten) Format(ctx *FmtCtx) {
	ctx.WriteString("UNLISTEN ")
	if node.Star {
		ctx.WriteString("* ")
	} else if node.ChannelName != nil {
		ctx.FormatNode(node.ChannelName)
	}
}

// String implements the Statement interface.
func (node *Unlisten) String() string {
	return AsString(node)
}
