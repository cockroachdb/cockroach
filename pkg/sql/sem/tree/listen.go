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

// Listen represents a LISTEN statement.
type Listen struct {
	ChannelName *UnresolvedObjectName
}

var _ Statement = &Listen{}

// Format implements the NodeFormatter interface.
func (node *Listen) Format(ctx *FmtCtx) {
	ctx.WriteString("Listen ")
	ctx.FormatNode(node.ChannelName)
}

// String implements the Statement interface.
func (node *Listen) String() string {
	return AsString(node)
}
