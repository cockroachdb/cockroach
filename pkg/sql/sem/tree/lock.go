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

type Lock struct {
	TableName *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *Lock) Format(ctx *FmtCtx) {
	ctx.WriteString("LOCK TABLE ")
	ctx.FormatNode(node.TableName)
	ctx.WriteString(" IN ACCESS SHARE MODE")
}
