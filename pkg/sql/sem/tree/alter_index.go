// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// AlterIndex represents an ALTER INDEX statement.
type AlterIndex struct {
	IfExists bool
	Index    TableIndexName
	Cmds     AlterIndexCmds
}

var _ Statement = &AlterIndex{}

// Format implements the NodeFormatter interface.
func (node *AlterIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER INDEX ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Index)
	ctx.FormatNode(&node.Cmds)
}

// AlterIndexCmds represents a list of index alterations.
type AlterIndexCmds []AlterIndexCmd

// Format implements the NodeFormatter interface.
func (node *AlterIndexCmds) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(",")
		}
		ctx.FormatNode(n)
	}
}

// AlterIndexCmd represents an index modification operation.
type AlterIndexCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterIndex*) conform to the AlterIndexCmd interface.
	alterIndexCmd()
}

func (*AlterIndexPartitionBy) alterIndexCmd() {}

var _ AlterIndexCmd = &AlterIndexPartitionBy{}

// AlterIndexPartitionBy represents an ALTER INDEX PARTITION BY
// command.
type AlterIndexPartitionBy struct {
	*PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *AlterIndexPartitionBy) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.PartitionBy)
}
