// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "fmt"

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

func (*AlterIndexPartitionBy) alterIndexCmd()          {}
func (*AlterIndexSetStorageParams) alterIndexCmd()    {}
func (*AlterIndexResetStorageParams) alterIndexCmd()  {}

var _ AlterIndexCmd = &AlterIndexPartitionBy{}
var _ AlterIndexCmd = &AlterIndexSetStorageParams{}
var _ AlterIndexCmd = &AlterIndexResetStorageParams{}

// AlterIndexPartitionBy represents an ALTER INDEX PARTITION BY
// command.
type AlterIndexPartitionBy struct {
	*PartitionByIndex
}

// Format implements the NodeFormatter interface.
func (node *AlterIndexPartitionBy) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.PartitionByIndex)
}

// AlterIndexSetStorageParams represents an ALTER INDEX SET (...) command.
type AlterIndexSetStorageParams struct {
	StorageParams StorageParams
}

// Format implements the NodeFormatter interface.
func (node *AlterIndexSetStorageParams) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET (")
	ctx.FormatNode(&node.StorageParams)
	ctx.WriteString(")")
}

// AlterIndexResetStorageParams represents an ALTER INDEX RESET (...) command.
type AlterIndexResetStorageParams struct {
	Params []string
}

// Format implements the NodeFormatter interface.
func (node *AlterIndexResetStorageParams) Format(ctx *FmtCtx) {
	ctx.WriteString(" RESET (")
	for i, param := range node.Params {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.WriteString(param)
	}
	ctx.WriteString(")")
}

// AlterIndexVisible represents a ALTER INDEX ... [VISIBLE | NOT VISIBLE] statement.
type AlterIndexVisible struct {
	Index        TableIndexName
	Invisibility IndexInvisibility
	IfExists     bool
}

var _ Statement = &AlterIndexVisible{}

// Format implements the NodeFormatter interface.
func (node *AlterIndexVisible) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER INDEX ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Index)
	switch {
	case node.Invisibility.FloatProvided:
		ctx.WriteString(" VISIBILITY " + fmt.Sprintf("%.2f", 1-node.Invisibility.Value))
	case node.Invisibility.Value == 1.0:
		ctx.WriteString(" NOT VISIBLE")
	default:
		ctx.WriteString(" VISIBLE")
	}
}
