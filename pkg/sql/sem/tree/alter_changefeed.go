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

import "strconv"

// AlterChangefeed represents an ALTER CHANGEFEED statement.
type AlterChangefeed struct {
	JobID int64
	Cmds  AlterChangefeedCmds
}

var _ Statement = &AlterChangefeed{}

// Format implements the NodeFormatter interface.
func (node *AlterChangefeed) Format(ctx *FmtCtx) {
	ctx.WriteString(`ALTER CHANGEFEED `)
	ctx.WriteString(strconv.Itoa(int(node.JobID)))
	ctx.FormatNode(&node.Cmds)
}

type AlterChangefeedCmds []AlterChangefeedCmd

// Format implements the NodeFormatter interface.
func (node *AlterChangefeedCmds) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(" ")
		}
		ctx.FormatNode(n)
	}
}

// AlterChangefeedCmd represents a changefeed modification operation.
type AlterChangefeedCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterChangefeed*) conform to the AlterChangefeedCmd interface.
	alterChangefeedCmd()
}

func (*AlterChangefeedAddTarget) alterChangefeedCmd()  {}
func (*AlterChangefeedDropTarget) alterChangefeedCmd() {}

var _ AlterChangefeedCmd = &AlterChangefeedAddTarget{}
var _ AlterChangefeedCmd = &AlterChangefeedDropTarget{}

type AlterChangefeedAddTarget struct {
	Targets TargetList
}

// Format implements the NodeFormatter interface.
func (node *AlterChangefeedAddTarget) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD TARGET ")
	ctx.FormatNode(&node.Targets.Tables)
}

type AlterChangefeedDropTarget struct {
	Targets TargetList
}

// Format implements the NodeFormatter interface.
func (node *AlterChangefeedDropTarget) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP TARGET ")
	ctx.FormatNode(&node.Targets.Tables)
}
