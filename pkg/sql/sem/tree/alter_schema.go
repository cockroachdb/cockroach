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

// AlterSchema represents an ALTER SCHEMA statement.
type AlterSchema struct {
	Schema Name
	Cmd    AlterSchemaCmd
}

var _ Statement = &AlterSchema{}

// Format implements the NodeFormatter interface.
func (node *AlterSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER SCHEMA ")
	ctx.FormatNode(&node.Schema)
	ctx.FormatNode(node.Cmd)
}

// AlterSchemaCmd represents a schema modification operation.
type AlterSchemaCmd interface {
	NodeFormatter
	alterSchemaCmd()
}

func (*AlterSchemaRename) alterSchemaCmd() {}

// AlterSchemaRename represents an ALTER SCHEMA RENAME command.
type AlterSchemaRename struct {
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterSchemaRename) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

func (*AlterSchemaOwner) alterSchemaCmd() {}

// AlterSchemaOwner represents an ALTER SCHEMA OWNER TO command.
type AlterSchemaOwner struct {
	Owner Name
}

// Format implements the NodeFormatter interface.
func (node *AlterSchemaOwner) Format(ctx *FmtCtx) {
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&node.Owner)
}
