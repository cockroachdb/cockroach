// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// AlterSchema represents an ALTER SCHEMA statement.
type AlterSchema struct {
	Schema ObjectNamePrefix
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
	Owner RoleSpec
}

// Format implements the NodeFormatter interface.
func (node *AlterSchemaOwner) Format(ctx *FmtCtx) {
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&node.Owner)
}
