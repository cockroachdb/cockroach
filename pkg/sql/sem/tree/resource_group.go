// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// CreateResourceGroup represents a CREATE RESOURCE GROUP statement.
type CreateResourceGroup struct {
	IfNotExists bool
	Name        Name
	Options     KVOptions
}

var _ Statement = &CreateResourceGroup{}

// Format implements the NodeFormatter interface.
func (node *CreateResourceGroup) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE RESOURCE GROUP ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" WITH (")
	node.Options.Format(ctx)
	ctx.WriteByte(')')
}

// AlterResourceGroup represents an ALTER RESOURCE GROUP statement.
type AlterResourceGroup struct {
	IfExists bool
	Name     Name
	Options  KVOptions
}

var _ Statement = &AlterResourceGroup{}

// Format implements the NodeFormatter interface.
func (node *AlterResourceGroup) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER RESOURCE GROUP ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" WITH (")
	node.Options.Format(ctx)
	ctx.WriteByte(')')
}

// DropResourceGroup represents a DROP RESOURCE GROUP statement.
type DropResourceGroup struct {
	IfExists bool
	Name     Name
}

var _ Statement = &DropResourceGroup{}

// Format implements the NodeFormatter interface.
func (node *DropResourceGroup) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP RESOURCE GROUP ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Name)
}

// ShowResourceGroup represents a SHOW RESOURCE GROUP <name> statement.
type ShowResourceGroup struct {
	Name Name
}

var _ Statement = &ShowResourceGroup{}

// Format implements the NodeFormatter interface.
func (node *ShowResourceGroup) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW RESOURCE GROUP ")
	ctx.FormatNode(&node.Name)
}

// ShowResourceGroups represents a SHOW RESOURCE GROUPS statement.
type ShowResourceGroups struct{}

var _ Statement = &ShowResourceGroups{}

// Format implements the NodeFormatter interface.
func (node *ShowResourceGroups) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW RESOURCE GROUPS")
}
