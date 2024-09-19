// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// AlterRole represents an `ALTER ROLE ... WITH options` statement.
type AlterRole struct {
	Name      RoleSpec
	IfExists  bool
	IsRole    bool
	KVOptions KVOptions
}

// Format implements the NodeFormatter interface.
func (node *AlterRole) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER")
	if node.IsRole {
		ctx.WriteString(" ROLE ")
	} else {
		ctx.WriteString(" USER ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Name)

	if len(node.KVOptions) > 0 {
		ctx.WriteString(" WITH")
		node.KVOptions.formatAsRoleOptions(ctx)
	}
}

// AlterRoleSet represents an `ALTER ROLE ... SET` statement.
type AlterRoleSet struct {
	RoleName     RoleSpec
	IfExists     bool
	IsRole       bool
	AllRoles     bool
	DatabaseName Name
	SetOrReset   *SetVar
}

// Format implements the NodeFormatter interface.
func (node *AlterRoleSet) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER")
	if node.IsRole {
		ctx.WriteString(" ROLE ")
	} else {
		ctx.WriteString(" USER ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	if node.AllRoles {
		ctx.WriteString("ALL ")
	} else {
		ctx.FormatNode(&node.RoleName)
		ctx.WriteString(" ")
	}
	if node.DatabaseName != "" {
		ctx.WriteString("IN DATABASE ")
		ctx.FormatNode(&node.DatabaseName)
		ctx.WriteString(" ")
	}
	ctx.FormatNode(node.SetOrReset)
}
