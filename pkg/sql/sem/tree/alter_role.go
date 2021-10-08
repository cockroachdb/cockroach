// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// AlterRole represents an `ALTER ROLE ... WITH options` statement.
type AlterRole struct {
	Name      Expr
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
	ctx.FormatNode(node.Name)

	if len(node.KVOptions) > 0 {
		ctx.WriteString(" WITH")
		node.KVOptions.formatAsRoleOptions(ctx)
	}
}

// AlterRoleSet represents an `ALTER ROLE ... SET` statement.
type AlterRoleSet struct {
	RoleName     Expr
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
		ctx.FormatNode(node.RoleName)
		ctx.WriteString(" ")
	}
	if node.DatabaseName != "" {
		ctx.WriteString("IN DATABASE ")
		ctx.FormatNode(&node.DatabaseName)
		ctx.WriteString(" ")
	}
	ctx.FormatNode(node.SetOrReset)
}
