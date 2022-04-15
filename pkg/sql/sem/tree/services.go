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

// CreateService represents a CREATE SERVICE statement.
type CreateService struct {
	Service        Name
	IfNotExists    bool
	DefaultOptions KVOptions
	Rules          *Select
}

var _ Statement = (*CreateService)(nil)

// Format implements the NodeFormatter interface.
func (node *CreateService) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SERVICE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Service)
	if len(node.DefaultOptions) > 0 {
		ctx.WriteString(" WITH DEFAULT OPTIONS (")
		ctx.FormatNode(&node.DefaultOptions)
		ctx.WriteByte(')')
	}
	if node.Rules != nil {
		ctx.WriteString(" USING RULES FROM ")
		ctx.FormatNode(node.Rules)
	}
}

// AlterService represents an ALTER SERVICE statement.
type AlterService struct {
	Service  Name
	IfExists bool
	Cmd      AlterServiceCmd
}

var _ Statement = (*AlterService)(nil)

// Format implements the NodeFormatter interface.
func (node *AlterService) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER SERVICE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Service)
	ctx.FormatNode(node.Cmd)
}

// AlterServiceCmd is the type of one of the ALTER SERVICE statement
// commands.
type AlterServiceCmd interface {
	NodeFormatter
	alterServiceCmd()
}

func (*AlterServiceOwner) alterServiceCmd()       {}
func (*AlterServiceRefresh) alterServiceCmd()     {}
func (*AlterServiceRename) alterServiceCmd()      {}
func (*AlterServiceSetDefaults) alterServiceCmd() {}
func (*AlterServiceRules) alterServiceCmd()       {}

// AlterServiceOwner represents an ALTER SERVICE OWNER TO statement.
type AlterServiceOwner struct {
	Owner RoleSpec
}

// Format implements the NodeFormatter interface.
func (node *AlterServiceOwner) Format(ctx *FmtCtx) {
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&node.Owner)
}

// AlterServiceRename represents an ALTER SERVICE RENAME TO statement.
type AlterServiceRename struct {
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterServiceRename) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterServiceSetDefaults represents an ALTER SERVICE SET DEFAULT OPTIONS statement.
type AlterServiceSetDefaults struct {
	Opts KVOptions
}

// Format implements the NodeFormatter interface.
func (node *AlterServiceSetDefaults) Format(ctx *FmtCtx) {
	if len(node.Opts) == 0 {
		ctx.WriteString(" DROP DEFAULT OPTIONS")
	} else {
		ctx.WriteString(" SET DEFAULT OPTIONS (")
		ctx.FormatNode(&node.Opts)
		ctx.WriteByte(')')
	}
}

// AlterServiceRules represents an ALTER SERVICE USING RULES FROM statement.
type AlterServiceRules struct {
	Rules *Select
}

// Format implements the NodeFormatter interface.
func (node *AlterServiceRules) Format(ctx *FmtCtx) {
	if node.Rules == nil {
		ctx.WriteString(" DROP RULES")
	} else {
		ctx.WriteString(" USING RULES FROM ")
		ctx.FormatNode(node.Rules)
	}
}

// AlterServiceRefresh represents an ALTER SERVICE REFRESHstatement.
type AlterServiceRefresh struct{}

// Format implements the NodeFormatter interface.
func (node *AlterServiceRefresh) Format(ctx *FmtCtx) {
	ctx.WriteString(" REFRESH")
}

// DropService represents a DROP SERVICE statement.
type DropService struct {
	Names    NameList
	IfExists bool
}

var _ Statement = (*DropService)(nil)

// Format implements the NodeFormatter interface.
func (node *DropService) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP SERVICE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
}
