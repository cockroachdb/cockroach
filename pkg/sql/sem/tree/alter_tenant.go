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

// ReplicationCutoverTime represent the user-specified cutover time
type ReplicationCutoverTime struct {
	Timestamp Expr
	Latest    bool
}

// AlterTenantReplication represents an ALTER TENANT REPLICATION statement.
type AlterTenantReplication struct {
	TenantSpec *TenantSpec
	Command    JobCommand
	Cutover    *ReplicationCutoverTime
	Options    TenantReplicationOptions
}

var _ Statement = &AlterTenantReplication{}

// Format implements the NodeFormatter interface.
func (n *AlterTenantReplication) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	ctx.FormatNode(n.TenantSpec)
	ctx.WriteByte(' ')
	if n.Cutover != nil {
		ctx.WriteString("COMPLETE REPLICATION TO ")
		if n.Cutover.Latest {
			ctx.WriteString("LATEST")
		} else {
			ctx.WriteString("SYSTEM TIME ")
			ctx.FormatNode(n.Cutover.Timestamp)
		}
	} else if !n.Options.IsDefault() {
		ctx.WriteString("SET REPLICATION ")
		ctx.FormatNode(&n.Options)
	} else if n.Command == PauseJob || n.Command == ResumeJob {
		ctx.WriteString(JobCommandToStatement[n.Command])
		ctx.WriteString(" REPLICATION")
	}
}

// TenantSpec designates a tenant for the ALTER TENANT statements.
type TenantSpec struct {
	Expr   Expr
	IsName bool
	All    bool
}

type alreadyDelimitedAsSyntacticDExpr interface {
	Expr
	alreadyDelimitedAsSyntacticDExpr()
}

func (*UnresolvedName) alreadyDelimitedAsSyntacticDExpr() {}
func (*ParenExpr) alreadyDelimitedAsSyntacticDExpr()      {}
func (*Subquery) alreadyDelimitedAsSyntacticDExpr()       {}
func (*Placeholder) alreadyDelimitedAsSyntacticDExpr()    {}
func (*NumVal) alreadyDelimitedAsSyntacticDExpr()         {}
func (*StrVal) alreadyDelimitedAsSyntacticDExpr()         {}
func (dNull) alreadyDelimitedAsSyntacticDExpr()           {}
func (*FuncExpr) alreadyDelimitedAsSyntacticDExpr()       {}

// Format implements the NodeFormatter interface.
func (n *TenantSpec) Format(ctx *FmtCtx) {
	if n.All {
		ctx.WriteString("ALL")
	} else if n.IsName {
		// Beware to enclose the expression within parentheses if it is
		// not a simple identifier and is not already enclosed in
		// parentheses.
		_, canOmitParentheses := n.Expr.(alreadyDelimitedAsSyntacticDExpr)
		if !canOmitParentheses {
			ctx.WriteByte('(')
		}
		ctx.FormatNode(n.Expr)
		if !canOmitParentheses {
			ctx.WriteByte(')')
		}
	} else {
		ctx.WriteByte('[')
		ctx.FormatNode(n.Expr)
		ctx.WriteByte(']')
	}
}

// AlterTenantRename represents an ALTER TENANT RENAME statement.
type AlterTenantRename struct {
	TenantSpec *TenantSpec
	NewName    Expr
}

var _ Statement = &AlterTenantRename{}

// Format implements the NodeFormatter interface.
func (n *AlterTenantRename) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	ctx.FormatNode(n.TenantSpec)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(n.NewName)
}
