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

// TenantCapability is a key-value parameter representing a tenant capability.
type TenantCapability struct {
	Name  string
	Value Expr
}

// AlterTenantCapability represents an ALTER TENANT CAPABILITY statement.
type AlterTenantCapability struct {
	TenantSpec   *TenantSpec
	Capabilities []TenantCapability
	IsRevoke     bool
}

var _ Statement = &AlterTenantCapability{}

// Format implements the NodeFormatter interface.
func (n *AlterTenantCapability) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	ctx.FormatNode(n.TenantSpec)
	if n.IsRevoke {
		ctx.WriteString(" REVOKE CAPABILITY ")
	} else {
		ctx.WriteString(" GRANT CAPABILITY ")
	}
	for i, capability := range n.Capabilities {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.WriteString(capability.Name)
		value := capability.Value
		if value != nil {
			ctx.WriteString(" = ")
			ctx.FormatNode(value)
		}
	}
}

// TenantSpec designates a tenant for the ALTER TENANT statements.
type TenantSpec struct {
	Expr   Expr
	IsName bool
	All    bool
}

// alreadyDelimitedAsSyntacticDExpr is an interface that marks
// Expr types for which there is never an ambiguity when
// the expression syntax is followed by a non-reserved
// keyword. When this property is true, that expression
// can be pretty-printed without enclosing parentheses in
// a context followed by more non-reserved keywords, and
// result in syntax that is still unambiguous.
// That is, given an expression E and an arbitrary following
// word X, the syntax "E X" is always unambiguously parsed
// as "(E) X".
//
// This property is obviously true of "atomic" expressions such as
// string and number literals, and also obviously true of
// well-enclosed expressions "(...)" / "[...]". However, it is not
// always true of other composite expression types. For example,
// "A::B" (CastExpr) is not well-delimited because there are
// identifiers/keywords such that "A::B C" can be parsed as "A::(B
// C)". Consider "'a'::INTERVAL" and the non-reserved keyword
// "MINUTE".
//
// This property is closely related to the d_expr syntactic rule in
// the grammar, hence its name. *Approximately* the expression types
// produced by the d_expr rule tend to exhibit the "well-delimited"
// property. However, this is not a proper equivalence: certain Expr
// types are _also_ produced by other parsing rules than d_expr, so
// inspection of the contents of the Expr object is necessary to
// determine whether it is well-delimited or not (for example, some
// FuncExpr objects are well-delimited, and others are not).
// Therefore, it is not generally correct to assign the property to
// all the d_expr expression *types*. We can only do so for a few
// types for which we know that *all possible objects* of that type
// are well-delimited, such as Subquery, NumVal or Placeholder.
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

// AlterTenantService represents an ALTER TENANT START/STOP SERVICE statement.
type AlterTenantService struct {
	TenantSpec *TenantSpec
	Command    TenantServiceCmd
}

// TenantServiceCmd represents a parameter to ALTER TENANT.
type TenantServiceCmd int8

const (
	// TenantStartServiceExternal encodes START SERVICE EXTERNAL.
	TenantStartServiceExternal TenantServiceCmd = 0
	// TenantStartServiceExternal encodes START SERVICE SHARED.
	TenantStartServiceShared TenantServiceCmd = 1
	// TenantStartServiceExternal encodes STOP SERVICE.
	TenantStopService TenantServiceCmd = 2
)

var _ Statement = &AlterTenantService{}

// Format implements the NodeFormatter interface.
func (n *AlterTenantService) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	ctx.FormatNode(n.TenantSpec)
	switch n.Command {
	case TenantStartServiceExternal:
		ctx.WriteString(" START SERVICE EXTERNAL")
	case TenantStartServiceShared:
		ctx.WriteString(" START SERVICE SHARED")
	case TenantStopService:
		ctx.WriteString(" STOP SERVICE")
	}
}
