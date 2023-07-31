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

// AlterTenantSetClusterSetting represents an ALTER TENANT
// SET CLUSTER SETTING statement.
type AlterTenantSetClusterSetting struct {
	SetClusterSetting
	TenantID  Expr
	TenantAll bool
}

// Format implements the NodeFormatter interface.
func (n *AlterTenantSetClusterSetting) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	if n.TenantAll {
		ctx.WriteString("ALL")
	} else {
		_, canOmitParentheses := n.TenantID.(alreadyDelimitedAsSyntacticDExpr)
		if !canOmitParentheses {
			ctx.WriteByte('(')
		}
		ctx.FormatNode(n.TenantID)
		if !canOmitParentheses {
			ctx.WriteByte(')')
		}
	}
	ctx.WriteByte(' ')
	ctx.FormatNode(&n.SetClusterSetting)
}

// ShowTenantClusterSetting represents a SHOW CLUSTER SETTING ... FOR TENANT statement.
type ShowTenantClusterSetting struct {
	*ShowClusterSetting
	TenantID Expr
}

// Format implements the NodeFormatter interface.
func (node *ShowTenantClusterSetting) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.ShowClusterSetting)
	ctx.WriteString(" FOR TENANT ")
	_, canOmitParentheses := node.TenantID.(alreadyDelimitedAsSyntacticDExpr)
	if !canOmitParentheses {
		ctx.WriteByte('(')
	}
	ctx.FormatNode(node.TenantID)
	if !canOmitParentheses {
		ctx.WriteByte(')')
	}
}

// ShowTenantClusterSettingList represents a SHOW CLUSTER SETTINGS FOR TENANT statement.
type ShowTenantClusterSettingList struct {
	*ShowClusterSettingList
	TenantID Expr
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
func (node *ShowTenantClusterSettingList) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.ShowClusterSettingList)
	ctx.WriteString(" FOR TENANT ")
	_, canOmitParentheses := node.TenantID.(alreadyDelimitedAsSyntacticDExpr)
	if !canOmitParentheses {
		ctx.WriteByte('(')
	}
	ctx.FormatNode(node.TenantID)
	if !canOmitParentheses {
		ctx.WriteByte(')')
	}
}
