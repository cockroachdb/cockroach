// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

var _ Statement = &AlterPolicy{}

// AlterPolicy is a tree struct for the ALTER POLICY DDL statement
type AlterPolicy struct {
	PolicyName    Name
	TableName     *UnresolvedObjectName
	NewPolicyName Name
	Roles         RoleSpecList
	Exprs         PolicyExpressions
}

// Format implements the NodeFormatter interface.
func (node *AlterPolicy) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER POLICY ")
	ctx.FormatNode(&node.PolicyName)
	ctx.WriteString(" ON ")
	ctx.FormatNode(node.TableName)

	if node.NewPolicyName != "" {
		ctx.WriteString(" RENAME TO ")
		ctx.FormatNode(&node.NewPolicyName)
		return
	}

	if len(node.Roles) > 0 {
		ctx.WriteString(" TO ")
		ctx.FormatNode(&node.Roles)
	}
	ctx.FormatNode(&node.Exprs)
}
