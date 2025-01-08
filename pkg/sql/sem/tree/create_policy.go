// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

var _ Statement = &CreatePolicy{}

// PolicyCommand represents the type of commands a row-level security policy
// will apply against.
type PolicyCommand int

// PolicyCommand values
const (
	PolicyCommandDefault PolicyCommand = iota
	PolicyCommandAll
	PolicyCommandSelect
	PolicyCommandInsert
	PolicyCommandUpdate
	PolicyCommandDelete
)

var policyCommandName = [...]string{
	PolicyCommandDefault: "",
	PolicyCommandAll:     "ALL",
	PolicyCommandSelect:  "SELECT",
	PolicyCommandInsert:  "INSERT",
	PolicyCommandUpdate:  "UPDATE",
	PolicyCommandDelete:  "DELETE",
}

func (p PolicyCommand) String() string { return policyCommandName[p] }

// SafeValue implements the redact.SafeValue interface.
func (p PolicyCommand) SafeValue() {}

// PolicyType represents the type of the row-level security policy.
type PolicyType int

// PolicyType values
const (
	PolicyTypeDefault PolicyType = iota
	PolicyTypePermissive
	PolicyTypeRestrictive
)

var policyTypeName = [...]string{
	PolicyTypeDefault:     "",
	PolicyTypePermissive:  "PERMISSIVE",
	PolicyTypeRestrictive: "RESTRICTIVE",
}

func (p PolicyType) String() string { return policyTypeName[p] }

// SafeValue implements the redact.SafeValue interface.
func (p PolicyType) SafeValue() {}

type PolicyExpressions struct {
	Using     Expr
	WithCheck Expr
}

// Format implements the NodeFormatter interface
func (node *PolicyExpressions) Format(ctx *FmtCtx) {
	if node.Using != nil {
		ctx.WriteString(" USING (")
		ctx.FormatNode(node.Using)
		ctx.WriteString(")")
	}

	if node.WithCheck != nil {
		ctx.WriteString(" WITH CHECK (")
		ctx.FormatNode(node.WithCheck)
		ctx.WriteString(")")
	}
}

// CreatePolicy is a tree struct for the CREATE POLICY DDL statement
type CreatePolicy struct {
	PolicyName Name
	TableName  *UnresolvedObjectName
	Type       PolicyType
	Cmd        PolicyCommand
	Roles      RoleSpecList
	Exprs      PolicyExpressions
}

// Format implements the NodeFormatter interface.
func (node *CreatePolicy) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE POLICY ")
	ctx.FormatNode(&node.PolicyName)
	ctx.WriteString(" ON ")
	ctx.FormatNode(node.TableName)
	if node.Type != PolicyTypeDefault {
		ctx.WriteString(" AS ")
		ctx.WriteString(node.Type.String())
	}
	if node.Cmd != PolicyCommandDefault {
		ctx.WriteString(" FOR ")
		ctx.WriteString(node.Cmd.String())
	}

	if len(node.Roles) > 0 {
		ctx.WriteString(" TO ")
		ctx.FormatNode(&node.Roles)
	}
	ctx.FormatNode(&node.Exprs)
}
