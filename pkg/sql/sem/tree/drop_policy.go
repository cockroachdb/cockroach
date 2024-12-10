// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

var _ Statement = &DropPolicy{}

// DropPolicy is a tree struct for the DROP POLICY DDL statement
type DropPolicy struct {
	Policy       Name
	Table        *UnresolvedObjectName
	DropBehavior DropBehavior
	IfExists     bool
}

// Format implements the NodeFormatter interface.
func (node *DropPolicy) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP POLICY ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatName(string(node.Policy))
	ctx.WriteString(" ON ")
	ctx.FormatNode(node.Table)
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}
