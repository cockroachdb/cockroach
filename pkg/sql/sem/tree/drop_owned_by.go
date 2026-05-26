// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// DropOwnedBy represents a DROP OWNED BY command.
type DropOwnedBy struct {
	Roles        RoleSpecList
	DropBehavior DropBehavior
}

var _ Statement = &DropOwnedBy{}

// Format implements the NodeFormatter interface.
func (node *DropOwnedBy) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP OWNED BY ")
	for i := range node.Roles {
		if i > 0 {
			ctx.WriteString(", ")
		}
		node.Roles[i].Format(ctx)
	}
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}
