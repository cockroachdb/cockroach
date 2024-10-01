// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// ReassignOwnedBy represents a REASSIGN OWNED BY <name> TO <name> statement.
type ReassignOwnedBy struct {
	OldRoles RoleSpecList
	NewRole  RoleSpec
}

var _ Statement = &ReassignOwnedBy{}

// Format implements the NodeFormatter interface.
func (node *ReassignOwnedBy) Format(ctx *FmtCtx) {
	ctx.WriteString("REASSIGN OWNED BY ")
	for i := range node.OldRoles {
		if i > 0 {
			ctx.WriteString(", ")
		}
		node.OldRoles[i].Format(ctx)
	}
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewRole)
}
