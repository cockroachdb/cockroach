// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// ReassignOwnedBy represents a REASSIGN OWNED BY <name> TO <name> statement.
type ReassignOwnedBy struct {
	OldRoles []string
	NewRole  string
}

// Format implements the NodeFormatter interface.
func (node *ReassignOwnedBy) Format(ctx *FmtCtx) {
	ctx.WriteString("REASSIGN OWNED BY ")
	for i := range node.OldRoles {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNameP(&node.OldRoles[i])
	}
	ctx.WriteString(" TO ")
	ctx.FormatNameP(&node.NewRole)
}
