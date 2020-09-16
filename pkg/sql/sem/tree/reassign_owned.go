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

// ReassignOwned represents a REASSIGN OWNED BY <name> TO <name> statement.
type ReassignOwned struct {
	OldRoles NameList
	NewRole  Name
}

// Format implements the NodeFormatter interface.
func (node *ReassignOwned) Format(ctx *FmtCtx) {
	ctx.WriteString("REASSIGN OWNED BY ")
	ctx.FormatNode(&node.OldRoles)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewRole)
}
