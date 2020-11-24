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

import "github.com/cockroachdb/cockroach/pkg/security"

// DropOwnedBy represents a DROP OWNED BY command.
type DropOwnedBy struct {
	Roles        []security.SQLUsername
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
		ctx.FormatUsername(node.Roles[i])
	}
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}
