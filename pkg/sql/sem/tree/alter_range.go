// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "strconv"

// RelocateRange represents an `ALTER RANGE .. RELOCATE ..`
// statement.
type RelocateRange struct {
	Rows              *Select
	ToStoreID         int64
	FromStoreID       int64
	RelocateLease     bool
	RelocateNonVoters bool
}

// Format implements the NodeFormatter interface.
func (node *RelocateRange) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER RANGE RELOCATE ")
	if node.RelocateLease {
		ctx.WriteString("LEASE ")
	} else if node.RelocateNonVoters {
		ctx.WriteString("NON_VOTERS ")
	} else {
		ctx.WriteString("VOTERS ")
	}
	if !node.RelocateLease {
		ctx.WriteString("FROM ")
		ctx.WriteString(strconv.FormatInt(node.FromStoreID, 10))
		ctx.WriteString(" ")
	}
	ctx.WriteString("TO ")
	ctx.WriteString(strconv.FormatInt(node.ToStoreID, 10))
	ctx.WriteString(" FOR ")
	ctx.FormatNode(node.Rows)
}
