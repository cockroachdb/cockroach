// Copyright 2023 The Cockroach Authors.
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

// Batch represents a BATCH statement.
type Batch struct {
	Size int64
}

// HasSize returns true if the Batch has a specified size.
func (b *Batch) HasSize() bool {
	return b.Size > 0
}

// Format implements the NodeFormatter interface.
func (b *Batch) Format(ctx *FmtCtx) {
	if b == nil {
		return
	}
	ctx.WriteString("BATCH ")
	if b.HasSize() {
		ctx.WriteString("SIZE ")
		ctx.WriteString(strconv.FormatInt(b.Size, 10))
		ctx.WriteString(" ")
	}
}
