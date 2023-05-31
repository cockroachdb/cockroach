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

// Batch represents a BATCH clause.
type Batch struct {
	// HasSize is true if a size was specified via BATCH SIZE <size>.
	// This is used to distinguish between
	// BATCH DELETE - valid
	// BATCH SIZE 0 DELETE - invalid
	HasSize bool
	// Size represents a size specified by BATCH SIZE <size>.
	// It must be positive.
	Size int64
}

// Format implements the NodeFormatter interface.
func (b *Batch) Format(ctx *FmtCtx) {
	if b == nil {
		return
	}
	ctx.WriteString("BATCH ")
	if b.HasSize {
		ctx.WriteString("SIZE ")
		ctx.WriteString(strconv.FormatInt(b.Size, 10))
		ctx.WriteString(" ")
	}
}
