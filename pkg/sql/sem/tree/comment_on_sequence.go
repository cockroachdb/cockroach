// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/lexbase"

// CommentOnSequence represents a COMMENT ON SEQUENCE statement.
type CommentOnSequence struct {
	Sequence *UnresolvedObjectName
	Comment  *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON SEQUENCE ")
	ctx.FormatNode(n.Sequence)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(knz): Replace all this with ctx.FormatNode
		// when COMMENT supports expressions.
		if ctx.flags.HasFlags(FmtHideConstants) {
			ctx.WriteString("'_'")
		} else {
			lexbase.EncodeSQLStringWithFlags(&ctx.Buffer, *n.Comment, ctx.flags.EncodeFlags())
		}
	} else {
		ctx.WriteString("NULL")
	}
}
