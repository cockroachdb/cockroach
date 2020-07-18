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

import "github.com/cockroachdb/cockroach/pkg/sql/lex"

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
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, *n.Comment, ctx.flags.EncodeFlags())
	} else {
		ctx.WriteString("NULL")
	}
}
