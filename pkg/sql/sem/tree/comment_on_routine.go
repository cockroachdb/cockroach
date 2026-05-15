// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/lexbase"

// CommentOnRoutine represents a COMMENT ON FUNCTION, COMMENT ON PROCEDURE,
// or COMMENT ON ROUTINE statement. The keyword the user wrote is captured
// by RoutineType: a single bit (UDFRoutine or ProcedureRoutine) corresponds
// to FUNCTION or PROCEDURE; both bits set corresponds to ROUTINE, which
// matches either kind.
type CommentOnRoutine struct {
	Routine     RoutineObj
	RoutineType RoutineType
	Comment     *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnRoutine) Format(ctx *FmtCtx) {
	switch n.RoutineType {
	case ProcedureRoutine:
		ctx.WriteString("COMMENT ON PROCEDURE ")
	case UDFRoutine | ProcedureRoutine:
		ctx.WriteString("COMMENT ON ROUTINE ")
	default:
		ctx.WriteString("COMMENT ON FUNCTION ")
	}
	ctx.FormatNode(&n.Routine)
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
