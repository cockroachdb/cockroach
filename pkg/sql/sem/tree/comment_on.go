// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/lexbase"

// CommentOnColumn represents an COMMENT ON COLUMN statement.
type CommentOnColumn struct {
	*ColumnItem
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnColumn) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON COLUMN ")
	ctx.FormatNode(n.ColumnItem)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(rafi): Replace all this with ctx.FormatNode
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

// CommentOnConstraint represents a COMMENT ON CONSTRAINT statement
type CommentOnConstraint struct {
	Constraint Name
	Table      *UnresolvedObjectName
	Comment    *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON CONSTRAINT ")
	ctx.FormatNode(&n.Constraint)
	ctx.WriteString(" ON ")
	ctx.FormatNode(n.Table)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(rafi): Replace all this with ctx.FormatNode
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

// CommentOnDatabase represents an COMMENT ON DATABASE statement.
type CommentOnDatabase struct {
	Name    Name
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON DATABASE ")
	ctx.FormatNode(&n.Name)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(rafi): Replace all this with ctx.FormatNode
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

// CommentOnIndex represents a COMMENT ON INDEX statement.
type CommentOnIndex struct {
	Index   TableIndexName
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON INDEX ")
	ctx.FormatNode(&n.Index)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(rafi): Replace all this with ctx.FormatNode
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
		// TODO(rafi): Replace all this with ctx.FormatNode
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

// CommentOnSchema represents an COMMENT ON SCHEMA statement.
type CommentOnSchema struct {
	Name    ObjectNamePrefix
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON SCHEMA ")
	ctx.FormatNode(&n.Name)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(rafi): Replace all this with ctx.FormatNode
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
		// TODO(rafi): Replace all this with ctx.FormatNode
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

// CommentOnTable represents an COMMENT ON TABLE statement.
type CommentOnTable struct {
	Table   *UnresolvedObjectName
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnTable) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON TABLE ")
	ctx.FormatNode(n.Table)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(rafi): Replace all this with ctx.FormatNode
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

// CommentOnType represents a COMMENT ON TYPE statement.
type CommentOnType struct {
	Name    *UnresolvedObjectName
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnType) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON TYPE ")
	ctx.FormatNode(n.Name)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(rafi): Replace all this with ctx.FormatNode
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

// CommentOnView represents a COMMENT ON VIEW statement.
type CommentOnView struct {
	View    *UnresolvedObjectName
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnView) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON VIEW ")
	ctx.FormatNode(n.View)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		// TODO(rafi): Replace all this with ctx.FormatNode
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
