package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

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
