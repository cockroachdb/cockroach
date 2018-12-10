// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/lex"

// CommentOnTable represents an COMMENT ON TABLE statement.
type CommentOnTable struct {
	Table   TableName
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnTable) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON TABLE ")
	ctx.FormatNode(&n.Table)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		lex.EncodeSQLStringWithFlags(ctx.Buffer, *n.Comment, ctx.flags.EncodeFlags())
	} else {
		ctx.WriteString("NULL")
	}
}
