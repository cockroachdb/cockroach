// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// CreateVectorizer represents a CREATE VECTORIZER statement.
//
// CREATE VECTORIZER ON <table>
//
//	USING COLUMN (col1, col2, ...)
//	[WITH key = value, ...]
var _ Statement = &CreateVectorizer{}

// CreateVectorizer creates an automatic embedding generator on a table.
type CreateVectorizer struct {
	TableName *UnresolvedObjectName
	Columns   NameList
	Options   KVOptions
}

// Format implements the NodeFormatter interface.
func (node *CreateVectorizer) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE VECTORIZER ON ")
	ctx.FormatNode(node.TableName)
	ctx.WriteString(" USING COLUMN (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if len(node.Options) > 0 {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// DropVectorizer represents a DROP VECTORIZER statement.
//
// DROP VECTORIZER [IF EXISTS] ON <table>
var _ Statement = &DropVectorizer{}

// DropVectorizer removes the automatic embedding generator from a table.
type DropVectorizer struct {
	TableName *UnresolvedObjectName
	IfExists  bool
}

// Format implements the NodeFormatter interface.
func (node *DropVectorizer) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP VECTORIZER ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.WriteString("ON ")
	ctx.FormatNode(node.TableName)
}
