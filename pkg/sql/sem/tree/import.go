// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// ImportOptionSchemaURI is the IMPORT option that specifies an external
// storage URI to read an Avro schema from. It is defined here rather than in
// sql/importer, which owns IMPORT option parsing, because Import.Format must
// redact its value and cannot depend on that package.
const ImportOptionSchemaURI = "schema_uri"

// Import represents a IMPORT statement.
type Import struct {
	Table      *TableName
	IntoCols   NameList
	FileFormat string
	Files      Exprs
	Options    KVOptions
}

var _ Statement = &Import{}

// Format implements the NodeFormatter interface.
func (node *Import) Format(ctx *FmtCtx) {
	ctx.WriteString("IMPORT ")

	ctx.WriteString("INTO ")
	ctx.FormatNode(node.Table)
	if node.IntoCols != nil {
		ctx.WriteByte('(')
		ctx.FormatNode(&node.IntoCols)
		ctx.WriteString(") ")
	} else {
		ctx.WriteString(" ")
	}
	ctx.WriteString(node.FileFormat)
	ctx.WriteString(" DATA ")
	if len(node.Files) == 1 {
		ctx.WriteString("(")
	}
	ctx.FormatURIs(node.Files)
	if len(node.Files) == 1 {
		ctx.WriteString(")")
	}

	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS (")
		node.Options.formatEach(ctx, func(n *KVOption, ctx *FmtCtx) {
			// The schema_uri option is an external storage URI.
			if string(n.Key) == ImportOptionSchemaURI {
				ctx.FormatURI(n.Value)
			} else {
				ctx.FormatNode(n.Value)
			}
		})
		ctx.WriteString(")")
	}
}
