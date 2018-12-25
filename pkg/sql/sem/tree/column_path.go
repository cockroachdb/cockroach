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

// ColumnPath corresponds to the path of a column in statements.
//
// ColumnPath is the public type for column path. It exposes the fields
// and can be default-constructed but cannot be instantiated with a
// non-default value; this encourages the use of the constructors below.
type ColumnPath struct {
	columnPath
}

type columnPath struct {
	Column Name

	ColumnPathPrefix
}

// ColumnPathPrefix corresponds to the path prefix of a column
type ColumnPathPrefix struct {
	CatalogName Name
	SchemaName  Name
	TableName   Name

	// ExplicitCatalog is true if the catalog was explicitly specified
	// or it needs to be rendered during pretty-printing.
	ExplicitCatalog bool
	// ExplicitSchema is true if the schema was explicitly specified
	// or it needs to be rendered during pretty-printing.
	ExplicitSchema bool
	// ExplicitTable is true if the table was explicitly specified
	// or it needs to be rendered during pretty-printing.
	ExplicitTable bool
}

// MakeTableName returns TableName of column.
func (c *ColumnPath) MakeTableName() TableName {
	return TableName{tblName{
		TableName: c.ColumnPathPrefix.TableName,
		TableNamePrefix: TableNamePrefix{
			CatalogName:     c.ColumnPathPrefix.CatalogName,
			ExplicitCatalog: c.ColumnPathPrefix.ExplicitCatalog,
			SchemaName:      c.ColumnPathPrefix.SchemaName,
			ExplicitSchema:  c.ColumnPathPrefix.ExplicitSchema,
		},
	}}
}

// Format implements the NodeFormatter interface.
func (c *ColumnPath) Format(ctx *FmtCtx) {
	c.ColumnPathPrefix.Format(ctx)
	if c.ExplicitTable || ctx.alwaysFormatTablePrefix() {
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.Column)
}

func (c *ColumnPath) String() string { return AsString(c) }

func makeColumnPathFromUnresolvedName(n *UnresolvedName) ColumnPath {
	return ColumnPath{columnPath{
		Column:           Name(n.Parts[0]),
		ColumnPathPrefix: makeColumnPathPrefixFromUnresolvedName(n),
	}}
}

func makeColumnPathPrefixFromUnresolvedName(n *UnresolvedName) ColumnPathPrefix {
	return ColumnPathPrefix{
		TableName:       Name(n.Parts[1]),
		SchemaName:      Name(n.Parts[2]),
		CatalogName:     Name(n.Parts[3]),
		ExplicitTable:   n.NumParts >= 2,
		ExplicitSchema:  n.NumParts >= 3,
		ExplicitCatalog: n.NumParts >= 4,
	}
}

// Format implements the NodeFormatter interface.
func (cp *ColumnPathPrefix) Format(ctx *FmtCtx) {
	alwaysFormat := ctx.alwaysFormatTablePrefix()
	if cp.ExplicitTable || alwaysFormat {
		if cp.ExplicitSchema || alwaysFormat {
			if cp.ExplicitCatalog || alwaysFormat {
				ctx.FormatNode(&cp.CatalogName)
				ctx.WriteByte('.')
			}
			ctx.FormatNode(&cp.SchemaName)
			ctx.WriteByte('.')
		}
		ctx.FormatNode(&cp.TableName)
	}
}

func (cp *ColumnPathPrefix) String() string { return AsString(cp) }
