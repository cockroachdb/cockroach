// Copyright 2016 The Cockroach Authors.
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

// TableName corresponds to the name of a table in a FROM clause,
// INSERT or UPDATE statement, etc.
//
// This is constructed for incoming SQL queries by normalizing an
// UnresolvedName using NormalizeTableName.
//
// Internal uses of this struct should not construct instances of
// TableName directly, and instead use the NewTableName /
// MakeTableName functions underneath.
//
// TableName is the public type for tblName. It exposes the fields
// and can be default-constructed but cannot be instantiated with a
// non-default value; this encourages the use of the constructors below.
type TableName struct {
	tblName
}

type tblName struct {
	// TableName is the unqualified name for the object
	// (table/view/sequence/function/type).
	TableName Name

	// TableNamePrefix is the path to the object.  This can be modified
	// further by name resolution, see name_resolution.go.
	TableNamePrefix
}

// TableNamePrefix corresponds to the path prefix of a table name.
type TableNamePrefix struct {
	CatalogName Name
	SchemaName  Name

	// ExplicitCatalog is true iff the catalog was explicitly specified
	// or it needs to be rendered during pretty-printing.
	ExplicitCatalog bool
	// ExplicitSchema is true iff the schema was explicitly specified
	// or it needs to be rendered during pretty-printing.
	ExplicitSchema bool
}

// Format implements the NodeFormatter interface.
func (tp *TableNamePrefix) Format(ctx *FmtCtx) {
	alwaysFormat := ctx.alwaysFormatTablePrefix()
	if tp.ExplicitSchema || alwaysFormat {
		if tp.ExplicitCatalog || alwaysFormat {
			ctx.FormatNode(&tp.CatalogName)
			ctx.WriteByte('.')
		}
		ctx.FormatNode(&tp.SchemaName)
	}
}

func (tp *TableNamePrefix) String() string { return AsString(tp) }

// Schema retrieves the unqualified schema name.
func (tp *TableNamePrefix) Schema() string {
	return string(tp.SchemaName)
}

// Catalog retrieves the unqualified catalog name.
func (tp *TableNamePrefix) Catalog() string {
	return string(tp.CatalogName)
}

// Format implements the NodeFormatter interface.
func (t *TableName) Format(ctx *FmtCtx) {
	if ctx.tableNameFormatter != nil {
		ctx.tableNameFormatter(ctx, t)
		return
	}
	t.TableNamePrefix.Format(ctx)
	if t.ExplicitSchema || ctx.alwaysFormatTablePrefix() {
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&t.TableName)
}
func (t *TableName) String() string { return AsString(t) }

// FQString renders the table name in full, not omitting the prefix
// schema and catalog names. Suitable for logging, etc.
func (t *TableName) FQString() string {
	return AsStringWithFlags(t, FmtAlwaysQualifyTableNames)
}

// Table retrieves the unqualified table name.
func (t *TableName) Table() string {
	return string(t.TableName)
}

// tableExpr implements the TableExpr interface.
func (*TableName) tableExpr() {}

// MakeTableName creates a new table name qualified with just a schema.
func MakeTableName(db, tbl Name) TableName {
	return TableName{tblName{
		TableName: tbl,
		TableNamePrefix: TableNamePrefix{
			CatalogName:     db,
			SchemaName:      PublicSchemaName,
			ExplicitSchema:  true,
			ExplicitCatalog: true,
		},
	}}
}

// NewTableName creates a new table name qualified with a given
// catalog and the public schema.
func NewTableName(db, tbl Name) *TableName {
	tn := MakeTableName(db, tbl)
	return &tn
}

// MakeTableNameWithSchema creates a new fully qualified table name.
func MakeTableNameWithSchema(db, schema, tbl Name) TableName {
	return TableName{tblName{
		TableName: tbl,
		TableNamePrefix: TableNamePrefix{
			CatalogName:     db,
			SchemaName:      schema,
			ExplicitSchema:  true,
			ExplicitCatalog: true,
		},
	}}
}

// MakeUnqualifiedTableName creates a new base table name.
func MakeUnqualifiedTableName(tbl Name) TableName {
	return TableName{tblName{
		TableName: tbl,
	}}
}

// NewUnqualifiedTableName creates a new base table name.
func NewUnqualifiedTableName(tbl Name) *TableName {
	tn := MakeUnqualifiedTableName(tbl)
	return &tn
}

func makeTableNameFromUnresolvedName(n *UnresolvedName) TableName {
	return TableName{tblName{
		TableName:       Name(n.Parts[0]),
		TableNamePrefix: makeTableNamePrefixFromUnresolvedName(n),
	}}
}

func makeTableNamePrefixFromUnresolvedName(n *UnresolvedName) TableNamePrefix {
	return TableNamePrefix{
		SchemaName:      Name(n.Parts[1]),
		CatalogName:     Name(n.Parts[2]),
		ExplicitSchema:  n.NumParts >= 2,
		ExplicitCatalog: n.NumParts >= 3,
	}
}

// TableNames represents a comma separated list (see the Format method)
// of table names.
type TableNames []TableName

// Format implements the NodeFormatter interface.
func (ts *TableNames) Format(ctx *FmtCtx) {
	sep := ""
	for i := range *ts {
		ctx.WriteString(sep)
		ctx.FormatNode(&(*ts)[i])
		sep = ", "
	}
}
func (ts *TableNames) String() string { return AsString(ts) }

// TableNameWithIndex represents a "table@index", used in statements that
// specifically refer to an index.
type TableNameWithIndex struct {
	Table TableName
	Index UnrestrictedName

	// SearchTable indicates that we have just an index (no table name); we will
	// need to search for a table that has an index with the given name.
	//
	// To allow schema-qualified index names in this case, the index is actually
	// specified in Table as the table name, and Index is empty.
	SearchTable bool
}

// Format implements the NodeFormatter interface.
func (n *TableNameWithIndex) Format(ctx *FmtCtx) {
	ctx.FormatNode(&n.Table)
	if n.Index != "" {
		ctx.WriteByte('@')
		ctx.FormatNode(&n.Index)
	}
}
func (n *TableNameWithIndex) String() string { return AsString(n) }

// TableNameWithIndexList is a list of indexes.
type TableNameWithIndexList []*TableNameWithIndex

// Format implements the NodeFormatter interface.
func (n *TableNameWithIndexList) Format(ctx *FmtCtx) {
	sep := ""
	for _, tni := range *n {
		ctx.WriteString(sep)
		ctx.FormatNode(tni)
		sep = ", "
	}
}
