// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// TableName corresponds to the name of a table in a FROM clause,
// INSERT or UPDATE statement, etc.
//
// This is constructed for incoming SQL queries from an UnresolvedObjectName,
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
	ctx := NewFmtCtx(FmtSimple)
	ctx.FormatNode(&t.CatalogName)
	ctx.WriteByte('.')
	ctx.FormatNode(&t.SchemaName)
	ctx.WriteByte('.')
	ctx.FormatNode(&t.TableName)
	return ctx.CloseAndGetString()
}

// Table retrieves the unqualified table name.
func (t *TableName) Table() string {
	return string(t.TableName)
}

// Equals returns true if the two table names are identical (including
// the ExplicitSchema/ExplicitCatalog flags).
func (t *TableName) Equals(other *TableName) bool {
	return *t == *other
}

// ToUnresolvedObjectName converts the table name to an unresolved object name.
// Schema and catalog are included if indicated by the ExplicitSchema and
// ExplicitCatalog flags.
func (t *TableName) ToUnresolvedObjectName() *UnresolvedObjectName {
	u := &UnresolvedObjectName{}

	u.NumParts = 1
	u.Parts[0] = string(t.TableName)
	if t.ExplicitSchema {
		u.Parts[u.NumParts] = string(t.SchemaName)
		u.NumParts++
	}
	if t.ExplicitCatalog {
		u.Parts[u.NumParts] = string(t.CatalogName)
		u.NumParts++
	}
	return u
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

// MakeTableNameFromPrefix creates a table name from an unqualified name
// and a resolved prefix.
func MakeTableNameFromPrefix(prefix TableNamePrefix, object Name) TableName {
	return TableName{tblName{
		TableName:       object,
		TableNamePrefix: prefix,
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

// TableIndexName refers to a table index. There are a few cases:
//
//  - if both the table name and the index name are set, refers to a specific
//    index in a specific table.
//
//  - if the table name is set and index name is empty, refers to the primary
//    index of that table.
//
//  - if the table name is empty and the index name is set, refers to an index
//    of that name among all tables within a catalog/schema; if there is a
//    duplicate name, that will result in an error. Note that it is possible to
//    specify the schema or catalog without specifying a table name; in this
//    case, Table.TableNamePrefix has the fields set but Table.TableName is
//    empty.
type TableIndexName struct {
	Table TableName
	Index UnrestrictedName
}

// Format implements the NodeFormatter interface.
func (n *TableIndexName) Format(ctx *FmtCtx) {
	if n.Index == "" {
		ctx.FormatNode(&n.Table)
		return
	}

	if n.Table.TableName != "" {
		// The table is specified.
		ctx.FormatNode(&n.Table)
		ctx.WriteByte('@')
		ctx.FormatNode(&n.Index)
		return
	}

	// The table is not specified. The schema/catalog can still be specified.
	if n.Table.ExplicitSchema || ctx.alwaysFormatTablePrefix() {
		ctx.FormatNode(&n.Table.TableNamePrefix)
		ctx.WriteByte('.')
	}
	// In this case, we must format the index name as a restricted name (quotes
	// must be added for reserved keywords).
	ctx.FormatNode((*Name)(&n.Index))
}

func (n *TableIndexName) String() string { return AsString(n) }

// TableIndexNames is a list of indexes.
type TableIndexNames []*TableIndexName

// Format implements the NodeFormatter interface.
func (n *TableIndexNames) Format(ctx *FmtCtx) {
	sep := ""
	for _, tni := range *n {
		ctx.WriteString(sep)
		ctx.FormatNode(tni)
		sep = ", "
	}
}
