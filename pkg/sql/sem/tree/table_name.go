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
// UnresolvedName via NormalizableTableName. See
// normalizable_table_name.go.
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
	f := ctx.flags
	alwaysFormat := f.HasFlags(FmtAlwaysQualifyTableNames) || ctx.tableNameFormatter != nil
	if tp.ExplicitSchema || alwaysFormat {
		if tp.ExplicitCatalog || alwaysFormat {
			ctx.FormatNode(&tp.CatalogName)
			ctx.WriteByte('.')
		}
		ctx.FormatNode(&tp.SchemaName)
		ctx.WriteByte('.')
	}
}
func (t *TableNamePrefix) String() string { return AsString(t) }

// Schema retrieves the unqualified schema name.
func (t *TableNamePrefix) Schema() string {
	return string(t.SchemaName)
}

// Catalog retrieves the unqualified catalog name.
func (t *TableNamePrefix) Catalog() string {
	return string(t.CatalogName)
}

// Format implements the NodeFormatter interface.
func (t *TableName) Format(ctx *FmtCtx) {
	t.TableNamePrefix.Format(ctx)
	ctx.FormatNode(&t.TableName)
}
func (t *TableName) String() string { return AsString(t) }

// Table retrieves the unqualified table name.
func (t *TableName) Table() string {
	return string(t.TableName)
}

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
