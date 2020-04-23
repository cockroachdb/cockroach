// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// ObjectName is an interface for operating on names of qualified objects
// including the types, tables, views and sequences.
type ObjectName interface {
	NodeFormatter

	// Set and retrieve the catalog of the object.
	Catalog() string
	SetCatalog(string)

	// Set and retrieve the schema of the object.
	Schema() string
	SetSchema(string)

	// Retrieve the unqualified name of the object.
	Object() string

	// Set and retrieve whether or not the object explicitly defines its catalog.
	HasExplicitCatalog() bool
	SetExplicitCatalog(bool)

	// Set and retrieve whether or not the object explicitly defines its schema.
	HasExplicitSchema() bool
	SetExplicitSchema(bool)
}

var _ ObjectName = &TableName{}
var _ ObjectName = &TypeName{}

// objName is the internal type for a qualified object.
type objName struct {
	// ObjectName is the unqualified name for the object
	// (table/view/sequence/function/type).
	ObjectName Name

	// ObjectNamePrefix is the path to the object.  This can be modified
	// further by name resolution, see name_resolution.go.
	ObjectNamePrefix
}

// Object implements the ObjectName interface.
func (o *objName) Object() string {
	return string(o.ObjectName)
}

// ObjectNamePrefix corresponds to the path prefix of an object name.
type ObjectNamePrefix struct {
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
func (op *ObjectNamePrefix) Format(ctx *FmtCtx) {
	alwaysFormat := ctx.alwaysFormatTablePrefix()
	if op.ExplicitSchema || alwaysFormat {
		if op.ExplicitCatalog || alwaysFormat {
			ctx.FormatNode(&op.CatalogName)
			ctx.WriteByte('.')
		}
		ctx.FormatNode(&op.SchemaName)
	}
}

func (op *ObjectNamePrefix) String() string { return AsString(op) }

// Schema retrieves the unqualified schema name.
func (op *ObjectNamePrefix) Schema() string {
	return string(op.SchemaName)
}

// SetSchema implements the ObjectName interface.
func (op *ObjectNamePrefix) SetSchema(schema string) {
	op.SchemaName = Name(schema)
}

// HasExplicitSchema implements the ObjectName interface.
func (op *ObjectNamePrefix) HasExplicitSchema() bool {
	return op.ExplicitSchema
}

// SetExplicitSchema implements the ObjectName interface.
func (op *ObjectNamePrefix) SetExplicitSchema(val bool) {
	op.ExplicitSchema = val
}

// Catalog retrieves the unqualified catalog name.
func (op *ObjectNamePrefix) Catalog() string {
	return string(op.CatalogName)
}

// SetCatalog implements the ObjectName interface.
func (op *ObjectNamePrefix) SetCatalog(catalog string) {
	op.CatalogName = Name(catalog)
}

// HasExplicitCatalog implements the ObjectName interface.
func (op *ObjectNamePrefix) HasExplicitCatalog() bool {
	return op.ExplicitCatalog
}

// SetExplicitCatalog implements the ObjectName interface.
func (op *ObjectNamePrefix) SetExplicitCatalog(val bool) {
	op.ExplicitCatalog = val
}
