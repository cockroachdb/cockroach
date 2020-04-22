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

// objName is the internal type for a qualified object.
type objName struct {
	// ObjectName is the unqualified name for the object
	// (table/view/sequence/function/type).
	ObjectName Name

	// ObjectNamePrefix is the path to the object.  This can be modified
	// further by name resolution, see name_resolution.go.
	ObjectNamePrefix
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
func (tp *ObjectNamePrefix) Format(ctx *FmtCtx) {
	alwaysFormat := ctx.alwaysFormatTablePrefix()
	if tp.ExplicitSchema || alwaysFormat {
		if tp.ExplicitCatalog || alwaysFormat {
			ctx.FormatNode(&tp.CatalogName)
			ctx.WriteByte('.')
		}
		ctx.FormatNode(&tp.SchemaName)
	}
}

func (tp *ObjectNamePrefix) String() string { return AsString(tp) }

// Schema retrieves the unqualified schema name.
func (tp *ObjectNamePrefix) Schema() string {
	return string(tp.SchemaName)
}

// Catalog retrieves the unqualified catalog name.
func (tp *ObjectNamePrefix) Catalog() string {
	return string(tp.CatalogName)
}
