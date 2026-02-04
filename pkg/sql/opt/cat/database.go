// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

// Schema is an interface to a database schema, which is a namespace that
// contains other database objects, like tables and views. Examples of schema
// are "public" and "crdb_internal".
type Database interface {
	Object

	// // Name returns the fully normalized, fully qualified, and fully resolved
	// // name of the schema (<db-name>.<schema-name>). The ExplicitCatalog
	// // and ExplicitSchema fields will always be true, since all parts of the
	// // name are always specified.
	// Name() *SchemaName

	// // GetDataSourceNames returns the list of names and IDs for the data sources
	// // that the schema contains.
	// GetDataSourceNames(ctx context.Context) ([]SchemaName, descpb.IDs, error)
}
