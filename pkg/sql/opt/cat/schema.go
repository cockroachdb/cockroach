// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// Schema is an interface to a database schema, which is a namespace that
// contains other database objects, like tables and views. Examples of schema
// are "public" and "crdb_internal".
type Schema interface {
	Object

	// Name returns the fully normalized, fully qualified, and fully resolved
	// name of the schema (<db-name>.<schema-name>). The ExplicitCatalog
	// and ExplicitSchema fields will always be true, since all parts of the
	// name are always specified.
	Name() *SchemaName

	// GetDataSourceNames returns the list of names and IDs for the data sources
	// that the schema contains.
	GetDataSourceNames(ctx context.Context) ([]DataSourceName, descpb.IDs, error)
}
