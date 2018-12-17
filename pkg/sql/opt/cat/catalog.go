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

// Package cat contains interfaces that are used by the query optimizer to avoid
// including specifics of sqlbase structures in the opt code.
package cat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// StableID permanently and uniquely identifies a catalog object (table, view,
// index, column, etc.) within its scope:
//
//   data source StableID: unique within database
//   index StableID: unique within table
//   column StableID: unique within table
//
// If a new catalog object is created, it will always be assigned a new StableID
// that has never, and will never, be reused by a different object in the same
// scope. This uniqueness guarantee is true even if the new object has the same
// name and schema as an old (and possibly dropped) object. The StableID will
// never change as long as the object exists.
//
// Note that while two instances of the same catalog object will always have the
// same StableID, they can have different schema if the schema has changed over
// time. See the Version type comments for more details.
//
// For sqlbase objects, the StableID is the 32-bit descriptor ID.
type StableID uint32

// Version is incremented any time the schema of a catalog data source (table,
// view, etc.) is changed in any way, including changes to any associated
// indexes. This enables cached data sources (or other data structures dependent
// on the data sources) to be invalidated when their schema changes.
//
// For sqlbase data sources, the version is the 32-bit descriptor version.
type Version uint32

// Catalog is an interface to a database catalog, exposing only the information
// needed by the query optimizer.
type Catalog interface {
	// ResolveDataSource locates a data source with the given name and returns it.
	// If no such data source exists, then ResolveDataSource returns an error. As
	// a side effect, the name parameter is updated to be fully qualified if it
	// was not before (i.e. to include catalog and schema names).
	ResolveDataSource(ctx context.Context, name *tree.TableName) (DataSource, error)

	// ResolveDataSourceByID is similar to ResolveDataSource, except that it
	// locates a data source by its StableID. See the comment for StableID for
	// more details.
	ResolveDataSourceByID(ctx context.Context, id StableID) (DataSource, error)

	// CheckPrivilege verifies that the current user has the given privilege on
	// the given data source. If not, then CheckPrivilege returns an error.
	CheckPrivilege(ctx context.Context, ds DataSource, priv privilege.Kind) error
}
