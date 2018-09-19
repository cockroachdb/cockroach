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

package sessiondata

import (
	"strings"
)

// PgDatabaseName is the name of the default postgres system database.
const PgDatabaseName = "postgres"

// DefaultDatabaseName is the name ofthe default CockroachDB database used
// for connections without a current db set.
const DefaultDatabaseName = "defaultdb"

// PgCatalogName is the name of the pg_catalog system schema.
const PgCatalogName = "pg_catalog"

// SearchPath represents a list of namespaces to search builtins in.
// The names must be normalized (as per Name.Normalize) already.
type SearchPath struct {
	paths             []string
	containsPgCatalog bool
}

// MakeSearchPath returns a new SearchPath struct. The paths slice must not be
// modified after hand-off to MakeSearchPath.
func MakeSearchPath(paths []string) SearchPath {
	containsPgCatalog := false
	for _, e := range paths {
		if e == PgCatalogName {
			containsPgCatalog = true
			break
		}
	}
	return SearchPath{
		paths:             paths,
		containsPgCatalog: containsPgCatalog,
	}
}

// Iter returns an iterator through the search path. We must include the
// implicit pg_catalog at the beginning of the search path, unless it has been
// explicitly set later by the user.
// "The system catalog schema, pg_catalog, is always searched, whether it is
// mentioned in the path or not. If it is mentioned in the path then it will be
// searched in the specified order. If pg_catalog is not in the path then it
// will be searched before searching any of the path items."
// - https://www.postgresql.org/docs/9.1/static/runtime-config-client.html
func (s SearchPath) Iter() SearchPathIter {
	if s.containsPgCatalog {
		return SearchPathIter{paths: s.paths, i: 0}
	}
	return SearchPathIter{paths: s.paths, i: -1}
}

// IterWithoutImplicitPGCatalog is the same as Iter, but does not include the
// implicit pg_catalog.
func (s SearchPath) IterWithoutImplicitPGCatalog() SearchPathIter {
	return SearchPathIter{paths: s.paths, i: 0}
}

// GetPathArray returns the underlying path array of this SearchPath. The
// resultant slice is not to be modified.
func (s SearchPath) GetPathArray() []string {
	return s.paths
}

func (s SearchPath) String() string {
	return strings.Join(s.paths, ", ")
}

// SearchPathIter enables iteration over the search paths without triggering an
// allocation. Use one of the SearchPath.Iter methods to get an instance of the
// iterator, and then repeatedly call the Next method in order to iterate over
// each search path.
type SearchPathIter struct {
	paths []string
	i     int
}

// Next returns the next search path, or false if there are no remaining paths.
func (iter *SearchPathIter) Next() (path string, ok bool) {
	if iter.i == -1 {
		iter.i++
		return PgCatalogName, true
	}
	if iter.i < len(iter.paths) {
		iter.i++
		return iter.paths[iter.i-1], true
	}
	return "", false
}
