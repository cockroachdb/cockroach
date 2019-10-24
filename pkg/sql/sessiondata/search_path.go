// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondata

import "strings"

// PgDatabaseName is the name of the default postgres system database.
const PgDatabaseName = "postgres"

// DefaultTemporarySchema is the temporary schema new sessions that have not
// created a temporary table start off with.
// This is prefixed with `pg_` to ensure there is no clash with a user defined
// schema if/when CRDB supports them. In PG, schema names starting with `pg_`
// are "reserved", so this can never clash with an actual physical schema.
const DefaultTemporarySchema = "pg_no_temp_schema"

// DefaultDatabaseName is the name ofthe default CockroachDB database used
// for connections without a current db set.
const DefaultDatabaseName = "defaultdb"

// PgCatalogName is the name of the pg_catalog system schema.
const PgCatalogName = "pg_catalog"

const PgTempSchemaName = "pg_temp"

// SearchPath represents a list of namespaces to search builtins in.
// The names must be normalized (as per Name.Normalize) already.
type SearchPath struct {
	paths                []string
	containsPgCatalog    bool
	containsPgTempSchema bool
	TempScName           string
}

// MakeSearchPath returns a new immutable SearchPath struct. The paths slice
// must not be modified after hand-off to MakeSearchPath.
func MakeSearchPath(paths []string, tempScName string) SearchPath {
	containsPgCatalog := false
	containsPgTempSchema := false
	for _, e := range paths {
		if e == PgCatalogName {
			containsPgCatalog = true
		} else if e == PgTempSchemaName {
			containsPgTempSchema = true
		}
	}
	return SearchPath{
		paths:                paths,
		containsPgCatalog:    containsPgCatalog,
		containsPgTempSchema: containsPgTempSchema,
		TempScName:           tempScName,
	}
}

func (s *SearchPath) UpdateTemporarySchemaName(tempScName string) {
	s.TempScName = tempScName
}

func (s *SearchPath) UpdatePaths(paths []string) {
	nsp := MakeSearchPath(paths, s.TempScName)
	s.paths = nsp.paths
	s.containsPgCatalog = nsp.containsPgCatalog
	s.containsPgTempSchema = nsp.containsPgTempSchema
}

func (s *SearchPath) MaybeResolveTemporarySchema(scName string) string {
	// If the scName is pg_temp and the TempScName has been set, pg_temp
	// can be used as an alias for name resolution.
	if scName == PgTempSchemaName && s.TempScName != DefaultTemporarySchema {
		return s.TempScName
	}
	return scName
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
	var paths []string
	if !s.containsPgTempSchema {
		paths = append(paths, PgTempSchemaName)
	}
	if !s.containsPgCatalog {
		paths = append(paths, PgCatalogName)
	}

	return SearchPathIter{paths: append(paths, s.paths...), tempScName: s.TempScName, i: 0}
}

// IterWithoutImplicitPGSchemas is the same as Iter, but does not include the
// implicit pg_temp and pg_catalog.
func (s SearchPath) IterWithoutImplicitPGSchemas() SearchPathIter {
	return SearchPathIter{paths: s.paths, tempScName: s.TempScName, i: 0}
}

// GetPathArray returns the underlying path array of this SearchPath. The
// resultant slice is not to be modified.
func (s SearchPath) GetPathArray() []string {
	return s.paths
}

func (s SearchPath) GetTemporarySchema() string {
	return s.TempScName
}

// Equals returns true if two SearchPaths are the same.
func (s SearchPath) Equals(other *SearchPath) bool {
	if s.containsPgCatalog != other.containsPgCatalog {
		return false
	}
	if len(s.paths) != len(other.paths) {
		return false
	}
	// Fast path: skip the check if it is the same slice.
	if &s.paths[0] != &other.paths[0] {
		for i := range s.paths {
			if s.paths[i] != other.paths[i] {
				return false
			}
		}
	}
	return true
}

func (s SearchPath) String() string {
	return strings.Join(s.paths, ", ")
}

// SearchPathIter enables iteration over the search paths without triggering an
// allocation. Use one of the SearchPath.Iter methods to get an instance of the
// iterator, and then repeatedly call the Next method in order to iterate over
// each search path.
type SearchPathIter struct {
	paths      []string
	tempScName string
	i          int
}

// Next returns the next search path, or false if there are no remaining paths.
func (iter *SearchPathIter) Next() (path string, ok bool) {
	if iter.i < len(iter.paths) {
		scName := iter.paths[iter.i]
		iter.i++
		if scName == PgTempSchemaName && iter.tempScName != DefaultTemporarySchema {
			return iter.tempScName, true
		} else if scName == PgTempSchemaName && iter.tempScName == DefaultTemporarySchema {
			// If the search path contains pg_temp, but no temporary schema has been
			// created, we can skip it.
			return iter.Next()
		}
		return scName, true
	}
	return "", false
}
