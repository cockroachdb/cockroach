// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiutil

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// IndexNames and IndexNamesList are utilities for representing an
// index span by its corresponding identifiers.
// They include the underlying span for comparison against other spans
// and omit the server and schema parts of their four part name.
type IndexNames struct {
	Database string
	Table    string
	Index    string
	Span     roachpb.Span
}

func (idx IndexNames) String() string {
	return fmt.Sprintf("%s.%s.%s", idx.Database, idx.Table, idx.Index)
}

type IndexNamesList []IndexNames

// quoteIfContainsDot adds quotes to an identifier if it contains
// the four part delimiter '.'.
func quoteIfContainsDot(identifier string) string {
	if strings.Contains(identifier, ".") {
		return `"` + identifier + `"`
	}
	return identifier
}

// ToOutput is a simple function which returns a set of
// de-duplicated, fully referenced database, table, and index names
// depending on the number of databases and tables which appear.
func (idxl IndexNamesList) ToOutput() ([]string, []string, []string) {
	fpi := quoteIfContainsDot
	seenDatabases := map[string]bool{}
	databases := []string{}
	for _, idx := range idxl {
		database := fpi(idx.Database)
		if !seenDatabases[database] {
			seenDatabases[database] = true
			databases = append(databases, database)
		}
	}

	multipleDatabases := len(databases) > 1
	seenTables := map[string]bool{}
	tables := []string{}
	for _, idx := range idxl {
		table := fpi(idx.Table)
		if multipleDatabases {
			table = fpi(idx.Database) + "." + table
		}
		if !seenTables[table] {
			seenTables[table] = true
			tables = append(tables, table)
		}
	}

	multipleTables := len(tables) > 1
	indexes := []string{}
	for _, idx := range idxl {
		index := fpi(idx.Index)
		if multipleTables {
			index = fpi(idx.Table) + "." + index
			if multipleDatabases {
				index = fpi(idx.Database) + "." + index
			}
		}
		indexes = append(indexes, index)
	}

	return databases, tables, indexes
}

// Equal only compares the names, not the spans
func (idx IndexNames) Equal(other IndexNames) bool {
	return idx.Database == other.Database &&
		idx.Table == other.Table &&
		idx.Index == other.Index
}
