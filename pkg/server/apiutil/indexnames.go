// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiutil

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// IndexNames and IndexNamesList are utilities for representing the indexes
// found in descriptors in human readable formats.
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

// ToOutput is a simple function which returns a set of de-duplicated,
// fully referenced database, table, and index names depending on the,
// number of databases and tables which appear.
func (idxl IndexNamesList) ToOutput() ([]string, []string, []string) {
	seenDatabases := map[string]bool{}
	databases := []string{}
	for _, idx := range idxl {
		if !seenDatabases[idx.Database] {
			seenDatabases[idx.Database] = true
			databases = append(databases, idx.Database)
		}
	}

	multipleDatabases := len(databases) > 1
	seenTables := map[string]bool{}
	tables := []string{}
	for _, idx := range idxl {
		table := idx.Table
		if multipleDatabases {
			table = idx.Database + "." + table
		}
		if !seenTables[table] {
			seenTables[table] = true
			tables = append(tables, table)
		}
	}

	multipleTables := len(tables) > 1
	indexes := []string{}
	for _, idx := range idxl {
		index := idx.Index
		if multipleTables {
			index = idx.Table + "." + index
			if multipleDatabases {
				index = idx.Database + "." + index
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
