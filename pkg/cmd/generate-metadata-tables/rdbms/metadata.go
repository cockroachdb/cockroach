// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// metadata.go provides Connectivity for mysql and postgres, and
// provides interfaces that helps to retrieve a schema from these
// databases for comparison purposes.

package rdbms

import (
	"io"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql"
)

// ConnectFns will be used to determine which kind of database will be used
// at runtime based on the flag.
var ConnectFns = map[string]func(address, user, catalog string) (DBMetadataConnection, error){
	sql.Postgres: postgresConnect,
	sql.MySQL:    mysqlConnect,
}

type columnMetadata struct {
	tableName    string
	columnName   string
	dataTypeName string
	dataTypeOid  uint32
}

type excludePattern struct {
	pattern *regexp.Regexp
	except  map[string]struct{}
}

// ColumnMetadataList is a list of rows coming from rdbms describing a column.
type ColumnMetadataList struct {
	data       []*columnMetadata
	exclusions []*excludePattern
}

// DBMetadataConnection structs can describe a schema like pg_catalog or
// information_schema.
type DBMetadataConnection interface {
	io.Closer
	DescribeSchema() (*ColumnMetadataList, error)
	DatabaseVersion() (string, error)
}

// ForEachRow iterates over the rows gotten from DescribeSchema() call.
func (l *ColumnMetadataList) ForEachRow(addRow func(string, string, string, uint32)) {
	addRowIfAllowed := func(metadata *columnMetadata) {
		for _, exclusion := range l.exclusions {
			tableName := metadata.tableName
			if _, ok := exclusion.except[tableName]; exclusion.pattern.MatchString(tableName) && !ok {
				return
			}
		}

		addRow(metadata.tableName, metadata.columnName, metadata.dataTypeName, metadata.dataTypeOid)
	}
	for _, metadata := range l.data {
		addRowIfAllowed(metadata)
	}
}
