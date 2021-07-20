// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// mysql.go has the implementations of DBMetadataConnection to
// connect and retrieve schemas from mysql rdbms.

package rdbms

import (
	gosql "database/sql"
	"fmt"
	"strings"

	// gosql implementation.
	_ "github.com/go-sql-driver/mysql"
)

const mysqlDescribeSchema = `
	SELECT 
		table_name, 
		column_name, 
		data_type 
	FROM information_schema.columns
	WHERE table_schema = ?
	ORDER BY table_name, column_name
`

type mysqlMetadataConnection struct {
	*gosql.DB
	catalog string
}

func mysqlConnect(address, user, catalog string) (DBMetadataConnection, error) {
	db, err := gosql.Open("mysql", fmt.Sprintf("%s@tcp(%s)/%s", user, address, catalog))
	if err != nil {
		return nil, err
	}
	return mysqlMetadataConnection{db, catalog}, nil
}

func (conn mysqlMetadataConnection) DatabaseVersion() (version string, err error) {
	row := conn.QueryRow("SELECT version()")
	err = row.Scan(&version)
	return version, err
}

func (conn mysqlMetadataConnection) DescribeSchema() (ColumnMetadataList, error) {
	var metadata ColumnMetadataList
	rows, err := conn.Query(mysqlDescribeSchema, conn.catalog)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		row := new(columnMetadata)
		if err := rows.Scan(&row.tableName, &row.columnName, &row.dataTypeName); err != nil {
			return nil, err
		}
		row.tableName = strings.ToLower(row.tableName)
		row.columnName = strings.ToLower(row.columnName)
		metadata = append(metadata, row)
	}

	return metadata, nil
}
