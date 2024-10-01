// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// mysql.go has the implementations of DBMetadataConnection to
// connect and retrieve schemas from mysql rdbms.

package rdbms

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
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
	ORDER BY table_name
`

var mysqlExclusions = []*excludePattern{
	{
		pattern: regexp.MustCompile(`innodb_.+`),
		except:  make(map[string]struct{}),
	},
}

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

func (conn mysqlMetadataConnection) DatabaseVersion(
	ctx context.Context,
) (version string, err error) {
	row := conn.QueryRowContext(ctx, "SELECT version()")
	err = row.Scan(&version)
	return version, err
}

func (conn mysqlMetadataConnection) DescribeSchema(
	ctx context.Context,
) (*ColumnMetadataList, error) {
	metadata := &ColumnMetadataList{exclusions: mysqlExclusions}
	rows, err := conn.QueryContext(ctx, mysqlDescribeSchema, conn.catalog)
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
		metadata.data = append(metadata.data, row)
	}

	return metadata, nil
}

func (conn mysqlMetadataConnection) Close(ctx context.Context) error {
	return conn.DB.Close()
}
