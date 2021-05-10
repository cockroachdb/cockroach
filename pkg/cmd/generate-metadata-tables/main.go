// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This connects to Postgresql and retrieves all the information about
// the tables in the pg_catalog schema. The result is printed into a
// comma separated lines which are meant to be store at a CSV and used
// to test that cockroachdb pg_catalog is up to date.
//
// This accepts the following arguments:
//
// -user: to change default pg username of `postgres`
// -addr: to change default pg address of `localhost:5432`
//
// Output of this file should generate:
// pkg/sql/testdata/pg_catalog_tables
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/generate-metadata-tables/rdbms"
	"github.com/cockroachdb/cockroach/pkg/sql"
)

var testdataDir = filepath.Join("pkg", "sql", "testdata")

// Flags or command line arguments.
var (
	flagAddress = flag.String("addr", "localhost:5432", "RDBMS address")
	flagUser    = flag.String("user", "postgres", "RDBMS user")
	flagSchema  = flag.String("catalog", "pg_catalog", "Catalog or namespace")
	flagRDBMS   = flag.String("rdbms", sql.Postgres, "Determines which RDBMS it will connect")
	flagStdout  = flag.Bool("stdout", false, "Instead of re-writing the test data it will print the out to console")
)

func main() {
	flag.Parse()
	conn, err := connect()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	dbVersion, err := conn.DatabaseVersion()
	if err != nil {
		panic(err)
	}
	pgCatalogFile := &sql.PGMetadataFile{
		PGVersion:  dbVersion,
		PGMetadata: sql.PGMetadataTables{},
	}

	rows, err := conn.DescribeSchema()
	if err != nil {
		panic(err)
	}

	rows.ForEachRow(func(tableName, columnName, dataTypeName string, dataTypeOid uint32) {
		pgCatalogFile.PGMetadata.AddColumnMetadata(tableName, columnName, dataTypeName, dataTypeOid)
		columnType := pgCatalogFile.PGMetadata[tableName][columnName]
		if dataTypeOid != 0 && !columnType.IsImplemented() {
			pgCatalogFile.AddUnimplementedType(columnType)
		}
	})

	writer, err := getWriter()
	if err != nil {
		panic(err)
	}
	pgCatalogFile.Save(writer)
}

func connect() (rdbms.DBMetadataConnection, error) {
	connect, ok := rdbms.ConnectFns[*flagRDBMS]
	if !ok {
		return nil, fmt.Errorf("connect to %s is not supported", *flagRDBMS)
	}
	return connect(*flagAddress, *flagUser, *flagSchema)
}

func testdata() string {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	if strings.HasSuffix(filepath.Join("pkg", "cmd", "generate-metadata-tables"), path) {
		return filepath.Join("..", "..", "..", testdataDir)
	}
	return testdataDir
}

func getWriter() (io.Writer, error) {
	if *flagStdout {
		return os.Stdout, nil
	}

	filename := sql.TablesMetadataFilename(testdata(), *flagRDBMS, *flagSchema)
	return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}
