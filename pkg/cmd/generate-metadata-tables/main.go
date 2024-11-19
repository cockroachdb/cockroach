// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This connects to Postgresql and retrieves all the information about
// the tables in the pg_catalog schema. The result is printed into a
// comma separated lines which are meant to be store at a CSV and used
// to test that cockroachdb pg_catalog is up to date.
//
// This accepts the following arguments:
//
// --user:    to change default pg username of `postgres`
// --addr:    to change default pg address of `localhost:5432`
// --catalog: can be pg_catalog or information_schema. Default is pg_catalog
// --rdbms:   can be postgres or mysql. Default is postgres
// --stdout:  for testing purposes, use this flag to send the output to the
//
//	console
//
// Output of this file should generate (If not using --stout):
// pkg/sql/testdata/<catalog>_tables_from_<rdbms>.json
package main

import (
	"context"
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
	ctx := context.Background()
	flag.Parse()
	conn, err := connect()
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	dbVersion, err := conn.DatabaseVersion(ctx)
	if err != nil {
		panic(err)
	}
	pgCatalogFile := &sql.PGMetadataFile{
		Version:    dbVersion,
		PGMetadata: sql.PGMetadataTables{},
	}

	rows, err := conn.DescribeSchema(ctx)
	if err != nil {
		panic(err)
	}

	rows.ForEachRow(func(tableName, columnName, dataTypeName string, dataTypeOid uint32) {
		pgCatalogFile.PGMetadata.AddColumnMetadata(tableName, columnName, dataTypeName, dataTypeOid)
	})

	writer, closeFn, err := getWriter()
	if err != nil {
		panic(err)
	}
	defer closeFn()
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

func getWriter() (io.Writer, func(), error) {
	if *flagStdout {
		return os.Stdout, func() {}, nil
	}

	filename := sql.TablesMetadataFilename(testdata(), *flagRDBMS, *flagSchema)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	return file, func() { file.Close() }, err
}
