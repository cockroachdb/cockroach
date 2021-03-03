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
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/jackc/pgx"
)

const getServerVersion = `SELECT current_setting('server_version');`

var (
	postgresAddr   = flag.String("addr", "localhost:5432", "Postgres server address")
	postgresUser   = flag.String("user", "postgres", "Postgres user")
	postgresSchema = flag.String("catalog", "pg_catalog", "Catalog or namespace, default: pg_catalog")
)

func main() {
	flag.Parse()
	db := connect()
	defer closeDB(db)
	pgCatalogFile := &sql.PGMetadataFile{
		PGVersion:  getPGVersion(db),
		PGMetadata: sql.PGMetadataTables{},
	}

	rows := describePgCatalog(db)
	defer rows.Close()
	for rows.Next() {
		var table, column, dataType string
		var dataTypeOid uint32
		if err := rows.Scan(&table, &column, &dataType, &dataTypeOid); err != nil {
			panic(err)
		}
		pgCatalogFile.PGMetadata.AddColumnMetadata(table, column, dataType, dataTypeOid)
	}

	pgCatalogFile.Save(os.Stdout)
}

func describePgCatalog(conn *pgx.Conn) *pgx.Rows {
	rows, err := conn.Query(sql.GetPGMetadataSQL, *postgresSchema)
	if err != nil {
		panic(err)
	}
	return rows
}

func getPGVersion(conn *pgx.Conn) (pgVersion string) {
	row := conn.QueryRow(getServerVersion)
	if err := row.Scan(&pgVersion); err != nil {
		panic(err)
	}
	return
}

func connect() *pgx.Conn {
	conf, err := pgx.ParseURI(fmt.Sprintf("postgresql://%s@%s?sslmode=disable", *postgresUser, *postgresAddr))
	if err != nil {
		panic(err)
	}
	conn, err := pgx.Connect(conf)
	if err != nil {
		panic(err)
	}

	return conn
}

func closeDB(conn *pgx.Conn) {
	if err := conn.Close(); err != nil {
		panic(err)
	}
}
