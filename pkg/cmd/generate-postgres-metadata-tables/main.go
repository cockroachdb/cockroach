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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/jackc/pgx"
	"github.com/lib/pq/oid"
)

const getServerVersion = `SELECT current_setting('server_version');`

// Flags or command line arguments.
var (
	postgresAddr   = flag.String("addr", "localhost:5432", "Postgres server address")
	postgresUser   = flag.String("user", "postgres", "Postgres user")
	postgresSchema = flag.String("catalog", "pg_catalog", "Catalog or namespace, default: pg_catalog")
)

// Map for unimplemented types.
// For reference see:
// https://www.npgsql.org/doc/dev/type-representations.html
var unimplementedEquivalencies = map[oid.Oid]oid.Oid{
	// These types only exists in information_schema.
	// cardinal_number in postgres is an INT4 but we already implemented columns as INT8.
	oid.Oid(13438): oid.T_int8,        // cardinal_number
	oid.Oid(13450): oid.T_text,        // yes_or_no
	oid.Oid(13441): oid.T_text,        // character_data
	oid.Oid(13443): oid.T_text,        // sql_identifier
	oid.Oid(13448): oid.T_timestamptz, // time_stamp

	// Other types
	oid.T__aclitem:     oid.T__text,
	oid.T_pg_node_tree: oid.T_text,
	oid.T_xid:          oid.T_oid,
	oid.T_pg_lsn:       oid.T_text,
}

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
		var table, column, dataTypeName string
		var dataTypeOid uint32
		if err := rows.Scan(&table, &column, &dataTypeName, &dataTypeOid); err != nil {
			panic(err)
		}
		mappedTypeName, mappedTypeOid, err := getMappedType(dataTypeName, dataTypeOid)
		if err != nil {
			panic(err)
		}
		pgCatalogFile.PGMetadata.AddColumnMetadata(table, column, mappedTypeName, mappedTypeOid)
		columnType := pgCatalogFile.PGMetadata[table][column]
		if !columnType.IsImplemented() {
			pgCatalogFile.AddUnimplementedType(columnType)
		}
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

// getMappedType checks if the postgres type can be implemented as other crdb
// implemented type.
func getMappedType(dataTypeName string, dataTypeOid uint32) (string, uint32, error) {
	actualOid := oid.Oid(dataTypeOid)
	mappedOid, ok := unimplementedEquivalencies[actualOid]
	if !ok {
		// No mapped type
		return dataTypeName, dataTypeOid, nil
	}

	_, ok = types.OidToType[mappedOid]
	if !ok {
		// not expected this to happen
		return "", 0, fmt.Errorf("type with oid %d is unimplemented", mappedOid)
	}

	typeName, ok := oid.TypeName[mappedOid]
	if !ok {
		// not expected this to happen
		return "", 0, fmt.Errorf("type name for oid %d does not exist in oid.TypeName map", mappedOid)
	}

	return typeName, uint32(mappedOid), nil
}
