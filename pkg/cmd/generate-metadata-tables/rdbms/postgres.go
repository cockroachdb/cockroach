// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// postgres.go has the implementations of DBMetadataConnection to
// connect and retrieve schemas from postgres rdbms.

package rdbms

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/jackc/pgx"
	"github.com/lib/pq/oid"
)

const getServerVersion = `SELECT current_setting('server_version');`

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
	oid.T_xid:          oid.T_int8,
	oid.T_pg_lsn:       oid.T_text,
}

type pgMetadataConnection struct {
	*pgx.Conn
	catalog string
}

func postgresConnect(address, user, catalog string) (DBMetadataConnection, error) {
	conf, err := pgx.ParseURI(fmt.Sprintf("postgresql://%s@%s?sslmode=disable", user, address))
	if err != nil {
		return nil, err
	}
	conn, err := pgx.Connect(conf)
	if err != nil {
		return nil, err
	}

	return pgMetadataConnection{conn, catalog}, nil
}

func (conn pgMetadataConnection) DescribeSchema() (ColumnMetadataList, error) {
	var metadata []*columnMetadata
	rows, err := conn.Query(sql.GetPGMetadataSQL, conn.catalog)
	if err != nil {
		return nil, err
	}
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
		row := new(columnMetadata)
		row.tableName = table
		row.columnName = column
		row.dataTypeName = mappedTypeName
		row.dataTypeOid = mappedTypeOid
		metadata = append(metadata, row)
	}
	return metadata, nil
}

func (conn pgMetadataConnection) DatabaseVersion() (pgVersion string, err error) {
	row := conn.QueryRow(getServerVersion)
	err = row.Scan(&pgVersion)
	return pgVersion, err
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
