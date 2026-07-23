// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// postgres.go has the implementations of DBMetadataConnection to
// connect and retrieve schemas from postgres rdbms.

package rdbms

import (
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq/oid"
)

const getServerVersion = `SELECT current_setting('server_version');`

// Map for unimplemented types.
// For reference see:
// https://www.npgsql.org/doc/dev/type-representations.html
var unimplementedEquivalencies = map[oid.Oid]oid.Oid{
	// These types only exists in information_schema.
	// cardinal_number in postgres is an INT4 but we already implemented columns as INT8.
	oid.Oid(12653): oid.T_int8,        // cardinal_number
	oid.Oid(12665): oid.T_text,        // yes_or_no
	oid.Oid(12656): oid.T_text,        // character_data
	oid.Oid(12658): oid.T_text,        // sql_identifier
	oid.Oid(12663): oid.T_timestamptz, // time_stamp

	// pg_catalog
	oid.Oid(2277): oid.T__text, // anyarray
	oid.Oid(3361): oid.T_bytea, // pg_ndistinct
	oid.Oid(3402): oid.T_bytea, // pg_dependencies
	oid.Oid(5017): oid.T_bytea, // pg_mcv_list

	// Other types
	oid.T__aclitem:     oid.T__text,
	oid.T_pg_node_tree: oid.T_text,
	oid.T_xid:          oid.T_int8,
	oid.T_pg_lsn:       oid.T_text,
}

var postgresExclusions = []*excludePattern{
	{
		pattern: regexp.MustCompile(`^_pg_.+$`),
		except:  make(map[string]struct{}),
	},
}

type pgMetadataConnection struct {
	*pgx.Conn
	catalog string
}

func postgresConnect(address, user, catalog string) (DBMetadataConnection, error) {
	conf, err := pgx.ParseConfig(fmt.Sprintf("postgresql://%s@%s?sslmode=disable", user, address))
	if err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(context.Background(), conf)
	if err != nil {
		return nil, err
	}

	return pgMetadataConnection{conn, catalog}, nil
}

func (conn pgMetadataConnection) DescribeSchema(ctx context.Context) (*ColumnMetadataList, error) {
	var metadata []*columnMetadata
	rows, err := conn.Query(ctx, sql.GetPGMetadataSQL, conn.catalog)
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
	return &ColumnMetadataList{data: metadata, exclusions: postgresExclusions}, nil
}

func (conn pgMetadataConnection) DatabaseVersion(
	ctx context.Context,
) (pgVersion string, err error) {
	row := conn.QueryRow(ctx, getServerVersion)
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
