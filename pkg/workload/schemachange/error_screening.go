// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/jackc/pgx"
)

func tableExists(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	return scanBool(tx, `SELECT EXISTS (
	SELECT table_name
    FROM information_schema.tables 
   WHERE table_schema = $1
     AND table_name = $2
   )`, tableName.Schema(), tableName.Object())
}

func viewExists(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	return scanBool(tx, `SELECT EXISTS (
	SELECT table_name
    FROM information_schema.views 
   WHERE table_schema = $1
     AND table_name = $2
   )`, tableName.Schema(), tableName.Object())
}

func columnExistsOnTable(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	return scanBool(tx, `SELECT EXISTS (
	SELECT column_name
    FROM information_schema.columns 
   WHERE table_schema = $1
     AND table_name = $2
     AND column_name = $3
   )`, tableName.Schema(), tableName.Object(), columnName)
}

func typeExists(tx *pgx.Tx, typ tree.ResolvableTypeReference) (bool, error) {
	if !strings.Contains(typ.SQLString(), "enum") {
		return true, nil
	}

	return scanBool(tx, `SELECT EXISTS (
	SELECT typname
		FROM pg_catalog.pg_type
   WHERE typname = $1
	)`, typ.SQLString())
}

func tableHasRows(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	return scanBool(tx, fmt.Sprintf(`SELECT EXISTS (SELECT * FROM %s)`, tableName.String()))
}

func scanBool(tx *pgx.Tx, query string, args ...interface{}) (b bool, err error) {
	err = tx.QueryRow(query, args...).Scan(&b)
	return b, err
}

func schemaExists(tx *pgx.Tx, schemaName string) (bool, error) {
	return scanBool(tx, `SELECT EXISTS (
	SELECT schema_name
		FROM information_schema.schemata
   WHERE schema_name = $1
	)`, schemaName)
}
