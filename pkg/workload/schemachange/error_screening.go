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
	"strconv"
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

func tableHasDependencies(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	return scanBool(tx, `SELECT EXISTS (
		SELECT fd.descriptor_name
		  FROM crdb_internal.forward_dependencies AS fd
		 WHERE fd.descriptor_id =
				(
				SELECT c.oid
				  FROM pg_catalog.pg_class AS c
					JOIN pg_catalog.pg_namespace AS ns
            ON ns.oid = c.relnamespace
         WHERE c.relname = $1
           AND ns.nspname = $2
				)
	)`, tableName.Object(), tableName.Schema())
}

func columnIsDependedOn(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	hasDeps, err := tableHasDependencies(tx, tableName)
	if err != nil {
		return false, err
	}
	if !hasDeps {
		return false, nil
	}

	columnPositions, err := scanString(tx, `
	SELECT fd.dependedonby_details
		  FROM crdb_internal.forward_dependencies AS fd
		 WHERE fd.descriptor_id =
				(
				SELECT c.oid
				  FROM pg_catalog.pg_class AS c
					JOIN pg_catalog.pg_namespace AS ns
            ON ns.oid = c.relnamespace
         WHERE c.relname = $1
           AND ns.nspname = $2
				)
   `, tableName.Object(), tableName.Schema())

	if err != nil {
		return false, err
	}

	position, err := scanInt(tx, `
	SELECT ordinal_position
    FROM information_schema.columns 
   WHERE table_schema = $1
     AND table_name = $2
     AND column_name = $3
   `, tableName.Schema(), tableName.Object(), columnName)

	if err != nil {
		return false, err
	}

	columnPositionsArray := strings.Split(columnPositions, ": ")
	if len(columnPositionsArray) != 2 {
		return false, fmt.Errorf("failed to parse dependedonby_details in columnIsDependedOn")
	}
	columns := strings.Split(columnPositionsArray[1][1:len(columnPositionsArray[1])-1], " ")
	for _, ordinalPositionString := range columns {
		ordinalPosition, err := strconv.Atoi(ordinalPositionString)
		if err != nil {
			return false, err
		}

		if position == ordinalPosition {
			return true, nil
		}
	}

	return false, nil
}

func colIsPrimaryKey(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	return scanBool(tx, `SELECT EXISTS(
	SELECT column_name from information_schema.table_constraints AS c
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.table_name = c.table_name AND ccu.table_schema = c.table_schema AND ccu.constraint_name = c.constraint_name
   WHERE c.table_schema = $1 AND c.table_name = $2 AND ccu.column_name = $3 AND c.constraint_type = 'PRIMARY KEY'
	)`, tableName.Schema(), tableName.Object(), columnName)
}

func scanInt(tx *pgx.Tx, query string, args ...interface{}) (i int, err error) {
	err = tx.QueryRow(query, args...).Scan(&i)
	return i, err
}

func scanString(tx *pgx.Tx, query string, args ...interface{}) (s string, err error) {
	err = tx.QueryRow(query, args...).Scan(&s)
	return s, err
}
