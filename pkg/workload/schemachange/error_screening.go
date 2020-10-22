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
	q := fmt.Sprintf(`SELECT EXISTS (
	SELECT table_name
    FROM information_schema.tables 
   WHERE table_schema = '%s'
     AND table_name = '%s'
   )`, tableName.SchemaName, tableName.ObjectName.String())

	var exists bool
	if err := tx.QueryRow(q).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func columnExistsOnTable(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	q := fmt.Sprintf(`SELECT EXISTS (
	SELECT column_name
    FROM information_schema.columns 
   WHERE table_schema = '%s'
     AND table_name = '%s'
     AND column_name = '%s'
   )`, tableName.SchemaName, tableName.ObjectName, columnName)

	var exists bool
	if err := tx.QueryRow(q).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func typeExists(tx *pgx.Tx, typ tree.ResolvableTypeReference) (bool, error) {
	if !strings.Contains(typ.SQLString(), "enum") {
		return true, nil
	}

	q := fmt.Sprintf(`SELECT EXISTS (
	SELECT typname
		FROM pg_catalog.pg_type
   WHERE typname = '%s'
	)`, typ.SQLString())

	var exists bool
	if err := tx.QueryRow(q).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func tableHasRows(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	q := fmt.Sprintf(`
		SELECT count(*)  
			FROM %s 
	`, tableName.String())

	var count int
	if err := tx.QueryRow(q).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
