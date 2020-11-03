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

func sequenceExists(tx *pgx.Tx, seqName *tree.TableName) (bool, error) {
	return scanBool(tx, `SELECT EXISTS (
	SELECT sequence_name
    FROM information_schema.sequences
   WHERE sequence_schema = $1
     AND sequence_name = $2
   )`, seqName.Schema(), seqName.Object())
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
	return scanBool(tx, `
	SELECT EXISTS(
        SELECT fd.descriptor_name
          FROM crdb_internal.forward_dependencies AS fd
         WHERE fd.descriptor_id
               = (
                    SELECT c.oid
                      FROM pg_catalog.pg_class AS c
                      JOIN pg_catalog.pg_namespace AS ns ON
                            ns.oid = c.relnamespace
                     WHERE c.relname = $1 AND ns.nspname = $2
                )
       )
	`, tableName.Object(), tableName.Schema())
}

func columnIsDependedOn(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	// To see if a column is depended on, the ordinal_position of the column is looked up in
	// information_schema.columns. Then, this position is used to see if that column has view dependencies
	// or foreign key dependencies which would be stored in crdb_internal.forward_dependencies and
	// pg_catalog.pg_constraint respectively.
	//
	// crdb_internal.forward_dependencies.dependedonby_details is an array of ordinal positions
	// stored as a list of numbers in a string, so SQL functions are used to parse these values
	// into arrays. unnest is used to flatten rows with this column of array type into multiple rows,
	// so performing unions and joins is easier.
	return scanBool(tx, `SELECT EXISTS(
		SELECT source.column_id
			FROM (
			   SELECT DISTINCT column_id
			     FROM (
			           SELECT unnest(
			                   string_to_array(
			                    rtrim(
			                     ltrim(
			                      fd.dependedonby_details,
			                      'Columns: ['
			                     ),
			                     ']'
			                    ),
			                    ' '
			                   )::INT8[]
			                  ) AS column_id
			             FROM crdb_internal.forward_dependencies
			                   AS fd
			            WHERE fd.descriptor_id
			                  = $1::REGCLASS
			          )
			   UNION  (
			           SELECT unnest(confkey) AS column_id
			             FROM pg_catalog.pg_constraint
			            WHERE confrelid = $1::REGCLASS
			          )
			 ) AS cons
			 INNER JOIN (
			   SELECT ordinal_position AS column_id
			     FROM information_schema.columns
			    WHERE table_schema = $2
			      AND table_name = $3
			      AND column_name = $4
			  ) AS source ON source.column_id = cons.column_id
)`, tableName.String(), tableName.Schema(), tableName.Object(), columnName)
}

func colIsPrimaryKey(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	return scanBool(tx, `
	SELECT EXISTS(
				SELECT column_name
				  FROM information_schema.table_constraints AS c
				  JOIN information_schema.constraint_column_usage
								AS ccu ON ccu.table_name = c.table_name
				      AND ccu.table_schema = c.table_schema
				      AND ccu.constraint_name = c.constraint_name
				 WHERE c.table_schema = $1
				   AND c.table_name = $2
				   AND ccu.column_name = $3
				   AND c.constraint_type = 'PRIMARY KEY'
       );
	`, tableName.Schema(), tableName.Object(), columnName)
}

// valuesViolateUniqueConstraints determines if any unique constraints (including primary constraints)
// will be violated upon inserting the specified rows into the specified table.
func violatesUniqueConstraints(
	tx *pgx.Tx, tableName *tree.TableName, columns []string, rows [][]string,
) (bool, error) {

	if len(rows) == 0 {
		return false, fmt.Errorf("violatesUniqueConstraints: no rows provided")
	}

	// Fetch unique constraints from the database. The format returned is an array of string arrays.
	// Each string array is a group of column names for which a unique constraint exists.
	constraints, err := scanStringArrayRows(tx, `
	 SELECT DISTINCT array_agg(cols.column_name ORDER BY cols.column_name)
					    FROM (
					          SELECT d.oid,
					                 d.table_name,
					                 d.schema_name,
					                 conname,
					                 contype,
					                 unnest(conkey) AS position
					            FROM (
					                  SELECT c.oid AS oid,
					                         c.relname AS table_name,
					                         ns.nspname AS schema_name
					                    FROM pg_catalog.pg_class AS c
					                    JOIN pg_catalog.pg_namespace AS ns ON
					                          ns.oid = c.relnamespace
					                   WHERE ns.nspname = $1
					                     AND c.relname = $2
					                 ) AS d
					            JOIN (
					                  SELECT conname, conkey, conrelid, contype
					                    FROM pg_catalog.pg_constraint
					                   WHERE contype = 'p' OR contype = 'u'
					                 ) ON conrelid = d.oid
					         ) AS cons
					    JOIN (
					          SELECT table_name,
					                 table_schema,
					                 column_name,
					                 ordinal_position
					            FROM information_schema.columns
					         ) AS cols ON cons.schema_name = cols.table_schema
					                  AND cols.table_name = cons.table_name
					                  AND cols.ordinal_position = cons.position
					GROUP BY cons.conname;
`, tableName.Schema(), tableName.Object())
	if err != nil {
		return false, err
	}

	for _, constraint := range constraints {
		// previousRows is used to check unique constraints among the values which
		// will be inserted into the database.
		previousRows := map[string]bool{}
		for _, row := range rows {
			violation, err := violatesUniqueConstraintsHelper(tx, tableName, columns, constraint, row, previousRows)
			if err != nil {
				return false, err
			}
			if violation {
				return true, nil
			}
		}
	}

	return false, nil
}

func violatesUniqueConstraintsHelper(
	tx *pgx.Tx,
	tableName *tree.TableName,
	columns []string,
	constraint []string,
	row []string,
	previousRows map[string]bool,
) (bool, error) {

	// Put values to be inserted into a column name to value map to simplify lookups.
	columnsToValues := map[string]string{}
	for i := 0; i < len(columns); i++ {
		columnsToValues[columns[i]] = row[i]
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf(`SELECT EXISTS (
			SELECT *
				FROM %s
       WHERE
		`, tableName.String()))

	atLeastOneNonNullValue := false
	for _, column := range constraint {

		// Null values are not checked because unique constraints do not apply to null values.
		if columnsToValues[column] != "NULL" {
			if atLeastOneNonNullValue {
				query.WriteString(fmt.Sprintf(` AND %s = %s`, column, columnsToValues[column]))
			} else {
				query.WriteString(fmt.Sprintf(`%s = %s`, column, columnsToValues[column]))
			}

			atLeastOneNonNullValue = true
		}
	}
	query.WriteString(")")

	// If there are only null values being inserted for each of the constrained columns,
	// then checking for uniqueness against other rows is not necessary.
	if !atLeastOneNonNullValue {
		return false, nil
	}

	queryString := query.String()

	// Check for uniqueness against other rows to be inserted. For simplicity, the `SELECT EXISTS`
	// query used to check for uniqueness against rows in the database can also
	// be used as a unique key to check for uniqueness among rows to be inserted.
	if _, duplicateEntry := previousRows[queryString]; duplicateEntry {
		return true, nil
	}
	previousRows[queryString] = true

	// Check for uniqueness against rows in the database.
	exists, err := scanBool(tx, queryString)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	return false, nil
}

func scanStringArrayRows(tx *pgx.Tx, query string, args ...interface{}) ([][]string, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := [][]string{}
	for rows.Next() {
		var columnNames []string
		err := rows.Scan(&columnNames)
		if err != nil {
			return nil, err
		}
		results = append(results, columnNames)
	}

	return results, err
}
