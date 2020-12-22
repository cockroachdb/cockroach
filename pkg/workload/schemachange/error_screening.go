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

func typeExists(tx *pgx.Tx, typ *tree.TypeName) (bool, error) {
	if !strings.Contains(typ.Object(), "enum") {
		return true, nil
	}

	return scanBool(tx, `SELECT EXISTS (
	SELECT ns.nspname, t.typname
  FROM pg_catalog.pg_namespace AS ns
  JOIN pg_catalog.pg_type AS t ON t.typnamespace = ns.oid
 WHERE ns.nspname = $1 AND t.typname = $2
	)`, typ.Schema(), typ.Object())
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

func indexExists(tx *pgx.Tx, tableName *tree.TableName, indexName string) (bool, error) {
	return scanBool(tx, `SELECT EXISTS(
			SELECT *
			  FROM information_schema.statistics
			 WHERE table_schema = $1
			   AND table_name = $2
			   AND index_name = $3
  )`, tableName.Schema(), tableName.Object(), indexName)
}

func columnsStoredInPrimaryIdx(
	tx *pgx.Tx, tableName *tree.TableName, columnNames tree.NameList,
) (bool, error) {
	columnsMap := map[string]bool{}
	for _, name := range columnNames {
		columnsMap[string(name)] = true
	}

	primaryColumns, err := scanStringArray(tx, `
	SELECT array_agg(column_name)
	  FROM (
	        SELECT DISTINCT column_name
	          FROM information_schema.statistics
	         WHERE index_name = 'primary'
	           AND table_schema = $1
	           AND table_name = $2
	       );
	`, tableName.Schema(), tableName.Object())

	if err != nil {
		return false, err
	}

	for _, primaryColumn := range primaryColumns {
		if _, exists := columnsMap[primaryColumn]; exists {
			return true, nil
		}
	}
	return false, nil
}

func scanStringArray(tx *pgx.Tx, query string, args ...interface{}) (b []string, err error) {
	err = tx.QueryRow(query, args...).Scan(&b)
	return b, err
}

// canApplyUniqueConstraint checks if the rows in a table are unique with respect
// to the specified columns such that a unique constraint can successfully be applied.
func canApplyUniqueConstraint(
	tx *pgx.Tx, tableName *tree.TableName, columns []string,
) (bool, error) {
	columnNames := strings.Join(columns, ", ")

	// If a row contains NULL in each of the columns relevant to a unique constraint,
	// then the row will always be unique to other rows with respect to the constraint
	// (even if there is another row with NULL values in each of the relevant columns).
	// To account for this, the whereNotNullClause below is constructed to ignore rows
	// with with NULL values in each of the relevant columns. Then, uniqueness can be
	// verified easily using a SELECT DISTINCT statement.
	whereNotNullClause := strings.Builder{}
	for idx, column := range columns {
		whereNotNullClause.WriteString(fmt.Sprintf("%s IS NOT NULL ", column))
		if idx != len(columns)-1 {
			whereNotNullClause.WriteString("OR ")
		}
	}

	return scanBool(tx,
		fmt.Sprintf(`
		SELECT (
	       SELECT count(*)
	         FROM (
	               SELECT DISTINCT %s
	                 FROM %s
	                WHERE %s
	              )
	      )
	      = (
	        SELECT count(*)
	          FROM %s
	         WHERE %s
	       );
	`, columnNames, tableName.String(), whereNotNullClause.String(), tableName.String(), whereNotNullClause.String()))

}

func columnContainsNull(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	return scanBool(tx, fmt.Sprintf(`SELECT EXISTS (
		SELECT %s
		  FROM %s
	   WHERE %s IS NULL
	)`, columnName, tableName.String(), columnName))
}

func constraintIsPrimary(
	tx *pgx.Tx, tableName *tree.TableName, constraintName string,
) (bool, error) {
	return scanBool(tx, fmt.Sprintf(`
	SELECT EXISTS(
	        SELECT *
	          FROM pg_catalog.pg_constraint
	         WHERE conrelid = '%s'::REGCLASS::INT
	           AND conname = '%s'
	           AND (contype = 'p')
	       );
	`, tableName.String(), constraintName))
}

// Checks if a column has a single unique constraint.
func columnHasSingleUniqueConstraint(
	tx *pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return scanBool(tx, `
	SELECT EXISTS(
	        SELECT column_name
	          FROM (
	                SELECT table_schema, table_name, column_name, ordinal_position,
	                       concat(table_schema,'.',table_name)::REGCLASS::INT8 AS tableid
	                  FROM information_schema.columns
	               ) AS cols
	          JOIN (
	                SELECT contype, conkey, conrelid
	                  FROM pg_catalog.pg_constraint
	               ) AS cons ON cons.conrelid = cols.tableid
	         WHERE table_schema = $1
	           AND table_name = $2
	           AND column_name = $3
	           AND (contype = 'u' OR contype = 'p')
	           AND array_length(conkey, 1) = 1
					   AND conkey[1] = ordinal_position
	       )
	`, tableName.Schema(), tableName.Object(), columnName)
}
func constraintIsUnique(
	tx *pgx.Tx, tableName *tree.TableName, constraintName string,
) (bool, error) {
	return scanBool(tx, fmt.Sprintf(`
	SELECT EXISTS(
	        SELECT *
	          FROM pg_catalog.pg_constraint
	         WHERE conrelid = '%s'::REGCLASS::INT
	           AND conname = '%s'
	           AND (contype = 'u')
	       );
	`, tableName.String(), constraintName))
}

func columnIsComputed(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	return scanBool(tx, `
     SELECT (
			 SELECT is_generated
				 FROM information_schema.columns
				WHERE table_schema = $1
				  AND table_name = $2
				  AND column_name = $3
            )
	         = 'YES'
`, tableName.Schema(), tableName.Object(), columnName)
}

func constraintExists(tx *pgx.Tx, constraintName string) (bool, error) {
	return scanBool(tx, fmt.Sprintf(`
	SELECT EXISTS(
	        SELECT *
	          FROM pg_catalog.pg_constraint
	           WHERE conname = '%s'
	       );
	`, constraintName))
}

func rowsSatisfyFkConstraint(
	tx *pgx.Tx,
	parentTable *tree.TableName,
	parentColumn *column,
	childTable *tree.TableName,
	childColumn *column,
) (bool, error) {
	// Self referential foreign key constraints are acceptable.
	if parentTable.Schema() == childTable.Schema() && parentTable.Object() == childTable.Object() && parentColumn.name == childColumn.name {
		return true, nil
	}
	return scanBool(tx, fmt.Sprintf(`
	SELECT NOT EXISTS(
	  SELECT *
	    FROM %s as t1
		  LEFT JOIN %s as t2
				     ON t1.%s = t2.%s
	   WHERE t2.%s IS NULL
  )`, childTable.String(), parentTable.String(), childColumn.name, parentColumn.name, parentColumn.name))
}

// violatesFkConstraints checks if the rows to be inserted will result in a foreign key violation.
func violatesFkConstraints(
	tx *pgx.Tx, tableName *tree.TableName, columns []string, rows [][]string,
) (bool, error) {
	fkConstraints, err := scanStringArrayRows(tx, fmt.Sprintf(`
		SELECT array[parent.table_schema, parent.table_name, parent.column_name, child.column_name]
		  FROM (
		        SELECT conkey, confkey, conrelid, confrelid
		          FROM pg_constraint
		         WHERE contype = 'f'
		           AND conrelid = '%s'::REGCLASS::INT8
		       ) AS con
		  JOIN (
		        SELECT column_name, ordinal_position, column_default
		          FROM information_schema.columns
		         WHERE table_schema = '%s'
		           AND table_name = '%s'
		       ) AS child ON conkey[1] = child.ordinal_position
		  JOIN (
		        SELECT pc.oid,
		               cols.table_schema,
		               cols.table_name,
		               cols.column_name,
		               cols.ordinal_position
		          FROM pg_class AS pc
		          JOIN pg_namespace AS pn ON pc.relnamespace = pn.oid
		          JOIN information_schema.columns AS cols ON (pc.relname = cols.table_name AND pn.nspname = cols.table_schema)
		       ) AS parent ON (
		                       con.confkey[1] = parent.ordinal_position
		                       AND con.confrelid = parent.oid
		                      )
		 WHERE child.column_name != 'rowid';
`, tableName.String(), tableName.Schema(), tableName.Object()))
	if err != nil {
		return false, err
	}

	// Maps a column name to its index. This way, the value of a column in a row can be looked up
	// using row[colToIndexMap["columnName"]] = "valueForColumn"
	columnNameToIndexMap := map[string]int{}
	for i, name := range columns {
		columnNameToIndexMap[name] = i
	}
	for _, row := range rows {
		for _, constraint := range fkConstraints {
			parentTableSchema := constraint[0]
			parentTableName := constraint[1]
			parentColumnName := constraint[2]
			childColumnName := constraint[3]

			// If self referential, there cannot be a violation.
			if parentTableSchema == tableName.Schema() && parentTableName == tableName.Object() && parentColumnName == childColumnName {
				continue
			}

			violation, err := violatesFkConstraintsHelper(tx, columnNameToIndexMap, parentTableSchema, parentTableName, parentColumnName, childColumnName, row)
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

// violatesFkConstraintsHelper checks if a single row will violate a foreign key constraint
// between the childColumn and parentColumn.
func violatesFkConstraintsHelper(
	tx *pgx.Tx,
	columnNameToIndexMap map[string]int,
	parentTableSchema, parentTableName, parentColumn, childColumn string,
	row []string,
) (bool, error) {

	// If the value to insert in the child column is NULL and the column default is NULL, then it is not possible to have a fk violation.
	childValue := row[columnNameToIndexMap[childColumn]]
	if childValue == "NULL" {
		return false, nil
	}

	return scanBool(tx, fmt.Sprintf(`
	SELECT NOT EXISTS (
	    SELECT * from %s.%s
	    WHERE %s = %s
	)
	`, parentTableSchema, parentTableName, parentColumn, childValue))
}
