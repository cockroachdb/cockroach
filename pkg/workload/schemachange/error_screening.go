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
	"github.com/cockroachdb/errors"
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

func tableHasRows(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	return scanBool(tx, fmt.Sprintf(`SELECT EXISTS (SELECT * FROM %s)`, tableName.String()))
}

func scanBool(tx *pgx.Tx, query string, args ...interface{}) (b bool, err error) {
	err = tx.QueryRow(query, args...).Scan(&b)
	return b, errors.Wrapf(err, "scanBool: %q %q", query, args)
}

func scanString(tx *pgx.Tx, query string, args ...interface{}) (s string, err error) {
	err = tx.QueryRow(query, args...).Scan(&s)
	return s, errors.Wrapf(err, "scanString: %q %q", query, args)
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
           AND fd.descriptor_id != fd.dependedonby_id
           AND fd.dependedonby_type != 'sequence'
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
                    AND fd.dependedonby_type != 'sequence'
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
	primaryColumns, err := scanStringArray(tx,
		`SELECT array_agg(column_name)
		FROM (
			SELECT DISTINCT column_name
				FROM information_schema.statistics
			WHERE index_name = 'primary'
				AND table_schema = $1
				AND table_name = $2
				AND storing = 'NO'
		);
	`, tableName.Schema(), tableName.Object())
	if err != nil {
		return false, err
	}

	for _, primaryColumn := range primaryColumns {
		if primaryColumn == columnName {
			return true, nil
		}
	}
	return false, nil
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
		return nil, errors.Wrapf(err, "scanStringArrayRows: %q %q", query, args)
	}
	defer rows.Close()

	results := [][]string{}
	for rows.Next() {
		var columnNames []string
		err := rows.Scan(&columnNames)
		if err != nil {
			return nil, errors.Wrapf(err, "scan: %q, args %q, scanArgs %q", query, columnNames, args)
		}
		results = append(results, columnNames)
	}

	return results, nil
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

func scanStringArray(tx *pgx.Tx, query string, args ...interface{}) (b []string, err error) {
	err = tx.QueryRow(query, args...).Scan(&b)
	return b, errors.Wrapf(err, "scanStringArray %q %q", query, args)
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

func columnIsStoredComputed(
	tx *pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	// Note that we COALESCE because the column may not exist.
	return scanBool(tx, `
SELECT COALESCE(
        (
            SELECT attgenerated
              FROM pg_catalog.pg_attribute
             WHERE attrelid = $1:::REGCLASS AND attname = $2
        )
        = 's',
        false
       );
`, tableName.String(), columnName)
}

func columnIsComputed(tx *pgx.Tx, tableName *tree.TableName, columnName string) (bool, error) {
	// Note that we COALESCE because the column may not exist.
	return scanBool(tx, `
SELECT COALESCE(
        (
            SELECT attgenerated
              FROM pg_catalog.pg_attribute
             WHERE attrelid = $1:::REGCLASS AND attname = $2
        )
        != '',
        false
       );
`, tableName.String(), columnName)
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

func columnIsInDroppingIndex(
	tx *pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return scanBool(tx, `
SELECT EXISTS(
        SELECT index_id
          FROM (
                SELECT DISTINCT index_id
                  FROM crdb_internal.index_columns
                 WHERE descriptor_id = $1::REGCLASS AND column_name = $2
               ) AS indexes
          JOIN crdb_internal.schema_changes AS sc ON sc.target_id
                                                     = indexes.index_id
                                                 AND table_id = $1::REGCLASS
                                                 AND type = 'INDEX'
                                                 AND direction = 'DROP'
       );
`, tableName.String(), columnName)
}

// A pair of CTE definitions that expect the first argument to be a table name.
const descriptorsAndConstraintMutationsCTE = `descriptors AS (
                    SELECT crdb_internal.pb_to_json(
                            'cockroach.sql.sqlbase.Descriptor',
                            descriptor
                           )->'table' AS d
                      FROM system.descriptor
                     WHERE id = $1::REGCLASS
                   ),
       constraint_mutations AS (
                                SELECT mut
                                  FROM (
                                        SELECT json_array_elements(
                                                d->'mutations'
                                               ) AS mut
                                          FROM descriptors
                                       )
                                 WHERE (mut->'constraint') IS NOT NULL
                            )`

func constraintInDroppingState(
	tx *pgx.Tx, tableName *tree.TableName, constraintName string,
) (bool, error) {
	// TODO(ajwerner): Figure out how to plumb the column name into this query.
	return scanBool(tx, `
  WITH `+descriptorsAndConstraintMutationsCTE+`
SELECT true
       IN (
            SELECT (t.f).value @> json_set('{"validity": "Dropping"}', ARRAY['name'], to_json($2:::STRING))
              FROM (
                    SELECT json_each(mut->'constraint') AS f
                      FROM constraint_mutations
                   ) AS t
        );
`, tableName.String(), constraintName)
}

func columnNotNullConstraintInMutation(
	tx *pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return scanBool(tx, `
  WITH `+descriptorsAndConstraintMutationsCTE+`,
       col AS (
            SELECT (c->>'id')::INT8 AS id
              FROM (
                    SELECT json_array_elements(d->'columns') AS c
                      FROM descriptors
                   )
             WHERE c->>'name' = $2
           )
SELECT EXISTS(
        SELECT *
          FROM constraint_mutations
          JOIN col ON mut->'constraint'->>'constraintType' = 'NOT_NULL'
                  AND (mut->'constraint'->>'notNullColumn')::INT8 = id
       );
`, tableName.String(), columnName)
}

func schemaContainsTypesWithCrossSchemaReferences(tx *pgx.Tx, schemaName string) (bool, error) {
	return scanBool(tx, `
  WITH database_id AS (
                    SELECT id
                      FROM system.namespace
                     WHERE "parentID" = 0
                       AND "parentSchemaID" = 0
                       AND name = current_database()
                   ),
       schema_id AS (
                    SELECT nsp.id
                      FROM system.namespace AS nsp
                      JOIN database_id ON "parentID" = database_id.id
                                      AND "parentSchemaID" = 0
                                      AND name = $1
                 ),
       descriptor_ids AS (
                        SELECT nsp.id
                          FROM system.namespace AS nsp,
                               schema_id,
                               database_id
                         WHERE nsp."parentID" = database_id.id
                           AND nsp."parentSchemaID" = schema_id.id
                      ),
       descriptors AS (
                    SELECT crdb_internal.pb_to_json(
                            'cockroach.sql.sqlbase.Descriptor',
                            descriptor
                           ) AS descriptor
                      FROM system.descriptor AS descriptors
                      JOIN descriptor_ids ON descriptors.id
                                             = descriptor_ids.id
                   ),
       types AS (
                SELECT descriptor
                  FROM descriptors
                 WHERE (descriptor->'type') IS NOT NULL
             ),
       table_references AS (
                            SELECT json_array_elements(
                                    descriptor->'table'->'dependedOnBy'
                                   ) AS ref
                              FROM descriptors
                             WHERE (descriptor->'table') IS NOT NULL
                        ),
       dependent AS (
                    SELECT (ref->>'id')::INT8 AS id FROM table_references
                 ),
       referenced_descriptors AS (
                                SELECT json_array_elements_text(
                                        descriptor->'type'->'referencingDescriptorIds'
                                       )::INT8 AS id
                                  FROM types
                              )
SELECT EXISTS(
        SELECT *
          FROM system.namespace
         WHERE id IN (SELECT id FROM referenced_descriptors)
           AND "parentSchemaID" NOT IN (SELECT id FROM schema_id)
           AND id NOT IN (SELECT id FROM dependent)
       );`, schemaName)
}

// enumMemberPresent determines whether val is a member of the enum.
// This includes non-public members.
func enumMemberPresent(tx *pgx.Tx, enum string, val string) (bool, error) {
	return scanBool(tx, `
WITH enum_members AS (
	SELECT
				json_array_elements(
						crdb_internal.pb_to_json(
								'cockroach.sql.sqlbase.Descriptor',
								descriptor
						)->'type'->'enumMembers'
				)->>'logicalRepresentation'
				AS v
		FROM
				system.descriptor
		WHERE
				id = ($1::REGTYPE::INT8 - 100000)
)
SELECT
	CASE WHEN EXISTS (
		SELECT v FROM enum_members WHERE v = $2::string
	) THEN true
	ELSE false
	END AS exists
`,
		enum,
		val,
	)
}

// tableHasOngoingSchemaChanges returns whether the table has any mutations lined up.
func tableHasOngoingSchemaChanges(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	return scanBool(
		tx,
		`
		SELECT json_array_length(
        crdb_internal.pb_to_json(
            'cockroach.sql.sqlbase.Descriptor',
            descriptor
        )->'table'->'mutations'
       )
       > 0
		FROM system.descriptor
	  WHERE id = $1::REGCLASS
		`,
		tableName.String(),
	)
}

// tableHasOngoingAlterPKSchemaChanges checks whether a given table has an ALTER
// PRIMARY KEY related change in progress.
func tableHasOngoingAlterPKSchemaChanges(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	return scanBool(
		tx,
		`
WITH
	descriptors
		AS (
			SELECT
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor
				)->'table'
					AS d
			FROM
				system.descriptor
			WHERE
				id = $1::REGCLASS
		)
SELECT
	EXISTS(
		SELECT
			mut
		FROM
			(
				SELECT
					json_array_elements(d->'mutations')
						AS mut
				FROM
					descriptors
			)
		WHERE
			(mut->'primaryKeySwap') IS NOT NULL
	);
		`,
		tableName.String(),
	)
}

// getRegionColumn returns the column used for partitioning a REGIONAL BY ROW
// table. This column is either the tree.RegionalByRowRegionDefaultCol column,
// or the column specified in the AS clause. This function asserts if the
// supplied table is not REGIONAL BY ROW.
func getRegionColumn(tx *pgx.Tx, tableName *tree.TableName) (string, error) {
	isTableRegionalByRow, err := tableIsRegionalByRow(tx, tableName)
	if err != nil {
		return "", err
	}
	if !isTableRegionalByRow {
		return "", errors.AssertionFailedf(
			"invalid call to get region column of table %s which is not a REGIONAL BY ROW table",
			tableName.String())
	}

	regionCol, err := scanString(
		tx,
		`
WITH
	descriptors
		AS (
			SELECT
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor
				)->'table'
					AS d
			FROM
				system.descriptor
			WHERE
				id = $1::REGCLASS
		)
SELECT
	COALESCE (d->'localityConfig'->'regionalByRow'->>'as', $2)
FROM
	descriptors;
`,
		tableName.String(),
		tree.RegionalByRowRegionDefaultCol,
	)
	if err != nil {
		return "", err
	}

	return regionCol, nil
}

// tableIsRegionalByRow checks whether the given table is a REGIONAL BY ROW table.
func tableIsRegionalByRow(tx *pgx.Tx, tableName *tree.TableName) (bool, error) {
	return scanBool(
		tx,
		`
WITH
	descriptors
		AS (
			SELECT
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor
				)->'table'
					AS d
			FROM
				system.descriptor
			WHERE
				id = $1::REGCLASS
		)
SELECT
	EXISTS(
		SELECT
			1
		FROM
			descriptors
		WHERE
			d->'localityConfig'->'regionalByRow' IS NOT NULL
	);
		`,
		tableName.String(),
	)
}

// databaseHasRegionChange determines whether the database is currently undergoing
// a region change.
func databaseHasRegionChange(tx *pgx.Tx) (bool, error) {
	isMultiRegion, err := scanBool(
		tx,
		`SELECT EXISTS (SELECT * FROM [SHOW REGIONS FROM DATABASE])`,
	)
	if err != nil || (!isMultiRegion && err == nil) {
		return false, err
	}
	return scanBool(
		tx,
		`
WITH enum_members AS (
	SELECT
				json_array_elements(
						crdb_internal.pb_to_json(
								'cockroach.sql.sqlbase.Descriptor',
								descriptor
						)->'type'->'enumMembers'
				)
				AS v
		FROM
				system.descriptor
		WHERE
				id = ('public.crdb_internal_region'::REGTYPE::INT8 - 100000)
)
SELECT EXISTS (
	SELECT 1 FROM enum_members
	WHERE v->>'direction' <> 'NONE'
)
		`,
	)
}

// databaseHasRegionalByRowChange checks whether a given database has any tables
// which are currently undergoing a change to or from REGIONAL BY ROW, or
// REGIONAL BY ROW tables with schema changes on it.
func databaseHasRegionalByRowChange(tx *pgx.Tx) (bool, error) {
	return scanBool(
		tx,
		`
WITH
	descriptors
		AS (
			SELECT
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor
				)->'table'
					AS d
			FROM
				system.descriptor
			WHERE
				id IN (
					SELECT id FROM system.namespace
					WHERE "parentID" = (
						SELECT id FROM system.namespace
						WHERE name = (SELECT database FROM [SHOW DATABASE])
						AND "parentID" = 0
					) AND "parentSchemaID" <> 0
				)
		)
SELECT (
	EXISTS(
		SELECT
			mut
		FROM
			(
				-- no schema changes on regional by row tables
				SELECT
					json_array_elements(d->'mutations')
						AS mut
				FROM (
					SELECT
						d
					FROM
						descriptors
					WHERE
						d->'localityConfig'->'regionalByRow' IS NOT NULL
				)
			)
	) OR EXISTS (
		-- no primary key swaps in the current database
		SELECT mut FROM (
			SELECT
				json_array_elements(d->'mutations')
					AS mut
			FROM descriptors
		)
		WHERE
			(mut->'primaryKeySwap') IS NOT NULL
	)
);
		`,
	)
}
