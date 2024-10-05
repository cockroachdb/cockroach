// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (og *operationGenerator) tableExists(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) (bool, error) {
	return og.scanBool(ctx, tx, `SELECT EXISTS (
	SELECT table_name
    FROM information_schema.tables 
   WHERE table_schema = $1
     AND table_name = $2
   )`, tableName.Schema(), tableName.Object())
}

func (og *operationGenerator) viewExists(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) (bool, error) {
	return og.scanBool(ctx, tx, `SELECT EXISTS (
	SELECT table_name
    FROM information_schema.views 
   WHERE table_schema = $1
     AND table_name = $2
   )`, tableName.Schema(), tableName.Object())
}

func (og *operationGenerator) sequenceExists(
	ctx context.Context, tx pgx.Tx, seqName *tree.TableName,
) (bool, error) {
	return og.scanBool(ctx, tx, `SELECT EXISTS (
	SELECT sequence_name
    FROM information_schema.sequences
   WHERE sequence_schema = $1
     AND sequence_name = $2
   )`, seqName.Schema(), seqName.Object())
}

func (og *operationGenerator) columnExistsOnTable(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return og.scanBool(ctx, tx, `SELECT EXISTS (
	SELECT column_name
    FROM information_schema.columns 
   WHERE table_schema = $1
     AND table_name = $2
     AND column_name = $3
   )`, tableName.Schema(), tableName.Object(), columnName)
}

func (og *operationGenerator) tableHasRows(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) (bool, error) {
	return og.scanBool(ctx, tx, fmt.Sprintf(`SELECT EXISTS (SELECT * FROM %s)`, tableName.String()))
}

func (og *operationGenerator) scanInt(
	ctx context.Context, tx pgx.Tx, query string, args ...interface{},
) (i int, err error) {
	return Scan[int](ctx, og, tx, query, args...)
}

func (og *operationGenerator) scanBool(
	ctx context.Context, tx pgx.Tx, query string, args ...interface{},
) (b bool, err error) {
	return Scan[bool](ctx, og, tx, query, args...)
}

func (og *operationGenerator) schemaExists(
	ctx context.Context, tx pgx.Tx, schemaName string,
) (bool, error) {
	return og.scanBool(ctx, tx, `SELECT EXISTS (
	SELECT schema_name
		FROM information_schema.schemata
   WHERE schema_name = $1
	)`, schemaName)
}

func (og *operationGenerator) tableHasDependencies(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) (bool, error) {
	return og.scanBool(ctx, tx, `
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

func (og *operationGenerator) columnIsDependedOn(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	// To see if a column is depended on, the ordinal_position of the column is looked up in
	// information_schema.columns. Then, this position is used to see if that column has view dependencies
	// or foreign key dependencies which would be stored in crdb_internal.forward_dependencies and
	// pg_catalog.pg_constraint respectively.
	//
	// crdb_internal.forward_dependencies.dependedonby_details is an array of ordinal positions
	// stored as a list of numbers in a string, so SQL functions are used to parse these values
	// into arrays. unnest is used to flatten rows with this column of array type into multiple rows,
	// so performing unions and joins is easier.
	return og.scanBool(ctx, tx, `SELECT EXISTS(
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

// colIsRefByComputed determines if a column is referenced by a computed column.
func (og *operationGenerator) colIsRefByComputed(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return og.scanBool(ctx, tx, `SELECT EXISTS(
    SELECT
       attrelid::REGCLASS AS table_name,
       attname AS column_name,
       pg_get_expr(adbin, adrelid) AS computed_formula
    FROM
       pg_attribute
    JOIN
       pg_attrdef ON attrelid = adrelid AND attnum = adnum
    WHERE
       atthasdef
       AND attrelid = $1::REGCLASS
       AND pg_get_expr(adbin, adrelid) ILIKE '%%' || $2 || '%%'
)`, tableName.String(), columnName)
}

func (og *operationGenerator) columnIsDependedOnByView(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return og.scanBool(ctx, tx, `SELECT EXISTS(
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

func (og *operationGenerator) colIsPrimaryKey(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	primaryColumns, err := og.scanStringArray(ctx, tx,
		`
SELECT array_agg(column_name)
  FROM (
        SELECT DISTINCT column_name
          FROM information_schema.statistics
         WHERE index_name
               IN (
                  SELECT index_name
                    FROM crdb_internal.table_indexes
                   WHERE index_type = 'primary' AND descriptor_id = $3::REGCLASS
                )
               AND table_schema = $1
               AND table_name = $2
               AND storing = 'NO'
       );
	`, tableName.Schema(), tableName.Object(), tableName.String())
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

// exprColumnCollector collects all the columns observed inside
// an expression.
type exprColumnCollector struct {
	colInfo         map[string]column
	columnsObserved map[string]column
}

var _ tree.Visitor = &exprColumnCollector{}

// newExprColumnCollector constructs an expression collector, that
// will search for a set of columns.
func newExprColumnCollector(colInfo []column) *exprColumnCollector {
	collect := exprColumnCollector{
		colInfo:         make(map[string]column),
		columnsObserved: make(map[string]column),
	}
	for _, col := range colInfo {
		collect.colInfo[col.name] = col
	}
	return &collect
}

// VisitPost implements tree.Visitor
func (e *exprColumnCollector) VisitPost(expr tree.Expr) (newNode tree.Expr) {
	return expr
}

// VisitPre implements tree.Visitor
func (e *exprColumnCollector) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.ColumnItem:
		e.columnsObserved[t.ColumnName.String()] = e.colInfo[t.ColumnName.String()]
	case *tree.UnresolvedName:
		e.columnsObserved[t.String()] = e.colInfo[t.String()]
	}
	return true, expr
}

// valuesViolateUniqueConstraints determines if any unique constraints (including primary
// constraints and constraint expressions) that  will be violated upon inserting
// the specified rows into the specified table.
func (og *operationGenerator) valuesViolateUniqueConstraints(
	ctx context.Context,
	tx pgx.Tx,
	tableName *tree.TableName,
	columns []string,
	colInfo []column,
	rows [][]string,
) (bool, codesWithConditions, error) {
	var generatedCodes codesWithConditions
	constraints, err := og.scanStringArrayRows(ctx, tx, `
    WITH tab_json AS (
                    SELECT crdb_internal.pb_to_json(
                            'desc',
                            descriptor
                           )->'table' AS t
                      FROM system.descriptor
                     WHERE id = $1::REGCLASS
                  ),
         columns_json AS (
                        SELECT json_array_elements(t->'columns') AS c FROM tab_json
                      ),
         columns AS (
                    SELECT (c->>'id')::INT8 AS col_id,
                           IF(
                            (c->'inaccessible')::BOOL,
                            c->>'computeExpr',
                            c->>'name'
                           ) AS expr
                      FROM columns_json
                 ),
         indexes_json AS (
                         SELECT json_array_elements(t->'indexes') AS idx
                           FROM tab_json
                         UNION ALL SELECT t->'primaryIndex' FROM tab_json
                      ),
         unique_indexes AS (
                            SELECT idx->'name' AS name,
                                   json_array_elements(
                                    idx->'keyColumnIds'
                                   )::STRING::INT8 AS col_id
                              FROM indexes_json
                             WHERE (idx->'unique')::BOOL
                        ),
         index_exprs AS (
                        SELECT name, expr
                          FROM unique_indexes AS idx
                               INNER JOIN columns AS c ON idx.col_id = c.col_id
                     )
  SELECT ARRAY['(' || array_to_string(array_agg(expr), ', ') || ')'] AS final_expr
    FROM index_exprs
   WHERE expr != 'rowid'
GROUP BY name;
`, tableName.String())
	if err != nil {
		return false, nil, og.checkAndAdjustForUnknownSchemaErrors(err)
	}
	// Determine if the tuples are unique for a given constraint, where the index
	// will be the constraint.
	constraintTuples := make([]map[string]struct{}, 0, len(constraints))
	for range constraints {
		constraintTuples = append(constraintTuples, make(map[string]struct{}))
	}
	for _, row := range rows {
		hasGenerationError := false
		// Put values to be inserted into a column name to value map to simplify lookups.
		columnsToValues := map[string]string{}
		for i := 0; i < len(columns); i++ {
			columnsToValues[columns[i]] = row[i]
		}
		newCols := make(map[string]string)
		// Resolve any generated expressions, which have been validated earlier.
		for _, colInfo := range colInfo {
			if !colInfo.generated {
				continue
			}
			evalTxn, err := tx.Begin(ctx)
			if err != nil {
				return false, nil, err
			}
			newCols[colInfo.name], err = og.generateColumn(ctx, tx, colInfo, columnsToValues)
			if err != nil {
				if rbkErr := evalTxn.Rollback(ctx); rbkErr != nil {
					return false, nil, errors.WithSecondaryError(err, rbkErr)
				}
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) {
					return false, nil, err
				}
				// Only accept know error types for generated expressions.
				if !isValidGenerationError(pgErr.Code) {
					return false, nil, err
				}
				generatedCodes = append(generatedCodes,
					codesWithConditions{
						{code: pgcode.MakeCode(pgErr.Code), condition: true},
					}...,
				)
				hasGenerationError = true
				continue
			}
			err = evalTxn.Commit(ctx)
			if err != nil {
				return false, nil, err
			}
		}
		for k, v := range newCols {
			columnsToValues[k] = v
		}
		// Skip over constraint validation, since we know an expression is bad here.
		if hasGenerationError {
			continue
		}
		// Next validate the uniqueness of both constraints and index expressions.
		for constraintIdx, constraint := range constraints {
			nonTupleConstraint := constraint[0]
			if len(nonTupleConstraint) > 2 &&
				nonTupleConstraint[0] == '(' &&
				nonTupleConstraint[len(nonTupleConstraint)-1] == ')' {
				nonTupleConstraint = nonTupleConstraint[1 : len(nonTupleConstraint)-1]
			}
			hasNullsQuery := strings.Builder{}
			hasNullsQuery.WriteString("SELECT num_nulls(")
			hasNullsQuery.WriteString(nonTupleConstraint)
			hasNullsQuery.WriteString(") > 0 FROM (VALUES(")

			tupleSelectQuery := strings.Builder{}
			tupleSelectQuery.WriteString("SELECT array[(")
			tupleSelectQuery.WriteString(constraint[0])
			tupleSelectQuery.WriteString(")::STRING] FROM (VALUES(")

			query := strings.Builder{}
			columns := strings.Builder{}
			t, err := parser.ParseExpr(constraint[0])
			if err != nil {
				return false, nil, err
			}
			collector := newExprColumnCollector(colInfo)
			t.Walk(collector)
			query.WriteString("SELECT COUNT (*) > 0 FROM (SELECT * FROM ")
			query.WriteString(tableName.String())
			query.WriteString(" WHERE ")
			query.WriteString(constraint[0])
			query.WriteString("= ( SELECT ")
			query.WriteString(" ")
			query.WriteString(constraint[0])
			query.WriteString(" FROM (VALUES( ")
			colIdx := 0
			for col := range collector.columnsObserved {
				value := columnsToValues[col]
				if colIdx != 0 {
					query.WriteString(",")
					columns.WriteString(",")
					tupleSelectQuery.WriteString(",")
					hasNullsQuery.WriteString(",")
				}
				query.WriteString(value)
				columns.WriteString(col)
				hasNullsQuery.WriteString(value)
				tupleSelectQuery.WriteString(value)
				colIdx++
			}
			hasNullsQuery.WriteString(") ) AS T(")
			hasNullsQuery.WriteString(columns.String())
			hasNullsQuery.WriteString(")")
			tupleSelectQuery.WriteString(") ) AS T(")
			tupleSelectQuery.WriteString(columns.String())
			tupleSelectQuery.WriteString(")")
			query.WriteString(") ) AS T(")
			query.WriteString(columns.String())
			query.WriteString(") ) )")
			evalTxn, err := tx.Begin(ctx)
			if err != nil {
				return false, nil, err
			}
			// Detect if any null values exist.
			handleEvalTxnError := func(err error) (bool, error) {
				// No choice but to rollback, expression is malformed.
				rollbackErr := evalTxn.Rollback(ctx)
				if rollbackErr != nil {
					return false, errors.CombineErrors(err, rollbackErr)
				}
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) {
					return false, err
				}
				// Only accept known error types for generated expressions.
				if !isValidGenerationError(pgErr.Code) {
					return false, err
				}
				generatedCodes = append(generatedCodes,
					codesWithConditions{
						{code: pgcode.MakeCode(pgErr.Code), condition: true},
					}...,
				)
				return true, nil
			}
			hasNullValues, err := og.scanBool(ctx, evalTxn, hasNullsQuery.String())
			if err != nil {
				skipConstraint, err := handleEvalTxnError(err)
				if err != nil {
					return false, generatedCodes, err
				}
				if skipConstraint {
					continue
				}
			}
			// If it has null values, we are going to skip later on,
			// so skip this operation.
			var exists bool
			if !hasNullValues {
				exists, err = og.scanBool(ctx, evalTxn, query.String())
				if err != nil {
					skipConstraint, err := handleEvalTxnError(err)
					if err != nil {
						return false, generatedCodes, err
					}
					if skipConstraint {
						continue
					}
				}
			}
			err = evalTxn.Commit(ctx)
			if err != nil {
				return false, nil, err
			}
			// Proceed to the next constraint if it has NULL values.
			if hasNullValues {
				continue
			}
			if exists {
				return true, nil, nil
			}
			// Gather the tuples and check if it's unique.
			values, err := og.scanStringArrayNullableRows(ctx, tx, tupleSelectQuery.String())
			if err != nil {
				return false, nil, err
			}
			var value string
			if values[0][0] != nil {
				value = *values[0][0]
				if _, ok := constraintTuples[constraintIdx][value]; ok {
					return true, nil, nil
				}
				constraintTuples[constraintIdx][value] = struct{}{}
			}
		}
	}
	return false, generatedCodes, nil
}

// ErrSchemaChangesDisallowedDueToPkSwap is generated when schema changes are
// disallowed on a table because PK swap is already in progress.
var ErrSchemaChangesDisallowedDueToPkSwap = errors.New("not schema changes allowed on selected table due to PK swap")

func (og *operationGenerator) tableHasPrimaryKeySwapActive(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) error {

	indexName, err := og.scanStringArray(
		ctx,
		tx,
		`
SELECT array_agg(index_name)
  FROM (
SELECT
	index_name
FROM
	crdb_internal.table_indexes
WHERE
	index_type = 'primary'
	AND descriptor_id = $1::REGCLASS
       );
	`, tableName.String(),
	)
	if err != nil {
		return err
	}

	isIndexDropping, err := og.scanBool(
		ctx,
		tx,
		`
SELECT count(*) > 0
  FROM crdb_internal.schema_changes
 WHERE type = 'INDEX'
       AND table_id = $1::REGCLASS
       AND  target_name = $2
       AND direction = 'DROP';
`,
		tableName.String(),
		indexName[0],
	)
	if err != nil {
		return err
	}
	if isIndexDropping {
		return ErrSchemaChangesDisallowedDueToPkSwap
	}
	return nil
}

func getValidGenerationErrors() errorCodeSet {
	return errorCodeSet{
		pgcode.NumericValueOutOfRange:    struct{}{},
		pgcode.FloatingPointException:    struct{}{},
		pgcode.InvalidTextRepresentation: struct{}{},
	}
}

// isValidGenerationError these codes can be observed when evaluating values
// for generated expressions. These are errors are not ignored, but added into
// the expected set of errors.
func isValidGenerationError(code string) bool {
	pgCode := pgcode.MakeCode(code)
	return getValidGenerationErrors().contains(pgCode)
}

// validateGeneratedExpressionsForInsert goes through generated expressions and
// detects if a valid value can be generated with a given insert row.
func (og *operationGenerator) validateGeneratedExpressionsForInsert(
	ctx context.Context,
	tx pgx.Tx,
	tableName *tree.TableName,
	columns []string,
	colInfos []column,
	row []string,
) (bool, codesWithConditions, codesWithConditions, error) {
	var expectedErrors codesWithConditions
	var potentialErrors codesWithConditions
	// Put values to be inserted into a column name to value map to simplify lookups.
	columnsToValues := map[string]string{}
	for i := 0; i < len(columns); i++ {
		columnsToValues[columns[i]] = row[i]
	}
	nullViolationAdded := false
	validateExpression := func(expr string, typ string, isNullable bool, addGenerated bool) error {
		evalTx, err := tx.Begin(ctx)
		if err != nil {
			return err
		}
		query := strings.Builder{}
		query.WriteString("SELECT ((")
		query.WriteString(expr)
		query.WriteString(")::")
		query.WriteString(typ)
		query.WriteString(") IS NULL ")
		query.WriteString("AS c FROM ( VALUES(")
		// Second builder to ensure that no evaluating arithmetic doesn't lead
		// to overflows, if the order during runtime is different.
		queryEvalOrderCheck := strings.Builder{}
		queryEvalOrderCheck.WriteString(query.String())
		cols := strings.Builder{}
		colIdx := 0
		for colName, value := range columnsToValues {
			if colIdx != 0 {
				query.WriteString(",")
				queryEvalOrderCheck.WriteString(",")
				cols.WriteString(",")
			}
			nonNullValue := value
			if value == "NULL" {
				if colInfos[colIdx].typ.IsNumeric() {
					// We intentionally use NULL in case any division operations are encountered.
					// This reduces odds of extra overflows, but these will be evaluated as
					// potential errors not expected ones.
					nonNullValue = fmt.Sprintf("1::%s", colInfos[colIdx].typ.SQLString())
				}
			}
			query.WriteString(value)
			queryEvalOrderCheck.WriteString(nonNullValue)
			cols.WriteString(colName)
			colIdx++
		}

		if addGenerated {
			for _, colInfo := range colInfos {
				if !colInfo.generated {
					continue
				}
				col, err := og.generateColumn(ctx, tx, colInfo, columnsToValues)
				if err != nil {
					return err
				}
				if colIdx != 0 {
					query.WriteString(",")
					queryEvalOrderCheck.WriteString(",")
					cols.WriteString(",")
				}
				query.WriteString(col)
				queryEvalOrderCheck.WriteString(col)
				cols.WriteString(colInfo.name)
				colIdx++
			}
		}
		query.WriteString(")) AS t(")
		query.WriteString(cols.String())
		query.WriteString(");")
		queryEvalOrderCheck.WriteString(")) AS t(")
		queryEvalOrderCheck.WriteString(cols.String())
		queryEvalOrderCheck.WriteString(");")
		isNull, err := og.scanBool(ctx, evalTx, query.String())
		// Evaluating the expression generated a value, which can be either arithmetic
		// or overflow errors.
		if err != nil {
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				rbkErr := evalTx.Rollback(ctx)
				return errors.CombineErrors(err, rbkErr)
			}
			if !isValidGenerationError(pgErr.Code) {
				return err
			}
			expectedErrors = expectedErrors.append(pgcode.MakeCode(pgErr.Code))
		}
		if isNull && !isNullable && !nullViolationAdded {
			nullViolationAdded = true
			expectedErrors = expectedErrors.append(pgcode.NotNullViolation)
		}
		// Re-run the another variant in case we have NULL values in arithmetic
		// of expression, the evaluation order can differ depending on how variables
		// get bound during the actual insert.
		if err == nil && isNull {
			if _, err := og.scanBool(ctx, evalTx, queryEvalOrderCheck.String()); err != nil {
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) {
					rbkErr := evalTx.Rollback(ctx)
					return errors.CombineErrors(err, rbkErr)
				}
				// Note: Invalid errors are allowed, since this is a heuristic. We replaced
				// random NULL values with zero.
				if isValidGenerationError(pgErr.Code) {
					potentialErrors = potentialErrors.append(pgcode.MakeCode(pgErr.Code))
				}
			}
		}
		// Always rollback the context used to validate the expression, so the
		// main transaction doesn't stall.
		err = evalTx.Rollback(ctx)
		if err != nil {
			return err
		}
		return nil
	}

	// Loop over all columns that are generated and validate we run into no errors
	// evaluating them.
	for _, colInfo := range colInfos {
		if !colInfo.generated {
			continue
		}
		err := validateExpression(colInfo.generatedExpression, colInfo.typ.SQLString(), colInfo.nullable, false)
		if err != nil {
			return false, nil, nil, err
		}
	}
	// Any bad generated expression means we don't have to bother with indexes next,
	// since we expect the insert to fail earlier.
	if expectedErrors == nil {
		// Validate unique constraint expressions that are backed by indexes.
		constraints, err := og.scanStringArrayRows(ctx, tx, `
WITH tab_json AS (
                    SELECT crdb_internal.pb_to_json(
                            'desc',
                            descriptor
                           )->'table' AS t
                      FROM system.descriptor
                     WHERE id = $1::REGCLASS
                  ),
         columns_json AS (
                        SELECT json_array_elements(t->'columns') AS c FROM tab_json
                      ),
         columns AS (
                    SELECT (c->>'id')::INT8 AS col_id,
                           IF(
                            (c->'inaccessible')::BOOL,
                            c->>'computeExpr',
                            c->>'name'
                           ) AS expr
                      FROM columns_json
                 ),
         indexes_json AS (
                         SELECT json_array_elements(t->'indexes') AS idx
                           FROM tab_json
                         UNION ALL SELECT t->'primaryIndex' FROM tab_json
                      ),
         unique_indexes AS (
                            SELECT idx->'name' AS name,
                                   json_array_elements(
                                    idx->'keyColumnIds'
                                   )::STRING::INT8 AS col_id
                              FROM indexes_json
                        ),
         index_exprs AS (
                        SELECT name, expr
                          FROM unique_indexes AS idx
                               INNER JOIN columns AS c ON idx.col_id = c.col_id
                     )
  SELECT ARRAY['(' || array_to_string(array_agg(expr), ', ') || ')'] AS final_expr
    FROM index_exprs
   WHERE expr != 'rowid'
GROUP BY name;
		`, tableName.String())
		if err != nil {
			return false, nil, nil, og.checkAndAdjustForUnknownSchemaErrors(err)
		}

		for _, constraint := range constraints {
			err := validateExpression(constraint[0], "STRING", true, true)
			if err != nil {
				return false, nil, nil, err
			}
		}
	}
	return len(expectedErrors) > 0, expectedErrors, potentialErrors, nil
}

// generateColumn generates values for columns that are generated.
func (og *operationGenerator) generateColumn(
	ctx context.Context, tx pgx.Tx, colInfo column, columnsToValues map[string]string,
) (string, error) {
	if !colInfo.generated {
		return "", errors.AssertionFailedf("column is not generated: %v", colInfo.name)
	}
	// Adjust floating point precision, so that precision matches the one used
	// by cockroach internally.
	_, err := tx.Exec(ctx, " set extra_float_digits=3;")
	if err != nil {
		return "", err
	}
	query := strings.Builder{}
	query.WriteString("SELECT array[(")
	query.WriteString(colInfo.generatedExpression)
	query.WriteString(")::")
	query.WriteString(colInfo.typ.SQLString())
	query.WriteString("::STRING] AS c FROM ( VALUES(")
	cols := strings.Builder{}
	colIdx := 0
	for colName, value := range columnsToValues {
		if colIdx != 0 {
			query.WriteString(",")
			cols.WriteString(",")
		}
		query.WriteString(value)
		cols.WriteString(colName)
		colIdx++
	}
	query.WriteString(")) AS t(")
	query.WriteString(cols.String())
	query.WriteString(");")
	val, err := og.scanStringArrayNullableRows(ctx, tx, query.String())
	if err != nil {
		return "", err
	}
	if len(val) > 0 && val[0][0] != nil {
		if colInfo.typ.Family() == types.StringFamily {
			str := tree.AsStringWithFlags(tree.NewDString(*val[0][0]), tree.FmtParsable)
			return str, nil
		}
		return fmt.Sprintf("'" + *val[0][0] + "'::" + colInfo.typ.SQLString()), nil
	}
	return "NULL", nil
}

func (og *operationGenerator) scanStringArrayNullableRows(
	ctx context.Context, tx pgx.Tx, query string, args ...interface{},
) ([][]*string, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "scanStringArrayNullableRows: %q %q", query, args)
	}
	defer rows.Close()

	var results [][]*string
	for rows.Next() {
		var columnNames []*string
		err := rows.Scan(&columnNames)
		if err != nil {
			return nil, errors.Wrapf(err, "scan: %q, args %v, scanArgs %q", query, columnNames, args)
		}
		results = append(results, columnNames)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	{
		// Instead of having pointers within the log file, we are going to
		// dereference everything and convert NULLs properly.
		humanReadableResults := make([][]string, 0, len(results))
		for _, res := range results {
			humanReadableRes := make([]string, 0, len(res))
			for _, col := range res {
				colWithNullStr := "NULL"
				if col != nil {
					colWithNullStr = *col
				}
				humanReadableRes = append(humanReadableRes, colWithNullStr)
			}
			humanReadableResults = append(humanReadableResults, humanReadableRes)
		}
		og.LogQueryResults(
			query,
			humanReadableResults,
			args...)
	}
	return results, nil
}

func (og *operationGenerator) scanStringArrayRows(
	ctx context.Context, tx pgx.Tx, query string, args ...interface{},
) ([][]string, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "scanStringArrayRows: %q %q", query, args)
	}
	defer rows.Close()

	var results [][]string
	for rows.Next() {
		var columnNames []string
		err := rows.Scan(&columnNames)
		if err != nil {
			return nil, errors.Wrapf(err, "scan: %q, args %q, scanArgs %q", query, columnNames, args)
		}
		results = append(results, columnNames)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	og.LogQueryResults(
		query,
		results,
		args...)
	return results, nil
}

func (og *operationGenerator) indexExists(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, indexName string,
) (bool, error) {
	return og.scanBool(ctx, tx, `SELECT EXISTS(
			SELECT *
			  FROM information_schema.statistics
			 WHERE table_schema = $1
			   AND table_name = $2
			   AND index_name = $3
  )`, tableName.Schema(), tableName.Object(), indexName)
}

func (og *operationGenerator) scanStringArray(
	ctx context.Context, tx pgx.Tx, query string, args ...interface{},
) (b []string, err error) {
	err = tx.QueryRow(ctx, query, args...).Scan(&b)
	if err == nil {
		og.LogQueryResults(
			query,
			b,
			args...,
		)
	}
	return b, errors.Wrapf(err, "scanStringArray %q %q", query, args)
}

// canApplyUniqueConstraint checks if the rows in a table are unique with respect
// to the specified columns such that a unique constraint can successfully be applied.
func (og *operationGenerator) canApplyUniqueConstraint(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columns []string,
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

	return og.scanBool(ctx, tx,
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

func (og *operationGenerator) columnContainsNull(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return og.scanBool(ctx, tx, fmt.Sprintf(`SELECT EXISTS (
		SELECT %s
		  FROM %s
	   WHERE %s IS NULL
	)`, lexbase.EscapeSQLIdent(columnName), tableName.String(), lexbase.EscapeSQLIdent(columnName)))
}

func (og *operationGenerator) constraintIsPrimary(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, constraintName string,
) (bool, error) {
	return og.scanBool(ctx, tx, fmt.Sprintf(`
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
func (og *operationGenerator) columnHasSingleUniqueConstraint(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	// Rowid will always be unique, though the index is hidden.
	if columnName == "rowid" {
		return true, nil
	}
	return og.scanBool(ctx, tx, `
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
func (og *operationGenerator) constraintIsUnique(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, constraintName string,
) (bool, error) {
	return og.scanBool(ctx, tx, fmt.Sprintf(`
	SELECT EXISTS(
	        SELECT *
	          FROM pg_catalog.pg_constraint
	         WHERE conrelid = '%s'::REGCLASS::INT
	           AND conname = '%s'
	           AND (contype = 'u')
	       );
	`, tableName.String(), constraintName))
}

func (og *operationGenerator) columnIsStoredComputed(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	// Note that we COALESCE because the column may not exist.
	return og.scanBool(ctx, tx, `
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

func (og *operationGenerator) columnIsVirtualComputed(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	// Note that we COALESCE because the column may not exist.
	return og.scanBool(ctx, tx, `
SELECT COALESCE(
        (
            SELECT attgenerated
              FROM pg_catalog.pg_attribute
             WHERE attrelid = $1:::REGCLASS AND attname = $2
        )
        = 'v',
        false
       );
`, tableName.String(), columnName)
}

func (og *operationGenerator) constraintExists(
	ctx context.Context, tx pgx.Tx, constraintName string,
) (bool, error) {
	return og.scanBool(ctx, tx, fmt.Sprintf(`
	SELECT EXISTS(
	        SELECT *
	          FROM pg_catalog.pg_constraint
	           WHERE conname = '%s'
	       );
	`, constraintName))
}

func (og *operationGenerator) rowsSatisfyFkConstraint(
	ctx context.Context,
	tx pgx.Tx,
	parentTable *tree.TableName,
	parentColumn *column,
	childTable *tree.TableName,
	childColumn *column,
) (bool, error) {
	// Self referential foreign key constraints are acceptable.
	selfReferential, err := og.scanBool(ctx, tx,
		`SELECT $1:::REGCLASS=$2:::REGCLASS`,
		parentTable.String(), childTable.String())
	if err != nil {
		return false, err
	}
	if selfReferential && parentColumn.name == childColumn.name {
		return true, nil
	}

	// Validate the parent table has rows.
	childRows, err := og.scanInt(ctx, tx,
		fmt.Sprintf(`
SELECT count(*) FROM %s
		`, childTable.String()),
	)
	if err != nil {
		return false, err
	}

	// If child table is empty then no violation can exist.
	if childRows == 0 {
		return true, nil
	}

	numJoinRows, err := og.scanInt(ctx, tx, fmt.Sprintf(`
	  SELECT count(*)
	    FROM %s as t1
		  LEFT JOIN %s as t2
				     ON t1.%s = t2.%s
			WHERE t2.%s IS NOT NULL
`, childTable.String(), parentTable.String(), childColumn.name, parentColumn.name, parentColumn.name))
	if err != nil {
		return false, err
	}
	return numJoinRows == childRows, err
}

var (
	// regexpUnknownSchemaErr matches unknown schema errors with
	// a descriptor ID, which will have the form: unknown schema "[123]"
	regexpUnknownSchemaErr = regexp.MustCompile(`unknown schema "\[\d+]"`)
)

// checkAndAdjustForUnknownSchemaErrors in certain contexts we will attempt to
// bind descriptors without leasing them, since we are using crdb_internal tables,
// so it's possible for said descriptor to be dropped before we bind it. This
// method will allow for "unknown schema [xx]" in those contexts.
func (og *operationGenerator) checkAndAdjustForUnknownSchemaErrors(err error) error {
	if pgErr := new(pgconn.PgError); errors.As(err, &pgErr) &&
		pgcode.MakeCode(pgErr.Code) == pgcode.InvalidSchemaName {
		if regexpUnknownSchemaErr.MatchString(pgErr.Message) {
			og.LogMessage(fmt.Sprintf("Rolling back due to unknown schema error %v",
				err))
			// Force a rollback and log inside the operation generator.
			return errors.Mark(err, errRunInTxnRbkSentinel)
		}
	}
	return err
}

// violatesFkConstraints checks if the rows to be inserted will result in a foreign key violation.
func (og *operationGenerator) violatesFkConstraints(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columns []string, rows [][]string,
) (bool, error) {
	fkConstraints, err := og.scanStringArrayRows(ctx, tx, fmt.Sprintf(`
		SELECT array[parent.table_schema, parent.table_name, parent.column_name, child.column_name]
		  FROM (
		        SELECT conname, conkey, confkey, conrelid, confrelid
		          FROM pg_constraint
		         WHERE contype = 'f'
		           AND conrelid = '%s'::REGCLASS::INT8
		       ) AS con
			JOIN ( SELECT CONSTRAINT_NAME from information_schema.table_constraints 
				      WHERE table_schema ='%s' AND
								    table_name='%s' AND
										(crdb_internal.is_constraint_active('%s', constraint_name) = true)
           ) AS tc ON conname = tc.CONSTRAINT_NAME
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
`, tableName.String(), tableName.Schema(), tableName.Object(), tableName.String(), tableName.Schema(), tableName.Object()))
	if err != nil {
		return false, og.checkAndAdjustForUnknownSchemaErrors(err)
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
			parentAndChildAreSame := parentTableSchema == tableName.Schema() && parentTableName == tableName.Object()
			if parentAndChildAreSame && parentColumnName == childColumnName {
				continue
			}

			violation, err := og.violatesFkConstraintsHelper(
				ctx, tx, columnNameToIndexMap, parentTableSchema, parentTableName, parentColumnName, tableName.String(), childColumnName, parentAndChildAreSame, row, rows,
			)
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
func (og *operationGenerator) violatesFkConstraintsHelper(
	ctx context.Context,
	tx pgx.Tx,
	columnNameToIndexMap map[string]int,
	parentTableSchema, parentTableName, parentColumn, childTableName, childColumn string,
	parentAndChildAreSameTable bool,
	row []string,
	allRows [][]string,
) (bool, error) {

	// If the value to insert in the child column is NULL and the column default is NULL, then it is not possible to have a fk violation.
	childValue := row[columnNameToIndexMap[childColumn]]
	if childValue == "NULL" {
		return false, nil
	}
	// If the parent and child are the same table, then any rows in an existing
	// insert may satisfy the same constraint.
	var parentAndChildSameQueryColumns []string
	if parentAndChildAreSameTable {
		colsInfo, err := og.getTableColumns(ctx, tx, childTableName, false)
		if err != nil {
			return false, err
		}
		// Put values to be inserted into a column name to value map to simplify lookups.
		columnsToValues := map[string]string{}
		for name, idx := range columnNameToIndexMap {
			columnsToValues[name] = row[idx]
		}
		colIdx := 0
		for idx, colInfo := range colsInfo {
			if colInfo.name == parentColumn {
				colIdx = idx
				break
			}
		}
		for _, otherRow := range allRows {
			parentValueInSameInsert := otherRow[columnNameToIndexMap[parentColumn]]
			// If the parent column is generated, spend time to generate the value.
			if colsInfo[colIdx].generated {
				var err error
				parentValueInSameInsert, err = og.generateColumn(ctx, tx, colsInfo[colIdx], columnsToValues)
				if err != nil {
					return false, err
				}
			}
			// Skip over NULL values.
			if parentValueInSameInsert == "NULL" {
				continue
			}
			parentAndChildSameQueryColumns = append(parentAndChildSameQueryColumns,
				fmt.Sprintf("%s = %s", parentValueInSameInsert, childValue))
		}
	}
	checkSharedParentChildRows := ""
	if len(parentAndChildSameQueryColumns) > 0 {
		checkSharedParentChildRows = fmt.Sprintf("false = ANY (ARRAY [%s]) AND",
			strings.Join(parentAndChildSameQueryColumns, ","))
	}
	return og.scanBool(ctx, tx, fmt.Sprintf(`
	    SELECT %s count(*) = 0 from %s.%s
	    WHERE %s = (%s)
	`,
		checkSharedParentChildRows, parentTableSchema, parentTableName, parentColumn, childValue))
}

func (og *operationGenerator) columnIsInDroppingIndex(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return og.scanBool(ctx, tx, `
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

func (og *operationGenerator) constraintInDroppingState(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, constraintName string,
) (bool, error) {
	// TODO(ajwerner): Figure out how to plumb the column name into this query.
	return og.scanBool(ctx, tx, `
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

func (og *operationGenerator) columnNotNullConstraintInMutation(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName, columnName string,
) (bool, error) {
	return og.scanBool(ctx, tx, `
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

func (og *operationGenerator) schemaContainsTypesWithCrossSchemaReferences(
	ctx context.Context, tx pgx.Tx, schemaName string,
) (bool, error) {
	return og.scanBool(ctx, tx, `
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
func (og *operationGenerator) enumMemberPresent(
	ctx context.Context, tx pgx.Tx, enum string, val string,
) (bool, error) {
	return og.scanBool(ctx, tx, `
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
func (og *operationGenerator) tableHasOngoingSchemaChanges(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) (bool, error) {
	return og.scanBool(
		ctx,
		tx,
		`
SELECT
	json_array_length(
		COALESCE(
			crdb_internal.pb_to_json(
				'cockroach.sql.sqlbase.Descriptor',
				descriptor
			)->'table'->'mutations',
			'[]'
		)
	)
	> 0
FROM
	system.descriptor
WHERE
	id = $1::REGCLASS;
		`,
		tableName.String(),
	)
}

// tableHasOngoingAlterPKSchemaChanges checks whether a given table has an ALTER
// PRIMARY KEY related change in progress.
func (og *operationGenerator) tableHasOngoingAlterPKSchemaChanges(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) (bool, error) {
	return og.scanBool(
		ctx,
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
func (og *operationGenerator) getRegionColumn(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) (string, error) {
	isTableRegionalByRow, err := og.tableIsRegionalByRow(ctx, tx, tableName)
	if err != nil {
		return "", err
	}
	if !isTableRegionalByRow {
		return "", errors.AssertionFailedf(
			"invalid call to get region column of table %s which is not a REGIONAL BY ROW table",
			tableName.String())
	}

	regionCol, err := Scan[string](ctx, og, tx, `
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
func (og *operationGenerator) tableIsRegionalByRow(
	ctx context.Context, tx pgx.Tx, tableName *tree.TableName,
) (bool, error) {
	return og.scanBool(
		ctx,
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

// databaseHasMultiRegion determines whether the database is multi-region
// enabled.
func (og *operationGenerator) databaseIsMultiRegion(ctx context.Context, tx pgx.Tx) (bool, error) {
	return og.scanBool(
		ctx,
		tx,
		`SELECT EXISTS (SELECT * FROM [SHOW REGIONS FROM DATABASE])`,
	)
}

// databaseHasRegionChange determines whether the database is currently undergoing
// a region change.
func (og *operationGenerator) databaseHasRegionChange(
	ctx context.Context, tx pgx.Tx,
) (bool, error) {
	isMultiRegion, err := og.scanBool(
		ctx,
		tx,
		`SELECT EXISTS (SELECT * FROM [SHOW REGIONS FROM DATABASE])`,
	)
	if err != nil || !isMultiRegion {
		return false, err
	}
	return og.scanBool(
		ctx,
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
func (og *operationGenerator) databaseHasRegionalByRowChange(
	ctx context.Context, tx pgx.Tx,
) (bool, error) {
	return og.scanBool(
		ctx,
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

// databaseHasTablesWithPartitioning detects if any of the tables have partitioning
// on them already.
func (og *operationGenerator) databaseHasTablesWithPartitioning(
	ctx context.Context, tx pgx.Tx, database string,
) (bool, error) {
	return og.scanBool(ctx,
		tx,
		fmt.Sprintf(`SELECT count(*)> 0 FROM %s.crdb_internal.partitions`,
			database),
	)
}
