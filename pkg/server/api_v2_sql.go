// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// swagger:operation POST /sql/ execSQL
//
// Execute one or more SQL statements
//
// Executes one or more SQL statements.
//
// If the execute parameter is not specified, only check the SQL
// syntax.
//
// If only one SQL statement is specified, it is executed using an
// implicit transaction.
//
// If multiple SQL statements are specified, they are executed using a
// common transaction. This means that the client cannot use
// BEGIN/COMMIT/ROLLBACK. If any statement encounters a non-retriable error,
// the transaction is aborted and execution stops.
//
// There is no session state shared across the statements. For example,
// SET statements are ineffective.
//
// ---
// parameters:
// - name: sql
//   type: string
//   in: form
//   description: The SQL statement(s) to run.
//   required: true
// - name: database
//   type: string
//   in: form
//   description: The current database for the execution. Defaults to defaultdb.
// - name: application_name
//   type: string
//   in: form
//   description: The SQL application_name parameter.
// - name: timeout
//   type: string
//   in: form
//   description: Max time budget for the execution, using Go duration syntax. Default to 5 seconds.
//   required: false
// - name: max_result_size
//   type: integer
//   in: form
//   description:
//        Max size in bytes for the execution field in the response.
//        Execution stops with an error if the results do not fit.
// - name: show_columns_always
//   type: string
//   in: form
//   description:
//        If set and non-empty, force the output to contain column definitions even
//        when no rows were produced.
// consumes:
// - application/x-www-form-urlencoded
// produces:
// - application/json
// - text/plain
// responses:
//  '405':
//    description: Bad method. Only the POST method is supported.
//  '400':
//    description: Bad request. Bad input encoding, missing SQL or invalid parameter.
//  '200':
//    description: Query results and optional execution error.
//    schema:
//      type: object
//      required:
//       - num_statements
//       - execution
//      properties:
//        num_statements:
//          type: integer
//          description: The number of statements in the input SQL.
//        txn_error:
//          type: object
//          description: The details of the error, if an error was encountered.
//          required:
//            - message
//            - code
//          properties:
//            code:
//              type: string
//              description: The SQLSTATE 5-character code of the error.
//            message:
//              type: string
//          additionalProperties: {}
//        execution:
//          type: object
//          required:
//            - retries
//            - txn_results
//          properties:
//            retries:
//              type: integer
//              description: The number of times the transaction was retried.
//            txn_results:
//              type: array
//              description: The result sets, one per SQL statement.
//              items:
//                type: object
//                required:
//                  - statement
//                  - tag
//                  - start
//                  - end
//                properties:
//                  statement:
//                    type: integer
//                    description: The statement index in the SQL input.
//                  tag:
//                    type: string
//                    description: The short statement tag.
//                  start:
//                    type: string
//                    description: Start timestamp, encoded as RFC3339.
//                  end:
//                    type: string
//                    description: End timestamp, encoded as RFC3339.
//                  rows_affected:
//                    type: integer
//                    description: The number of rows affected.
//                  columns:
//                    type: array
//                    description: The list of columns in the result rows.
//                    items:
//                      type: object
//                      properties:
//                        name:
//                          type: string
//                          description: The column name.
//                        type:
//                          type: string
//                          description: The SQL type of the column.
//                        oid:
//                          type: integer
//                          description: The PostgreSQL OID for the column type.
//                      required:
//                        - name
//                        - type
//                        - oid
//                  rows:
//                    type: array
//                    description: The result rows.
//                    items: {}
func (a *apiV2Server) execSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "only POST supported", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()
	ctx = a.admin.server.AnnotateCtx(ctx)

	// Parse the parameters.
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	timeoutS := r.PostForm.Get("timeout")
	if timeoutS == "" {
		timeoutS = "5s"
	}
	timeout, err := time.ParseDuration(timeoutS)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	maxSizeS := r.PostForm.Get("max_result_size")
	if maxSizeS == "" {
		maxSizeS = "10000"
	}
	maxSize, err := strconv.Atoi(maxSizeS)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sql := r.PostForm.Get("sql")
	if sql == "" {
		http.Error(w, "no SQL specified", http.StatusBadRequest)
		return
	}
	appName := r.PostForm.Get("application_name")
	dbName := r.PostForm.Get("database")
	if dbName == "" {
		// TODO(knz): maybe derive the default value off the username?
		dbName = "defaultdb"
	}
	showCols := r.PostForm.Get("show_columns_always") != ""

	// Parse the input SQL.
	statements, err := parser.Parse(sql)
	if err != nil {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		formatError(w, err)
		return
	}
	// If the client did not request execution, just print what
	// we saw and call it a day.
	if r.PostForm.Get("execute") == "" {
		w.Header().Add("Content-Type", "text/plain; charset=utf-8")
		w.Header().Add("X-Content-Type-Options", "nosniff")
		fmt.Fprintln(w, "-- syntax check only; pass 'execute' in the POST request to run")
		fmt.Fprint(w, statements)
		return
	}

	// The SQL username that owns this session.
	username := getSQLUsername(ctx)

	// We will execute. The result will be JSON. Errors, if any,
	// will be reported as a JSON payload.
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// We can't use json.Marshal for the outer shell of the results, because
	// we want to stream the results and avoid accumulating results in RAM.
	fmt.Fprintf(w, "{\"num_statements\": %d,\n \"execution\": {\n", len(statements))

	// data conversion parameters: use defaults.
	// TODO(knz): Maybe let the client override this via query parameters.
	dcc := sessiondatapb.DataConversionConfig{}

	// wb is the buffer used to format the result set. It will be constrained
	// by the maxSize.
	wb := &bytes.Buffer{}
	checkSize := func() error {
		if wb.Len() > maxSize {
			return errors.New("max result size exceeded")
		}
		return nil
	}

	// jbuf is the buffer used to format individual datums.
	var jbuf bytes.Buffer

	// runner is the function that will execute all the statements as a group.
	// If there's just one statement, we execute them with an implicit,
	// auto-commit transaction.
	runner := func(ctx context.Context, fn func(context.Context, *kv.Txn) error) error { return fn(ctx, nil) }
	if len(statements) > 1 {
		// We need a transaction to group the statements together.
		// We use TxnWithSteppingEnabled here even though we don't
		// use stepping below, because that buys us admission control.
		runner = func(ctx context.Context, fn func(context.Context, *kv.Txn) error) error {
			return a.admin.server.db.TxnWithSteppingEnabled(ctx, sessiondatapb.Normal, fn)
		}
	}

	err = contextutil.RunWithTimeout(ctx, "run-sql-via-api", timeout, func(ctx context.Context) error {
		// The number of transaction retries. We keep track of this to
		// report it in the results.
		retryNum := 0

		return runner(ctx, func(ctx context.Context, txn *kv.Txn) error {
			wb.Reset()
			fmt.Fprintf(wb, "   \"retries\":%d,\n   \"txn_results\": [\n", retryNum)
			retryNum++
			defer fmt.Fprint(wb, "\n  ]")
			stmtResultComma := ""
			for stmtIdx, stmt := range statements {
				if err := a.shouldStop(ctx); err != nil {
					return err
				}
				returnType := stmt.AST.StatementReturnType()
				stmtErr := func() (retErr error) {
					fmt.Fprint(wb, stmtResultComma)
					stmtResultComma = ", "
					fmt.Fprintf(wb, "{\n  \"statement\": %d,\n  \"tag\": \"%s\",\n  \"start\":\"%s\"",
						stmtIdx+1,
						stmt.AST.StatementTag(),
						timeutil.Now().Format(time.RFC3339Nano))
					defer func() {
						fmt.Fprintf(wb, ",\n  \"end\":\"%s\"", timeutil.Now().Format(time.RFC3339Nano))
						if retErr != nil {
							fmt.Fprintf(wb, ",\n  \"error\": ")
							formatError(wb, retErr)
						}
						fmt.Fprintf(wb, "\n}")
					}()

					it, err := a.admin.ie.QueryIteratorEx(ctx, "run-query-via-api", txn,
						sessiondata.InternalExecutorOverride{
							User:            username,
							Database:        dbName,
							ApplicationName: appName,
						},
						stmt.SQL)
					if err != nil {
						return err
					}
					// We have to make sure to close the iterator since we might return from the
					// for loop early (before Next() returns false).
					defer func(it sqlutil.InternalRows) {
						if returnType == tree.RowsAffected || (returnType != tree.Rows && it.RowsAffected() > 0) {
							// NB: we need to place the number of rows affected inside
							// quotes because json integers must be 53 bits or less and
							// the row affected count may be 64 bits.
							fmt.Fprintf(wb, ",\n  \"rows_affected\":\"%d\"", it.RowsAffected())
						}
						retErr = errors.CombineErrors(retErr, it.Close())
					}(it)
					ok, err := it.Next(ctx)
					if err != nil {
						return err
					}
					// Only print the column headers if there's at least one result row
					// or the client requested the column header explicitly.
					cols := it.Types()
					if ok || showCols {
						if err := func() error {
							fmt.Fprint(wb, ",\n  \"columns\": [")
							defer wb.WriteByte(']')
							if err := showColumns(wb, &jbuf, cols); err != nil {
								return err
							}
							return checkSize()
						}(); err != nil {
							return err
						}
					}
					if ok {
						fmt.Fprint(wb, ",\n  \"rows\": [\n")
						defer fmt.Fprint(wb, "\n  ]")
						rowNum := 0
						for ; ok; ok, err = it.Next(ctx) {
							if err := a.shouldStop(ctx); err != nil {
								return err
							}
							if rowNum > 0 {
								fmt.Fprint(wb, ",\n")
							}
							if err := showRow(wb, checkSize, &jbuf, cols, dcc, it.Cur()); err != nil {
								return err
							}
							rowNum++
						}
					}
					return err
				}()
				if stmtErr != nil {
					return stmtErr
				}
			}
			return nil
		})
	})
	if err != nil {
		fmt.Fprint(wb, ",\n \"txn_error\": ")
		formatError(wb, err)
	}
	if _, err := wb.WriteTo(w); err != nil {
		fmt.Fprintf(w, `, {"error": "flush error"}`)
	}
	fmt.Fprint(w, "\n}}\n")
}

func (a *apiV2Server) shouldStop(ctx context.Context) error {
	select {
	case <-a.admin.server.stopper.ShouldQuiesce():
		return errors.New("server is shutting down")
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func formatError(w io.Writer, sqlErr error) {
	pqErr := pgerror.Flatten(sqlErr)
	b, err := gojson.MarshalIndent(pqErr, "", " ")
	if err != nil {
		fmt.Fprintf(w, `{"error": "json marshal error"}`)
		return
	}
	_, _ = w.Write(b)
}

func showColumns(wb io.Writer, jbuf *bytes.Buffer, cols colinfo.ResultColumns) error {
	jbuf.Reset()
	for colIdx, c := range cols {
		if colIdx > 0 {
			fmt.Fprint(jbuf, ", ")
		}
		jbuf.WriteString("{\"name\":")
		json.FromString(c.Name).Format(jbuf)
		jbuf.WriteString(", \"type\":")
		json.FromString(c.Typ.SQLString()).Format(jbuf)
		jbuf.WriteString(", \"oid\":")
		// NB: JSON integers have to be 53 bits or less. We don't
		// need to do anything here because Oid is 32-bit.
		fmt.Fprintf(jbuf, "%d", c.Typ.Oid())
		jbuf.WriteString("}")
	}
	_, err := jbuf.WriteTo(wb)
	return err
}

func showRow(
	wb io.Writer,
	checkSize func() error,
	jbuf *bytes.Buffer,
	cols colinfo.ResultColumns,
	dcc sessiondatapb.DataConversionConfig,
	row tree.Datums,
) error {
	fmt.Fprint(wb, "   {")
	defer fmt.Fprint(wb, "}")
	for colIdx, d := range row {
		if colIdx > 0 {
			fmt.Fprint(wb, ",")
		}
		jbuf.Reset()
		json.FromString(cols[colIdx].Name).Format(jbuf)
		jbuf.WriteByte(':')
		j, err := tree.AsJSON(d, dcc, time.UTC)
		if err != nil {
			return err
		}
		j.Format(jbuf)
		_, err = jbuf.WriteTo(wb)
		if err != nil {
			return err
		}
		if err := checkSize(); err != nil {
			return err
		}
	}
	return nil
}
