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
// If multiple SQL statements are specified and the multi_statement
// option is set, the SQL statements are executed using a common
// transaction. This means that the client cannot use
// BEGIN/COMMIT/ROLLBACK. If any statement encounters a non-retriable
// error, the transaction is aborted and execution stops.
//
// Only a single SQL statement is allowed if the multi_statement
// option is  not set, as a form of protection against SQL injection
// attacks.
//
// There is no session state shared across the statements. For example,
// SET statements are ineffective.
//
// ---
// consumes:
// - application/json
// parameters:
// - in: body
//   name: request
//   schema:
//     type: object
//     required:
//     - statements
//     properties:
//       database:
//         type: string
//         description: The current database for the execution. Defaults to defaultdb.
//       application_name:
//         type: string
//         description: The SQL application_name parameter.
//       timeout:
//         type: string
//         description: Max time budget for the execution, using Go duration syntax. Default to 5 seconds.
//       max_result_size:
//         type: integer
//         description:
//            Max size in bytes for the execution field in the response.
//            Execution stops with an error if the results do not fit.
//       statements:
//         description: The SQL statement(s) to run.
//         type: array
//         items:
//            type: object
//            required:
//            - sql
//            properties:
//               sql:
//                 type: string
//                 description: SQL syntax for one statement.
//               arguments:
//                 type: array
//                 description: Placeholder parameter values.
// produces:
// - application/json
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
		topLevelError(w, errors.New("only POST supported"), http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()
	ctx = a.admin.server.AnnotateCtx(ctx)

	// Read the request arguments.
	// Is there a request payload?
	ct := r.Header.Get("Content-Type")
	// RFC 7231, section 3.1.1.5 - empty type
	//   MAY be treated as application/octet-stream
	if ct == "" {
		ct = "application/octet-stream"
	}
	if ct != "application/octet-stream" && ct != "application/json" {
		topLevelError(w, errors.Newf("expecting content-type json: %q", ct),
			http.StatusBadRequest)
		return
	}

	// Ensure we don't read too much data at a time.
	rc := http.MaxBytesReader(w, r.Body, 10*1024*1024 /* 10MiB */)
	defer rc.Close()

	// Now read it.
	requestPayload := struct {
		Timeout         string
		MaxResultSize   int
		Database        string
		ApplicationName string
		Execute         bool
		Statements      []struct {
			SQL       string
			stmt      parser.Statement
			Arguments []interface{} `json:",omitempty"`
		}
	}{}
	input, err := io.ReadAll(rc)
	if err != nil {
		topLevelError(w, err, http.StatusBadRequest)
		return
	}

	if err := gojson.Unmarshal(input, &requestPayload); err != nil {
		topLevelError(w, err, http.StatusBadRequest)
		return
	}

	if requestPayload.Timeout == "" {
		requestPayload.Timeout = "5s"
	}
	timeout, err := time.ParseDuration(requestPayload.Timeout)
	if err != nil {
		topLevelError(w, err, http.StatusBadRequest)
		return
	}
	if requestPayload.MaxResultSize == 0 {
		requestPayload.MaxResultSize = 10000
	}
	if len(requestPayload.Statements) == 0 {
		topLevelError(w, errors.New("no statements specified"), http.StatusBadRequest)
		return
	}
	if requestPayload.Database == "" {
		// TODO(knz): maybe derive the default value off the username?
		requestPayload.Database = "defaultdb"
	}
	if requestPayload.ApplicationName == "" {
		requestPayload.ApplicationName = "$ api-v2-sql"
	}

	// Parse the input SQL.
	for i := range requestPayload.Statements {
		s := &requestPayload.Statements[i]
		stmts, err := parser.Parse(s.SQL)
		if err != nil {
			topLevelError(w, errors.WithDetail(
				errors.Wrapf(err, "parsing statement %d", i+1), s.SQL),
				http.StatusBadRequest)
			return
		}
		if len(stmts) != 1 {
			topLevelError(w, errors.WithDetail(errors.Wrapf(
				errors.Newf("expecting 1 statement, found %d", len(stmts)),
				"parsing statement %d", i+1), s.SQL), http.StatusBadRequest)
			return
		}
		s.stmt = stmts[0]
		if s.stmt.NumPlaceholders != len(s.Arguments) {
			topLevelError(w, errors.WithDetail(
				errors.Newf("parsing statement %d: expected %d placeholder(s), got %d",
					i+1, s.stmt.NumPlaceholders, len(s.Arguments)), s.SQL), http.StatusBadRequest)
			return
		}
	}

	// If the client did not request execution, just print what
	// we saw and call it a day.
	var onlyPrint []byte
	if !requestPayload.Execute {
		for i := range requestPayload.Statements {
			s := &requestPayload.Statements[i]
			s.SQL = s.stmt.AST.String()
		}
		onlyPrint, err = gojson.MarshalIndent(&requestPayload, "", " ")
		if err != nil {
			topLevelError(w, err, http.StatusBadRequest)
			return
		}
	}
	// The result will be JSON. Errors, if any,
	// will be reported as a JSON payload.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	if !requestPayload.Execute {
		_, _ = w.Write(onlyPrint)
		return
	}

	// The SQL username that owns this session.
	username := getSQLUsername(ctx)

	// We can't use json.Marshal for the outer shell of the results, because
	// we want to stream the results and avoid accumulating results in RAM.
	fmt.Fprintf(w, "{\"num_statements\": %d,\n \"execution\": {\n", len(requestPayload.Statements))

	// data conversion parameters: use defaults.
	// TODO(knz): Maybe let the client override this via query parameters.
	dcc := sessiondatapb.DataConversionConfig{}

	// wb is the buffer used to format the result set. It will be constrained
	// by the maxSize.
	wb := &bytes.Buffer{}
	checkSize := func() error {
		if wb.Len() > requestPayload.MaxResultSize {
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
	if len(requestPayload.Statements) > 1 {
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
			for stmtIdx, stmt := range requestPayload.Statements {
				if err := a.shouldStop(ctx); err != nil {
					return err
				}
				returnType := stmt.stmt.AST.StatementReturnType()
				stmtErr := func() (retErr error) {
					fmt.Fprint(wb, stmtResultComma)
					stmtResultComma = ", "
					fmt.Fprintf(wb, "{\n  \"statement\": %d,\n  \"tag\": \"%s\",\n  \"start\":\"%s\"",
						stmtIdx+1,
						stmt.stmt.AST.StatementTag(),
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
							Database:        requestPayload.Database,
							ApplicationName: requestPayload.ApplicationName,
						},
						stmt.SQL, stmt.Arguments...)
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
					// Print the column headers.
					cols := it.Types()
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
					// Print the result rows.
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

func topLevelError(w http.ResponseWriter, err error, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)

	fmt.Fprintln(w, `{"error":`)
	defer fmt.Fprintln(w, "}")
	formatError(w, err)
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
