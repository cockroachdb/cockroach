// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// SQLAPIClock is exposed for override by tests. Tenant tests are in
// the serverccl package.
var SQLAPIClock timeutil.TimeSource = timeutil.DefaultTimeSource{}

// # Execute one or more SQL statements
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
//   - in: body
//     name: request
//     schema:
//     type: object
//     required:
//   - statements
//     properties:
//     database:
//     type: string
//     description: The current database for the execution. Defaults to defaultdb.
//     application_name:
//     type: string
//     description: The SQL application_name parameter.
//     timeout:
//     type: string
//     description: Max time budget for the execution, using Go duration syntax. Default to 5 seconds.
//     max_result_size:
//     type: integer
//     description:
//     Max size in bytes for the execution field in the response.
//     Execution stops with an error if the results do not fit.
//     statements:
//     description: The SQL statement(s) to run.
//     type: array
//     items:
//     type: object
//     required:
//   - sql
//     properties:
//     sql:
//     type: string
//     description: SQL syntax for one statement.
//     arguments:
//     type: array
//     description: Placeholder parameter values.
//
// produces:
// - application/json
// responses:
//
//	'405':
//	  description: Bad method. Only the POST method is supported.
//	'400':
//	  description: Bad request. Bad input encoding, missing SQL or invalid parameter.
//	'500':
//	  description: Internal error encountered.
//	'200':
//	  description: Query results and optional execution error.
//	  schema:
//	    type: object
//	    required:
//	     - num_statements
//	     - execution
//	    properties:
//	      num_statements:
//	        type: integer
//	        description: The number of statements in the input SQL.
//	      txn_error:
//	        type: object
//	        description: The details of the error, if an error was encountered.
//	        required:
//	          - message
//	          - code
//	        properties:
//	          code:
//	            type: string
//	            description: The SQLSTATE 5-character code of the error.
//	          message:
//	            type: string
//	        additionalProperties: {}
//	      execution:
//	        type: object
//	        required:
//	          - retries
//	          - txn_results
//	        properties:
//	          retries:
//	            type: integer
//	            description: The number of times the transaction was retried.
//	          txn_results:
//	            type: array
//	            description: The result sets, one per SQL statement.
//	            items:
//	              type: object
//	              required:
//	                - statement
//	                - tag
//	                - start
//	                - end
//	              properties:
//	                statement:
//	                  type: integer
//	                  description: The statement index in the SQL input.
//	                tag:
//	                  type: string
//	                  description: The short statement tag.
//	                start:
//	                  type: string
//	                  description: Start timestamp, encoded as RFC3339.
//	                end:
//	                  type: string
//	                  description: End timestamp, encoded as RFC3339.
//	                rows_affected:
//	                  type: integer
//	                  description: The number of rows affected.
//	                columns:
//	                  type: array
//	                  description: The list of columns in the result rows.
//	                  items:
//	                    type: object
//	                    properties:
//	                      name:
//	                        type: string
//	                        description: The column name.
//	                      type:
//	                        type: string
//	                        description: The SQL type of the column.
//	                      oid:
//	                        type: integer
//	                        description: The PostgreSQL OID for the column type.
//	                    required:
//	                      - name
//	                      - type
//	                      - oid
//	                rows:
//	                  type: array
//	                  description: The result rows.
//	                  items: {}

func (a *apiV2Server) execSQL(w http.ResponseWriter, r *http.Request) {
	// Type for the request.
	type requestType struct {
		Timeout         string `json:"timeout"`
		MaxResultSize   int    `json:"max_result_size"`
		Database        string `json:"database"`
		ApplicationName string `json:"application_name"`
		Execute         bool   `json:"execute"`
		SeparateTxns    bool   `json:"separate_txns"`
		StopOnError     bool   `json:"stop_on_error"`
		Statements      []struct {
			SQL       string                               `json:"sql"`
			stmt      statements.Statement[tree.Statement] `json:"-"`
			Arguments []interface{}                        `json:"arguments,omitempty"`
		} `json:"statements"`
	}

	// Type for the result.
	type txnResult struct {
		Statement    int               `json:"statement"` // index of statement in request.
		Tag          string            `json:"tag"`       // SQL statement tag.
		Start        jsonTime          `json:"start"`     // start timestamp.
		End          jsonTime          `json:"end"`       // end timestamp.
		RowsAffected int               `json:"rows_affected"`
		Columns      columnsDefinition `json:"columns,omitempty"`
		Rows         []resultRow       `json:"rows,omitempty"`
		Error        *jsonError        `json:"error,omitempty"`
	}
	type execResult struct {
		Retries    int         `json:"retries,omitempty"`
		TxnResults []txnResult `json:"txn_results"`
	}
	var result struct {
		Error         *jsonError   `json:"error,omitempty"`
		NumStatements int          `json:"num_statements,omitempty"`
		Request       *requestType `json:"request,omitempty"`
		Execution     *execResult  `json:"execution,omitempty"`
	}
	httpCode := http.StatusOK
	defer func() {
		b, err := gojson.Marshal(&result)
		if err != nil {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusInternalServerError)
			log.Errorf(r.Context(), "JSON marshal error: %v", err)
			_, err = w.Write([]byte(err.Error()))
			if err != nil {
				log.Warningf(r.Context(), "HTTP short write: %v", err)
			}
			return
		}
		// The result will be JSON. Errors, if any,
		// will be reported as a JSON payload.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(httpCode)
		_, err = w.Write(b)
		if err != nil {
			log.Warningf(r.Context(), "HTTP short write: %v", err)
		}
	}()

	topLevelError := func(err error, code int) {
		result.Error = &jsonError{err}
		httpCode = code
	}

	if r.Method != "POST" {
		topLevelError(errors.New("only POST supported"), http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()
	ctx = a.sqlServer.ambientCtx.AnnotateCtx(ctx)

	// Read the request arguments.
	// Is there a request payload?
	ct := r.Header.Get("Content-Type")
	// RFC 7231, section 3.1.1.5 - empty type
	//   MAY be treated as application/octet-stream
	if ct == "" {
		ct = "application/octet-stream"
	}
	if ct != "application/octet-stream" && ct != "application/json" {
		topLevelError(errors.Newf("expecting content-type json: %q", ct),
			http.StatusBadRequest)
		return
	}

	// Ensure we don't read too much data at a time.
	rc := http.MaxBytesReader(w, r.Body, 10*1024*1024 /* 10MiB */)
	defer rc.Close()

	// Now read it.
	requestPayload := requestType{}
	input, err := io.ReadAll(rc)
	if err != nil {
		topLevelError(err, http.StatusBadRequest)
		return
	}

	if err := gojson.Unmarshal(input, &requestPayload); err != nil {
		topLevelError(err, http.StatusBadRequest)
		return
	}

	if requestPayload.Timeout == "" {
		requestPayload.Timeout = "5s"
	}
	timeout, err := time.ParseDuration(requestPayload.Timeout)
	if err != nil {
		topLevelError(err, http.StatusBadRequest)
		return
	}
	if requestPayload.MaxResultSize == 0 {
		requestPayload.MaxResultSize = 10000
	}
	if len(requestPayload.Statements) == 0 {
		topLevelError(errors.New("no statements specified"), http.StatusBadRequest)
		return
	}
	if requestPayload.Database == "" {
		requestPayload.Database = "system"
	}
	if requestPayload.ApplicationName == "" {
		requestPayload.ApplicationName = "$ api-v2-sql"
	}

	localityMetadataEnabled := ui.DatabaseLocalityMetadataEnabled.Get(a.sqlServer.execCfg.SV())

	// Parse the input SQL.
	for i := range requestPayload.Statements {
		s := &requestPayload.Statements[i]

		if !localityMetadataEnabled {
			// Note(davidh): This is a hack. The reason I'm putting the code
			// for it here is twofold: 1. The code to make this change happen
			// on the frontend is too complex because of the use of this SQL
			// API. We end up having to do a lot of client-side wrangling to
			// read the cluster settings, then make the DB request but filter
			// out the offending queries. Making the change here reduces the
			// amount of code necessary. 2. Reducing the diff size is
			// necessary because we want to backport this change to 23.2,
			// 24.1, and 24.2. This hack can be removed after 24.3 is
			// released because the new DB page will be in place.
			if localityMetadataQueryRegexp.Match([]byte(s.SQL)) {
				// Replace with a no-op to allow the rest of the execution to
				// continue and return a matchin list of responses. Client-side
				// code is resilient to empty results.
				s.SQL = "SELECT 1"
			}
		}

		stmts, err := parser.Parse(s.SQL)
		if err != nil {
			topLevelError(errors.WithDetail(
				errors.Wrapf(err, "parsing statement %d", i+1), s.SQL),
				http.StatusBadRequest)
			return
		}
		if len(stmts) != 1 {
			topLevelError(errors.WithDetail(errors.Wrapf(
				errors.Newf("expecting 1 statement, found %d", len(stmts)),
				"parsing statement %d", i+1), s.SQL), http.StatusBadRequest)
			return
		}
		s.stmt = stmts[0]
		if s.stmt.NumPlaceholders != len(s.Arguments) {
			topLevelError(errors.WithDetail(
				errors.Newf("parsing statement %d: expected %d placeholder(s), got %d",
					i+1, s.stmt.NumPlaceholders, len(s.Arguments)), s.SQL), http.StatusBadRequest)
			return
		}
	}

	result.NumStatements = len(requestPayload.Statements)

	// If the client did not request execution, just print what
	// we saw and call it a day.
	if !requestPayload.Execute {
		for i := range requestPayload.Statements {
			s := &requestPayload.Statements[i]
			s.SQL = s.stmt.AST.String()
		}
		result.Request = &requestPayload
		return
	}

	// The SQL username that owns this session.
	username := authserver.UserFromHTTPAuthInfoContext(ctx)

	options := []isql.TxnOption{
		isql.WithPriority(admissionpb.NormalPri),
	}

	// execResultRunner is the function that runs the entirety of the request.
	var execResultRunner func(context.Context, func(context.Context, isql.Txn) error, ...isql.TxnOption) error
	// stmtRunner is the function that runs each statement in turn.
	var stmtRunner func(ctx context.Context, outerTxn isql.Txn, queryFn func(ctx context.Context, innerTxn isql.Txn) error, opts ...isql.TxnOption) error
	var handleStmtErr func(outerErr, stmtErr error) (err error, terminate bool)

	// Select which runners to use depending on the transaction mode.
	if !requestPayload.SeparateTxns {
		execResultRunner = a.sqlServer.internalDB.Txn
		handleStmtErr = func(_, stmtErr error) (error, bool) {
			// In a single txn, any stmt err ends the txn
			return stmtErr, stmtErr != nil
		}
		stmtRunner = func(ctx context.Context, outerTxn isql.Txn, queryFn func(ctx context.Context, innerTxn isql.Txn) error, _ ...isql.TxnOption) error {
			return queryFn(ctx, outerTxn)
		}
	} else {
		execResultRunner = func(ctx context.Context, queryFn func(ctx context.Context, txn isql.Txn) error, _ ...isql.TxnOption) error {
			return queryFn(ctx, nil /* txn */)
		}
		handleStmtErr = func(outerErr, stmtErr error) (error, bool) {
			// If we encounter a stmt error with separate txns, set the outer error
			if stmtErr != nil {
				if outerErr == nil {
					outerErr = errors.New("separate transaction payload encountered transaction error(s)")
				}
				// If StopOnError is specified, return the outerErr and terminate
				if requestPayload.StopOnError {
					return outerErr, true
				}
			}
			// Return outerErr without terminating
			return outerErr, false
		}
		stmtRunner = func(ctx context.Context, _ isql.Txn, queryFn func(ctx context.Context, innerTxn isql.Txn) error, opts ...isql.TxnOption) error {
			return a.sqlServer.internalDB.Txn(ctx, queryFn, opts...)
		}
	}

	result.Execution = &execResult{}
	result.Execution.TxnResults = make([]txnResult, 0, len(requestPayload.Statements))

	err = timeutil.RunWithTimeout(ctx, "run-sql-via-api", timeout, func(ctx context.Context) error {
		retryNum := 0
		return execResultRunner(ctx, func(ctx context.Context, txn isql.Txn) error {
			result.Execution.TxnResults = result.Execution.TxnResults[:0]
			result.Execution.Retries = retryNum
			retryNum++
			curSize := uintptr(0)
			var outerErr error
			checkSize := func(size uintptr) error {
				if size > uintptr(requestPayload.MaxResultSize) {
					return errors.New("max result size exceeded")
				}
				return nil
			}
			addSize := func(row tree.Datums) error {
				for _, c := range row {
					curSize += c.Size()
				}
				return checkSize(curSize)
			}

			for stmtIdx, stmt := range requestPayload.Statements {
				// Is server shutting down? Or query timing out?
				if err := a.shouldStop(ctx); err != nil {
					return err
				}

				result.Execution.TxnResults = append(result.Execution.TxnResults, txnResult{})
				txnRes := &result.Execution.TxnResults[stmtIdx]

				returnType := stmt.stmt.AST.StatementReturnType()
				stmtErr := stmtRunner(ctx, txn, func(ctx context.Context, txn isql.Txn) (retErr error) {
					txnRes.Start = jsonTime(SQLAPIClock.Now())
					txnRes.Statement = stmtIdx + 1
					txnRes.Tag = stmt.stmt.AST.StatementTag()
					defer func() {
						txnRes.End = jsonTime(SQLAPIClock.Now())
						if retErr != nil {
							retErr = errors.Wrapf(retErr, "executing stmt %d", stmtIdx+1)
							txnRes.Error = &jsonError{retErr}
						}
					}()

					if returnType == tree.Ack || stmt.stmt.AST.StatementType() == tree.TypeTCL {
						// We want to disallow statements that modify txn state (like
						// BEGIN and COMMIT) because the internal executor does not
						// expect such statements. We'll lean on the safe side and
						// prohibit all statements with an ACK return type, similar
						// to the builtin `crdb_internal.execute_internally(...)`.
						return errors.New("disallowed statement type")
					}

					// If the max size has been exceeded by previous statements/transactions
					// avoid executing, return immediately.
					err := checkSize(curSize)
					if err != nil {
						return err
					}

					it, err := txn.QueryIteratorEx(ctx, "run-query-via-api", txn.KV(),
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
					defer func(it isql.Rows) {
						if returnType == tree.RowsAffected || (returnType != tree.Rows && it.RowsAffected() > 0) {
							txnRes.RowsAffected = it.RowsAffected()
						}
						retErr = errors.CombineErrors(retErr, it.Close())
					}(it)
					ok, err := it.Next(ctx)
					if err != nil {
						return err
					}

					// Make sure column names are not duplicated.
					txnRes.Columns = columnsDefinition(it.Types())
					seenColumnNames := make(map[string]int, len(txnRes.Columns))
					for i, col := range txnRes.Columns {
						origName := col.Name
						count := seenColumnNames[origName]
						if count > 0 {
							txnRes.Columns[i].Name = fmt.Sprintf("%s_%d", origName, count)
						}
						seenColumnNames[origName] = count + 1
					}

					for ; ok; ok, err = it.Next(ctx) {
						if err := a.shouldStop(ctx); err != nil {
							return err
						}
						txnRes.Rows = append(txnRes.Rows,
							resultRow{cols: it.Types(), row: it.Cur()})
						if err := addSize(it.Cur()); err != nil {
							return err
						}
					}
					return err
				}, options...)
				handledErr, terminate := handleStmtErr(outerErr, stmtErr)
				outerErr = handledErr
				if terminate {
					return outerErr
				}
			}
			return outerErr
		}, options...)
	})
	if err != nil {
		result.Error = &jsonError{err}
	}
}

type columnsDefinition colinfo.ResultColumns

func (cd columnsDefinition) MarshalJSON() ([]byte, error) {
	var jbuf bytes.Buffer
	jbuf.WriteByte('[')
	for colIdx, c := range cd {
		if colIdx > 0 {
			jbuf.WriteByte(',')
		}
		jbuf.WriteString("{\"name\":")
		json.FromString(c.Name).Format(&jbuf)
		jbuf.WriteString(",\"type\":")
		json.FromString(c.Typ.SQLString()).Format(&jbuf)
		jbuf.WriteString(",\"oid\":")
		// NB: JSON integers have to be 53 bits or less. We don't
		// need to do anything here because Oid is 32-bit.
		fmt.Fprintf(&jbuf, "%d", c.Typ.Oid())
		jbuf.WriteByte('}')
	}
	jbuf.WriteByte(']')
	return jbuf.Bytes(), nil
}

type jsonTime time.Time

func (t *jsonTime) MarshalJSON() ([]byte, error) {
	s := (*time.Time)(t).Format(time.RFC3339Nano)
	var buf bytes.Buffer
	json.FromString(s).Format(&buf)
	return buf.Bytes(), nil
}

type resultRow struct {
	cols colinfo.ResultColumns
	row  tree.Datums
}

func (r *resultRow) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for colIdx, d := range r.row {
		if colIdx > 0 {
			buf.WriteByte(',')
		}
		json.FromString(r.cols[colIdx].Name).Format(&buf)
		buf.WriteByte(':')
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		if err != nil {
			return nil, err
		}
		j.Format(&buf)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (a *apiV2Server) shouldStop(ctx context.Context) error {
	select {
	case <-a.sqlServer.stopper.ShouldQuiesce():
		return errors.New("server is shutting down")
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type jsonError struct{ error }

func (j jsonError) MarshalJSON() ([]byte, error) {
	pqErr := pgerror.Flatten(j.error)
	return gojson.MarshalIndent(pqErr, "", "")
}

// This regexp is constructed to match client-side queries written in
// `databaseDetailsApi.ts` and `tableDetailsApi.ts`. It's meant to
// filter out queries that use `SHOW RANGES` which are expensive to
// execute and can be disabled by the operator. See the Note above for
// an explanation of this hack.
var localityMetadataQueryRegexp = regexp.MustCompile(`SHOW RANGES FROM (DATABASE|TABLE)`)
