// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestParseTimeRange(t *testing.T) {
	t.Run("no params", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		start, end, err := parseTimeRange(req)
		require.NoError(t, err)
		require.Nil(t, start)
		require.Nil(t, end)
	})

	t.Run("valid start", func(t *testing.T) {
		req := httptest.NewRequest(
			"GET", "/test?start=2024-01-15T10:00:00Z", nil,
		)
		start, end, err := parseTimeRange(req)
		require.NoError(t, err)
		require.NotNil(t, start)
		require.Nil(t, end)
		require.Equal(t, 2024, start.Year())
	})

	t.Run("valid end", func(t *testing.T) {
		req := httptest.NewRequest(
			"GET", "/test?end=2024-01-16T10:00:00Z", nil,
		)
		start, end, err := parseTimeRange(req)
		require.NoError(t, err)
		require.Nil(t, start)
		require.NotNil(t, end)
	})

	t.Run("both start and end", func(t *testing.T) {
		req := httptest.NewRequest(
			"GET",
			"/test?start=2024-01-15T10:00:00Z&end=2024-01-16T10:00:00Z",
			nil,
		)
		start, end, err := parseTimeRange(req)
		require.NoError(t, err)
		require.NotNil(t, start)
		require.NotNil(t, end)
	})

	t.Run("invalid start", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test?start=not-a-time", nil)
		_, _, err := parseTimeRange(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "start")
	})

	t.Run("invalid end", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test?end=not-a-time", nil)
		_, _, err := parseTimeRange(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "end")
	})
}

// makeStmtInsightRow builds a tree.Datums row matching the 26-column
// statement insights query.
func makeStmtInsightRow(stmtID, query string) tree.Datums {
	return tree.Datums{
		dString("session-1"),          // session_id
		dString("txn-1"),              // txn_id
		dString("txn-fp-1"),           // txn_fingerprint_id
		dBool(true),                   // implicit_txn
		dString(stmtID),               // stmt_id
		dString("stmt-fp-1"),          // stmt_fingerprint_id
		dString(query),                // query
		dString("2024-01-15 10:00"),   // start_time
		dString("2024-01-15 10:01"),   // end_time
		dBool(false),                  // full_scan
		dString("testuser"),           // user_name
		dString("myapp"),              // app_name
		dString("mydb"),               // database_name
		dInt(100),                     // rows_read
		dInt(10),                      // rows_written
		dString("NORMAL"),             // priority
		dInt(0),                       // retries
		dString("00:00:01"),           // contention
		tree.DNull,                    // last_retry_reason
		dStringArray("SlowExecution"), // causes
		dString("SlowExecution"),      // problem
		dStringArray(),                // index_recommendations
		dString("AgHQAQ"),             // plan_gist
		dInt(5000000),                 // cpu_sql_nanos
		tree.DNull,                    // error_code
		dString("Completed"),          // status
	}
}

func TestGetStatementInsights(t *testing.T) {
	t.Run("success with contention enrichment", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) ([]tree.Datums, error) {
				switch string(opName) {
				case "dbconsole-get-stmt-insights":
					return []tree.Datums{
						makeStmtInsightRow("stmt-1", "SELECT * FROM users"),
					}, nil
				case "dbconsole-get-stmt-contention":
					return []tree.Datums{
						{
							dString("stmt-1"),      // waiting_stmt_id
							dString("block-txn"),   // blocking_txn_id
							dString("00:00:00.5"),  // contention_duration
							dString("/Table/42/1"), // key
							dString("users"),       // table_name
							dString("users_pkey"),  // index_name
						},
					}, nil
				}
				return nil, nil
			},
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/statements", nil,
		)
		w := httptest.NewRecorder()
		api.GetStatementInsights(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result StmtInsightsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Statements, 1)

		s := result.Statements[0]
		require.Equal(t, "SELECT * FROM users", s.Query)
		require.Equal(t, "mydb", s.DatabaseName)
		require.Equal(t, int64(100), s.RowsRead)
		require.Equal(t, []string{"SlowExecution"}, s.Causes)
		require.Equal(t, "SlowExecution", s.Problem)

		// Verify contention enrichment.
		require.Len(t, s.ContentionEvents, 1)
		require.Equal(t, "block-txn", s.ContentionEvents[0].BlockingTxnID)
		require.Equal(t, "users", s.ContentionEvents[0].TableName)
	})

	t.Run("with time range", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET",
			"/api/v2/dbconsole/insights/statements?start=2024-01-15T00:00:00Z&end=2024-01-16T00:00:00Z",
			nil,
		)
		w := httptest.NewRecorder()
		api.GetStatementInsights(w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx(
				nil, errors.New("query failed"),
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/statements", nil,
		)
		w := httptest.NewRecorder()
		api.GetStatementInsights(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

// makeTxnInsightRow builds a tree.Datums row matching the 20-column
// transaction insights query.
func makeTxnInsightRow(txnID, query string) tree.Datums {
	return tree.Datums{
		dString("session-1"),          // session_id
		dString(txnID),                // txn_id
		dString("txn-fp-1"),           // txn_fingerprint_id
		dBool(false),                  // implicit_txn
		dString(query),                // query
		dString("2024-01-15 10:00"),   // start_time
		dString("2024-01-15 10:01"),   // end_time
		dString("myapp"),              // app_name
		dString("testuser"),           // user_name
		dInt(500),                     // rows_read
		dInt(50),                      // rows_written
		dString("NORMAL"),             // priority
		dInt(1),                       // retries
		dString("00:00:02"),           // contention
		dStringArray("SlowExecution"), // problems
		dStringArray("SlowExecution"), // causes
		dStringArray("stmt-1"),        // stmt_execution_ids
		dInt(10000000),                // cpu_sql_nanos
		tree.DNull,                    // last_error_code
		dString("Completed"),          // status
	}
}

func TestGetTransactionInsights(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				makeTxnInsightRow("txn-1", "BEGIN; SELECT * FROM users; COMMIT"),
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/transactions", nil,
		)
		w := httptest.NewRecorder()
		api.GetTransactionInsights(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result TxnInsightsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Transactions, 1)

		txn := result.Transactions[0]
		require.Equal(t, "txn-1", txn.TxnID)
		require.Equal(t, int64(500), txn.RowsRead)
		require.Equal(t, int64(1), txn.Retries)
		require.Equal(t, []string{"SlowExecution"}, txn.Problems)
		require.Equal(t, []string{"stmt-1"}, txn.StmtExecutionIDs)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx(
				nil, errors.New("query failed"),
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/transactions", nil,
		)
		w := httptest.NewRecorder()
		api.GetTransactionInsights(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestGetTransactionInsightDetails(t *testing.T) {
	t.Run("success with all sub-queries", func(t *testing.T) {
		callCount := 0
		exec := &mockExecutor{
			queryBufferedExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) ([]tree.Datums, error) {
				callCount++
				switch string(opName) {
				case "dbconsole-txn-insight-details":
					return []tree.Datums{
						makeTxnInsightRow("txn-1", "SELECT * FROM users"),
					}, nil
				case "dbconsole-txn-insight-stmts":
					return []tree.Datums{
						makeStmtInsightRow("stmt-1", "SELECT * FROM users"),
					}, nil
				case "dbconsole-txn-insight-contention":
					return []tree.Datums{
						{
							dString("block-txn"),   // blocking_txn_id
							dString("00:00:00.5"),  // contention_duration
							dString("/Table/42/1"), // key
							dString("users"),       // table_name
							dString("users_pkey"),  // index_name
							dString("LOCK_WAIT"),   // contention_type
						},
					}, nil
				}
				return nil, nil
			},
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/transactions/txn-1", nil,
		)
		req = mux.SetURLVars(req, map[string]string{
			"txn_execution_id": "txn-1",
		})

		w := httptest.NewRecorder()
		api.GetTransactionInsightDetails(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, 3, callCount)

		var result TxnInsightDetailsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Equal(t, "txn-1", result.TxnExecutionID)
		require.NotNil(t, result.TxnDetails)
		require.Equal(t, "txn-1", result.TxnDetails.TxnID)
		require.Len(t, result.Statements, 1)
		require.Len(t, result.ContentionDetails, 1)
		require.Equal(t, "LOCK_WAIT", result.ContentionDetails[0].ContentionType)
		require.Empty(t, result.Errors.TxnDetailsErr)
		require.Empty(t, result.Errors.StatementsErr)
		require.Empty(t, result.Errors.ContentionErr)
	})

	t.Run("with exclusions", func(t *testing.T) {
		callCount := 0
		exec := &mockExecutor{
			queryBufferedExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) ([]tree.Datums, error) {
				callCount++
				return []tree.Datums{}, nil
			},
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET",
			"/api/v2/dbconsole/insights/transactions/txn-1?exclude_stmts=true&exclude_contention=true",
			nil,
		)
		req = mux.SetURLVars(req, map[string]string{
			"txn_execution_id": "txn-1",
		})

		w := httptest.NewRecorder()
		api.GetTransactionInsightDetails(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// Only txn details query should be made.
		require.Equal(t, 1, callCount)
	})

	t.Run("partial failure", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) ([]tree.Datums, error) {
				switch string(opName) {
				case "dbconsole-txn-insight-details":
					return nil, errors.New("txn query failed")
				case "dbconsole-txn-insight-stmts":
					return []tree.Datums{
						makeStmtInsightRow("stmt-1", "SELECT 1"),
					}, nil
				case "dbconsole-txn-insight-contention":
					return nil, errors.New("contention query failed")
				}
				return nil, nil
			},
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/transactions/txn-1", nil,
		)
		req = mux.SetURLVars(req, map[string]string{
			"txn_execution_id": "txn-1",
		})

		w := httptest.NewRecorder()
		api.GetTransactionInsightDetails(w, req)

		// Partial failures still return 200 with error details.
		require.Equal(t, http.StatusOK, w.Code)

		var result TxnInsightDetailsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Nil(t, result.TxnDetails)
		require.Len(t, result.Statements, 1)
		require.Empty(t, result.ContentionDetails)
		require.Equal(t, "txn query failed", result.Errors.TxnDetailsErr)
		require.Empty(t, result.Errors.StatementsErr)
		require.Equal(t, "contention query failed", result.Errors.ContentionErr)
	})
}
