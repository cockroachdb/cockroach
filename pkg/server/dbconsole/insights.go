// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
)

// StmtInsightContentionEvent represents a contention event associated with a
// statement insight.
type StmtInsightContentionEvent struct {
	// BlockingTxnID is the ID of the blocking transaction.
	BlockingTxnID string `json:"blocking_txn_id"`
	// ContentionDuration is the duration of the contention event.
	ContentionDuration string `json:"contention_duration"`
	// ContendedKey is the key that was contended.
	ContendedKey string `json:"contended_key"`
	// TableName is the table where contention occurred.
	TableName string `json:"table_name"`
	// IndexName is the index where contention occurred.
	IndexName string `json:"index_name"`
}

// StmtInsight represents a single statement execution insight.
type StmtInsight struct {
	SessionID            string                       `json:"session_id"`
	TxnID                string                       `json:"txn_id"`
	TxnFingerprintID     string                       `json:"txn_fingerprint_id"`
	ImplicitTxn          bool                         `json:"implicit_txn"`
	StmtID               string                       `json:"stmt_id"`
	StmtFingerprintID    string                       `json:"stmt_fingerprint_id"`
	Query                string                       `json:"query"`
	StartTime            string                       `json:"start_time"`
	EndTime              string                       `json:"end_time"`
	FullScan             bool                         `json:"full_scan"`
	UserName             string                       `json:"user_name"`
	AppName              string                       `json:"app_name"`
	DatabaseName         string                       `json:"database_name"`
	RowsRead             int64                        `json:"rows_read"`
	RowsWritten          int64                        `json:"rows_written"`
	Priority             string                       `json:"priority"`
	Retries              int64                        `json:"retries"`
	Contention           string                       `json:"contention,omitempty"`
	LastRetryReason      string                       `json:"last_retry_reason,omitempty"`
	Causes               []string                     `json:"causes"`
	Problem              string                       `json:"problem"`
	IndexRecommendations []string                     `json:"index_recommendations"`
	PlanGist             string                       `json:"plan_gist"`
	CPUSQLNanos          int64                        `json:"cpu_sql_nanos"`
	ErrorCode            string                       `json:"error_code,omitempty"`
	Status               string                       `json:"status"`
	ContentionEvents     []StmtInsightContentionEvent `json:"contention_events,omitempty"`
}

// StmtInsightsResponse contains the list of statement execution insights.
type StmtInsightsResponse struct {
	// Statements is the list of statement insights.
	Statements []StmtInsight `json:"statements"`
}

// GetStatementInsights returns statement execution insights with optional
// filtering by time range, statement execution ID, or statement fingerprint
// ID. Contention events are enriched server-side, eliminating N+1 queries.
//
// ---
// @Summary Get statement insights
// @Description Returns statement execution insights from
// crdb_internal.cluster_execution_insights with enriched contention data.
// @Tags Insights
// @Produce json
// @Param start query string false "Start time (RFC3339)"
// @Param end query string false "End time (RFC3339)"
// @Param stmt_execution_id query string false "Filter by statement execution ID"
// @Param stmt_fingerprint_id query string false "Filter by statement fingerprint ID (hex)"
// @Success 200 {object} StmtInsightsResponse
// @Failure 400 {string} string "Invalid parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /insights/statements [get]
func (api *ApiV2DBConsole) GetStatementInsights(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	// Build main insights query.
	query := safesql.NewQuery()
	query.Append(
		`SELECT session_id, txn_id, txn_fingerprint_id, implicit_txn,
            stmt_id, stmt_fingerprint_id, query, start_time, end_time,
            full_scan, user_name, app_name, database_name,
            rows_read, rows_written, priority, retries,
            contention, last_retry_reason, causes, problem,
            index_recommendations, plan_gist, cpu_sql_nanos,
            error_code, status
     FROM crdb_internal.cluster_execution_insights
     WHERE true`,
	)

	start, end, err := parseTimeRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if start != nil {
		query.Append(` AND start_time >= $`, *start)
	}
	if end != nil {
		query.Append(` AND end_time <= $`, *end)
	}
	if v := r.URL.Query().Get("stmt_execution_id"); v != "" {
		query.Append(` AND stmt_id = $::UUID`, v)
	}
	if v := r.URL.Query().Get("stmt_fingerprint_id"); v != "" {
		query.Append(` AND encode(stmt_fingerprint_id, 'hex') = $`, v)
	}
	query.Append(` ORDER BY start_time DESC`)

	if errs := query.Errors(); len(errs) > 0 {
		srverrors.APIV2InternalError(ctx, errors.CombineErrors(errs[0], errs[len(errs)-1]), w)
		return
	}

	ie := api.InternalDB.Executor()
	override := sessiondata.InternalExecutorOverride{User: sqlUser}

	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-stmt-insights", nil, /* txn */
		override, query.String(), query.QueryArguments()...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	stmts := make([]StmtInsight, 0, len(rows))
	for _, row := range rows {
		stmts = append(stmts, scanStmtInsight(row))
	}

	// Enrich with contention events in a single batch query.
	if len(stmts) > 0 {
		contentionRows, err := ie.QueryBufferedEx(
			ctx, "dbconsole-get-stmt-contention", nil, /* txn */
			override,
			`SELECT waiting_stmt_id, blocking_txn_id,
              contention_duration, key, table_name, index_name
       FROM crdb_internal.transaction_contention_events
       ORDER BY contention_duration DESC`,
		)
		if err == nil {
			// Build lookup by waiting_stmt_id.
			contentionByStmt := make(map[string][]StmtInsightContentionEvent)
			for _, cr := range contentionRows {
				stmtID := tree.AsStringWithFlags(cr[0], tree.FmtBareStrings)
				contentionByStmt[stmtID] = append(contentionByStmt[stmtID],
					StmtInsightContentionEvent{
						BlockingTxnID:      tree.AsStringWithFlags(cr[1], tree.FmtBareStrings),
						ContentionDuration: tree.AsStringWithFlags(cr[2], tree.FmtBareStrings),
						ContendedKey:       tree.AsStringWithFlags(cr[3], tree.FmtBareStrings),
						TableName:          string(tree.MustBeDString(cr[4])),
						IndexName:          string(tree.MustBeDString(cr[5])),
					})
			}
			for i := range stmts {
				if events, ok := contentionByStmt[stmts[i].StmtID]; ok {
					stmts[i].ContentionEvents = events
				}
			}
		}
		// Contention enrichment failure is non-fatal; we still return insights.
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, StmtInsightsResponse{
		Statements: stmts,
	})
}

func scanStmtInsight(row tree.Datums) StmtInsight {
	s := StmtInsight{
		SessionID:            tree.AsStringWithFlags(row[0], tree.FmtBareStrings),
		TxnID:                tree.AsStringWithFlags(row[1], tree.FmtBareStrings),
		TxnFingerprintID:     tree.AsStringWithFlags(row[2], tree.FmtBareStrings),
		ImplicitTxn:          bool(tree.MustBeDBool(row[3])),
		StmtID:               tree.AsStringWithFlags(row[4], tree.FmtBareStrings),
		StmtFingerprintID:    tree.AsStringWithFlags(row[5], tree.FmtBareStrings),
		Query:                string(tree.MustBeDString(row[6])),
		StartTime:            tree.AsStringWithFlags(row[7], tree.FmtBareStrings),
		EndTime:              tree.AsStringWithFlags(row[8], tree.FmtBareStrings),
		FullScan:             bool(tree.MustBeDBool(row[9])),
		UserName:             string(tree.MustBeDString(row[10])),
		AppName:              string(tree.MustBeDString(row[11])),
		DatabaseName:         string(tree.MustBeDString(row[12])),
		RowsRead:             int64(tree.MustBeDInt(row[13])),
		RowsWritten:          int64(tree.MustBeDInt(row[14])),
		Priority:             string(tree.MustBeDString(row[15])),
		Retries:              int64(tree.MustBeDInt(row[16])),
		Causes:               []string{},
		IndexRecommendations: []string{},
	}

	if row[17] != tree.DNull {
		s.Contention = tree.AsStringWithFlags(row[17], tree.FmtBareStrings)
	}
	if row[18] != tree.DNull {
		s.LastRetryReason = string(tree.MustBeDString(row[18]))
	}
	if row[19] != tree.DNull {
		if arr, ok := row[19].(*tree.DArray); ok {
			for _, d := range arr.Array {
				if d != tree.DNull {
					s.Causes = append(s.Causes, string(tree.MustBeDString(d)))
				}
			}
		}
	}
	if row[20] != tree.DNull {
		s.Problem = string(tree.MustBeDString(row[20]))
	}
	if row[21] != tree.DNull {
		if arr, ok := row[21].(*tree.DArray); ok {
			for _, d := range arr.Array {
				if d != tree.DNull {
					s.IndexRecommendations = append(
						s.IndexRecommendations, string(tree.MustBeDString(d)),
					)
				}
			}
		}
	}
	if row[22] != tree.DNull {
		s.PlanGist = string(tree.MustBeDString(row[22]))
	}
	if row[23] != tree.DNull {
		s.CPUSQLNanos = int64(tree.MustBeDInt(row[23]))
	}
	if row[24] != tree.DNull {
		s.ErrorCode = string(tree.MustBeDString(row[24]))
	}
	if row[25] != tree.DNull {
		s.Status = string(tree.MustBeDString(row[25]))
	}

	return s
}

// TxnInsight represents a single transaction execution insight.
type TxnInsight struct {
	SessionID        string   `json:"session_id"`
	TxnID            string   `json:"txn_id"`
	TxnFingerprintID string   `json:"txn_fingerprint_id"`
	ImplicitTxn      bool     `json:"implicit_txn"`
	Query            string   `json:"query"`
	StartTime        string   `json:"start_time"`
	EndTime          string   `json:"end_time"`
	AppName          string   `json:"app_name"`
	UserName         string   `json:"user_name"`
	RowsRead         int64    `json:"rows_read"`
	RowsWritten      int64    `json:"rows_written"`
	Priority         string   `json:"priority"`
	Retries          int64    `json:"retries"`
	Contention       string   `json:"contention,omitempty"`
	Problems         []string `json:"problems"`
	Causes           []string `json:"causes"`
	StmtExecutionIDs []string `json:"stmt_execution_ids"`
	CPUSQLNanos      int64    `json:"cpu_sql_nanos"`
	LastErrorCode    string   `json:"last_error_code,omitempty"`
	Status           string   `json:"status"`
}

// TxnInsightsResponse contains the list of transaction execution insights.
type TxnInsightsResponse struct {
	// Transactions is the list of transaction insights.
	Transactions []TxnInsight `json:"transactions"`
}

// GetTransactionInsights returns transaction execution insights.
//
// ---
// @Summary Get transaction insights
// @Description Returns transaction execution insights from
// crdb_internal.cluster_txn_execution_insights.
// @Tags Insights
// @Produce json
// @Param start query string false "Start time (RFC3339)"
// @Param end query string false "End time (RFC3339)"
// @Param txn_execution_id query string false "Filter by transaction execution ID"
// @Param txn_fingerprint_id query string false "Filter by transaction fingerprint ID (hex)"
// @Success 200 {object} TxnInsightsResponse
// @Failure 400 {string} string "Invalid parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /insights/transactions [get]
func (api *ApiV2DBConsole) GetTransactionInsights(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	query := safesql.NewQuery()
	query.Append(
		`SELECT session_id, txn_id, txn_fingerprint_id, implicit_txn,
            query, start_time, end_time, app_name, user_name,
            rows_read, rows_written, priority, retries,
            contention, problems, causes, stmt_execution_ids,
            cpu_sql_nanos, last_error_code, status
     FROM crdb_internal.cluster_txn_execution_insights
     WHERE true`,
	)

	start, end, err := parseTimeRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if start != nil {
		query.Append(` AND start_time >= $`, *start)
	}
	if end != nil {
		query.Append(` AND end_time <= $`, *end)
	}
	if v := r.URL.Query().Get("txn_execution_id"); v != "" {
		query.Append(` AND txn_id = $::UUID`, v)
	}
	if v := r.URL.Query().Get("txn_fingerprint_id"); v != "" {
		query.Append(` AND encode(txn_fingerprint_id, 'hex') = $`, v)
	}
	query.Append(` ORDER BY start_time DESC`)

	if errs := query.Errors(); len(errs) > 0 {
		srverrors.APIV2InternalError(ctx, errors.CombineErrors(errs[0], errs[len(errs)-1]), w)
		return
	}

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-txn-insights", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	txns := make([]TxnInsight, 0, len(rows))
	for _, row := range rows {
		txns = append(txns, scanTxnInsight(row))
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, TxnInsightsResponse{
		Transactions: txns,
	})
}

func scanTxnInsight(row tree.Datums) TxnInsight {
	t := TxnInsight{
		SessionID:        tree.AsStringWithFlags(row[0], tree.FmtBareStrings),
		TxnID:            tree.AsStringWithFlags(row[1], tree.FmtBareStrings),
		TxnFingerprintID: tree.AsStringWithFlags(row[2], tree.FmtBareStrings),
		ImplicitTxn:      bool(tree.MustBeDBool(row[3])),
		Query:            string(tree.MustBeDString(row[4])),
		StartTime:        tree.AsStringWithFlags(row[5], tree.FmtBareStrings),
		EndTime:          tree.AsStringWithFlags(row[6], tree.FmtBareStrings),
		AppName:          string(tree.MustBeDString(row[7])),
		UserName:         string(tree.MustBeDString(row[8])),
		RowsRead:         int64(tree.MustBeDInt(row[9])),
		RowsWritten:      int64(tree.MustBeDInt(row[10])),
		Priority:         string(tree.MustBeDString(row[11])),
		Retries:          int64(tree.MustBeDInt(row[12])),
		Problems:         []string{},
		Causes:           []string{},
		StmtExecutionIDs: []string{},
	}

	if row[13] != tree.DNull {
		t.Contention = tree.AsStringWithFlags(row[13], tree.FmtBareStrings)
	}
	if row[14] != tree.DNull {
		if arr, ok := row[14].(*tree.DArray); ok {
			for _, d := range arr.Array {
				if d != tree.DNull {
					t.Problems = append(t.Problems, string(tree.MustBeDString(d)))
				}
			}
		}
	}
	if row[15] != tree.DNull {
		if arr, ok := row[15].(*tree.DArray); ok {
			for _, d := range arr.Array {
				if d != tree.DNull {
					t.Causes = append(t.Causes, string(tree.MustBeDString(d)))
				}
			}
		}
	}
	if row[16] != tree.DNull {
		if arr, ok := row[16].(*tree.DArray); ok {
			for _, d := range arr.Array {
				if d != tree.DNull {
					t.StmtExecutionIDs = append(
						t.StmtExecutionIDs,
						tree.AsStringWithFlags(d, tree.FmtBareStrings),
					)
				}
			}
		}
	}
	if row[17] != tree.DNull {
		t.CPUSQLNanos = int64(tree.MustBeDInt(row[17]))
	}
	if row[18] != tree.DNull {
		t.LastErrorCode = string(tree.MustBeDString(row[18]))
	}
	if row[19] != tree.DNull {
		t.Status = string(tree.MustBeDString(row[19]))
	}

	return t
}

// TxnInsightContention represents contention details for a transaction
// insight, enriched with blocking transaction queries.
type TxnInsightContention struct {
	// BlockingTxnID is the ID of the blocking transaction.
	BlockingTxnID string `json:"blocking_txn_id"`
	// BlockingTxnQueries contains the queries run by the blocking transaction.
	BlockingTxnQueries []string `json:"blocking_txn_queries,omitempty"`
	// ContentionDuration is the duration of the contention event.
	ContentionDuration string `json:"contention_duration"`
	// ContendedKey is the key that was contended.
	ContendedKey string `json:"contended_key"`
	// TableName is the table where contention occurred.
	TableName string `json:"table_name"`
	// IndexName is the index where contention occurred.
	IndexName string `json:"index_name"`
	// ContentionType is the type of contention.
	ContentionType string `json:"contention_type"`
}

// TxnInsightDetailsErrors tracks which sub-queries failed during the
// composite transaction insight details request.
type TxnInsightDetailsErrors struct {
	// TxnDetailsErr is set if the transaction details query failed.
	TxnDetailsErr string `json:"txn_details_err,omitempty"`
	// ContentionErr is set if the contention details query failed.
	ContentionErr string `json:"contention_err,omitempty"`
	// StatementsErr is set if the statement insights query failed.
	StatementsErr string `json:"statements_err,omitempty"`
}

// TxnInsightDetailsResponse is the composite response for transaction insight
// details, combining transaction details, statement insights, and contention
// data into a single response.
type TxnInsightDetailsResponse struct {
	// TxnExecutionID is the execution ID being queried.
	TxnExecutionID string `json:"txn_execution_id"`
	// TxnDetails contains the transaction-level insight, if available.
	TxnDetails *TxnInsight `json:"txn_details,omitempty"`
	// Statements contains the statement insights for this transaction.
	Statements []StmtInsight `json:"statements,omitempty"`
	// ContentionDetails contains enriched contention events.
	ContentionDetails []TxnInsightContention `json:"contention_details,omitempty"`
	// Errors tracks which sub-queries failed.
	Errors TxnInsightDetailsErrors `json:"errors"`
}

// GetTransactionInsightDetails returns comprehensive details for a specific
// transaction execution, composing up to 5 internal queries into one
// response. This is the highest-impact BFF endpoint, replacing 3-5 sequential
// frontend round trips.
//
// ---
// @Summary Get transaction insight details
// @Description Returns comprehensive details for a specific transaction
// execution including transaction details, statement insights, and contention
// data.
// @Tags Insights
// @Produce json
// @Param txn_execution_id path string true "Transaction execution ID (UUID)"
// @Param start query string false "Start time (RFC3339)"
// @Param end query string false "End time (RFC3339)"
// @Param exclude_stmts query bool false "Exclude statement insights"
// @Param exclude_txn query bool false "Exclude transaction details"
// @Param exclude_contention query bool false "Exclude contention details"
// @Success 200 {object} TxnInsightDetailsResponse
// @Failure 400 {string} string "Invalid parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /insights/transactions/{txn_execution_id} [get]
func (api *ApiV2DBConsole) GetTransactionInsightDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	txnExecID := mux.Vars(r)["txn_execution_id"]
	if txnExecID == "" {
		http.Error(w, "missing txn_execution_id", http.StatusBadRequest)
		return
	}

	excludeStmts := r.URL.Query().Get("exclude_stmts") == "true"
	excludeTxn := r.URL.Query().Get("exclude_txn") == "true"
	excludeContention := r.URL.Query().Get("exclude_contention") == "true"

	ie := api.InternalDB.Executor()
	override := sessiondata.InternalExecutorOverride{User: sqlUser}

	resp := TxnInsightDetailsResponse{
		TxnExecutionID: txnExecID,
	}

	// 1. Transaction details.
	if !excludeTxn {
		rows, err := ie.QueryBufferedEx(
			ctx, "dbconsole-txn-insight-details", nil, /* txn */
			override,
			`SELECT session_id, txn_id, txn_fingerprint_id, implicit_txn,
              query, start_time, end_time, app_name, user_name,
              rows_read, rows_written, priority, retries,
              contention, problems, causes, stmt_execution_ids,
              cpu_sql_nanos, last_error_code, status
       FROM crdb_internal.cluster_txn_execution_insights
       WHERE txn_id = $1::UUID`,
			txnExecID,
		)
		if err != nil {
			resp.Errors.TxnDetailsErr = err.Error()
		} else if len(rows) > 0 {
			txn := scanTxnInsight(rows[0])
			resp.TxnDetails = &txn
		}
	}

	// 2. Statement insights for this transaction.
	if !excludeStmts {
		rows, err := ie.QueryBufferedEx(
			ctx, "dbconsole-txn-insight-stmts", nil, /* txn */
			override,
			`SELECT session_id, txn_id, txn_fingerprint_id, implicit_txn,
              stmt_id, stmt_fingerprint_id, query, start_time, end_time,
              full_scan, user_name, app_name, database_name,
              rows_read, rows_written, priority, retries,
              contention, last_retry_reason, causes, problem,
              index_recommendations, plan_gist, cpu_sql_nanos,
              error_code, status
       FROM crdb_internal.cluster_execution_insights
       WHERE txn_id = $1::UUID
       ORDER BY start_time ASC`,
			txnExecID,
		)
		if err != nil {
			resp.Errors.StatementsErr = err.Error()
		} else {
			stmts := make([]StmtInsight, 0, len(rows))
			for _, row := range rows {
				stmts = append(stmts, scanStmtInsight(row))
			}
			resp.Statements = stmts
		}
	}

	// 3. Contention details for this transaction.
	if !excludeContention {
		rows, err := ie.QueryBufferedEx(
			ctx, "dbconsole-txn-insight-contention", nil, /* txn */
			override,
			`SELECT blocking_txn_id, contention_duration,
              key, table_name, index_name, contention_type
       FROM crdb_internal.transaction_contention_events
       WHERE waiting_txn_id = $1::UUID
       ORDER BY contention_duration DESC`,
			txnExecID,
		)
		if err != nil {
			resp.Errors.ContentionErr = err.Error()
		} else {
			contention := make([]TxnInsightContention, 0, len(rows))
			for _, row := range rows {
				c := TxnInsightContention{
					BlockingTxnID:      tree.AsStringWithFlags(row[0], tree.FmtBareStrings),
					ContentionDuration: tree.AsStringWithFlags(row[1], tree.FmtBareStrings),
					ContendedKey:       tree.AsStringWithFlags(row[2], tree.FmtBareStrings),
					TableName:          string(tree.MustBeDString(row[3])),
					IndexName:          string(tree.MustBeDString(row[4])),
					ContentionType:     string(tree.MustBeDString(row[5])),
				}
				contention = append(contention, c)
			}
			resp.ContentionDetails = contention
		}
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, resp)
}

// parseTimeRange extracts optional start and end time query parameters.
func parseTimeRange(r *http.Request) (*time.Time, *time.Time, error) {
	var start, end *time.Time
	if v := r.URL.Query().Get("start"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, nil, errors.New("invalid start parameter: expected RFC3339 format")
		}
		start = &t
	}
	if v := r.URL.Query().Get("end"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, nil, errors.New("invalid end parameter: expected RFC3339 format")
		}
		end = &t
	}
	return start, end, nil
}
