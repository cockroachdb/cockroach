// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
)

// StatementUsingIndex represents a statement that uses a specific index.
type StatementUsingIndex struct {
	// FingerprintID is the hex-encoded statement fingerprint ID.
	FingerprintID string `json:"fingerprint_id"`
	// Query is the statement SQL text.
	Query string `json:"query"`
	// QuerySummary is a short summary of the statement.
	QuerySummary string `json:"query_summary"`
	// ImplicitTxn indicates whether the statement ran in an implicit
	// transaction.
	ImplicitTxn bool `json:"implicit_txn"`
	// Database is the database the statement was executed in.
	Database string `json:"database"`
	// AppName is the application name that executed the statement.
	AppName string `json:"app_name"`
	// Count is the number of times this statement was executed.
	Count int64 `json:"count"`
	// IndexRecommendations is the list of index recommendations for this
	// statement.
	IndexRecommendations []string `json:"index_recommendations"`
}

// StatementsUsingIndexResponse contains statements that use a specific index.
type StatementsUsingIndexResponse struct {
	// Statements is the list of statements using the specified index.
	Statements []StatementUsingIndex `json:"statements"`
}

// GetStatementsUsingIndex returns statements from persisted statistics that
// use a specific index.
//
// ---
// @Summary Get statements using index
// @Description Returns statements from
// crdb_internal.statement_statistics_persisted that reference the specified
// index.
// @Tags IndexDetails
// @Produce json
// @Param table query string true "Table name"
// @Param index query string true "Index name"
// @Param database query string true "Database name"
// @Param start query string false "Start time (RFC3339)"
// @Param end query string false "End time (RFC3339)"
// @Success 200 {object} StatementsUsingIndexResponse
// @Failure 400 {string} string "Missing required parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /index-details/statements [get]
func (api *ApiV2DBConsole) GetStatementsUsingIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	tableName := r.URL.Query().Get("table")
	indexName := r.URL.Query().Get("index")
	database := r.URL.Query().Get("database")

	if tableName == "" || indexName == "" || database == "" {
		http.Error(
			w, "missing required parameters: table, index, database",
			http.StatusBadRequest,
		)
		return
	}

	query := safesql.NewQuery()
	query.Append(
		`SELECT encode(fingerprint_id, 'hex') AS fingerprint_id,
            metadata->>'query' AS query_text,
            metadata->>'querySummary' AS query_summary,
            (metadata->>'implicitTxn')::BOOL AS implicit_txn,
            metadata->>'db' AS database_name,
            app_name,
            (statistics->'statistics'->'cnt')::INT AS exec_count,
            index_recommendations
     FROM crdb_internal.statement_statistics_persisted
     WHERE statistics->'statistics'->'indexes' @> $::JSONB
       AND metadata->>'db' = $`,
		`["`+tableName+`@`+indexName+`"]`, database,
	)

	start, end, err := parseTimeRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if start != nil {
		query.Append(` AND aggregated_ts >= $`, *start)
	}
	if end != nil {
		query.Append(` AND aggregated_ts <= $`, *end)
	}
	query.Append(` ORDER BY exec_count DESC`)

	if errs := query.Errors(); len(errs) > 0 {
		srverrors.APIV2InternalError(ctx, errors.CombineErrors(errs[0], errs[len(errs)-1]), w)
		return
	}

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-stmts-using-index", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	stmts := make([]StatementUsingIndex, 0, len(rows))
	for _, row := range rows {
		s := StatementUsingIndex{
			FingerprintID: string(tree.MustBeDString(row[0])),
			Query:         string(tree.MustBeDString(row[1])),
			QuerySummary:  string(tree.MustBeDString(row[2])),
			ImplicitTxn:   bool(tree.MustBeDBool(row[3])),
			Database:      string(tree.MustBeDString(row[4])),
			AppName:       string(tree.MustBeDString(row[5])),
			Count:         int64(tree.MustBeDInt(row[6])),
		}
		if row[7] != tree.DNull {
			if arr, ok := row[7].(*tree.DArray); ok {
				for _, d := range arr.Array {
					if d != tree.DNull {
						s.IndexRecommendations = append(
							s.IndexRecommendations, string(tree.MustBeDString(d)),
						)
					}
				}
			}
		}
		if s.IndexRecommendations == nil {
			s.IndexRecommendations = []string{}
		}
		stmts = append(stmts, s)
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, StatementsUsingIndexResponse{
		Statements: stmts,
	})
}
