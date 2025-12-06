// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// StatementFingerprint represents a single fingerprint entry.
type StatementFingerprint struct {
	Fingerprint string    `json:"fingerprint"`
	Query       string    `json:"query"`
	Summary     string    `json:"summary"`
	ImplicitTxn bool      `json:"implicit_txn"`
	Database    string    `json:"database"`
	CreatedAt   time.Time `json:"created_at"`
}

// # Get statement fingerprints
//
// Lists statement fingerprints from system.statement_fingerprints table.
// Supports search on fingerprint text with automatic fallback and pagination.
//
// Search behavior: First attempts case-insensitive substring matching (ILIKE).
// If no results are found, falls back to fuzzy similarity matching (trigram).
//
// ---
// parameters:
//   - name: search
//     type: string
//     in: query
//     description: Search query for fingerprint text. Uses substring matching first, then fuzzy matching if no results found.
//     required: false
//   - name: pageSize
//     type: integer
//     in: query
//     description: Maximum number of results to return in this call.
//     required: false
//   - name: pageNum
//     type: integer
//     in: query
//     description: Page number for pagination (1-indexed).
//     required: false
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Statement fingerprints response
//	  schema:
//	    "$ref": "#/definitions/PaginatedResponse"
func (a *apiV2Server) GetStatementFingerprints(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = a.sqlServer.AnnotateCtx(ctx)

	// Only allow GET method
	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	queryParams := r.URL.Query()
	searchQuery := queryParams.Get("search")

	// Parse pagination parameters using the same pattern as other endpoints
	pageNum, err := apiutil.GetIntQueryStringVal(queryParams, pageNumKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageNumKey),
			http.StatusBadRequest)
		return
	}
	pageSize, err := apiutil.GetIntQueryStringVal(queryParams, pageSizeKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageSizeKey),
			http.StatusBadRequest)
		return
	}

	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageNum <= 0 {
		pageNum = defaultPageNum
	}

	// Calculate offset from pageNum (pageNum is 1-indexed)
	offset := (pageNum - 1) * pageSize

	// Helper function to execute query and parse results
	executeQuery := func(query string, qargs []interface{}) ([]StatementFingerprint, int64, error) {
		it, err := a.sqlServer.internalExecutor.QueryIteratorEx(
			ctx, "get-statement-fingerprints", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			query, qargs...,
		)
		if err != nil {
			return nil, 0, err
		}
		defer func() { _ = it.Close() }()

		fingerprints := []StatementFingerprint{}
		var totalRowCount int64

		var ok bool
		setTotalRowCount := true
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()

			// Get total count from first row
			if setTotalRowCount {
				totalRowCount = int64(tree.MustBeDInt(row[6]))
				setTotalRowCount = false
			}
			fp := StatementFingerprint{
				Fingerprint: hex.EncodeToString([]byte(tree.MustBeDBytes(row[0]))),
				Query:       string(tree.MustBeDString(row[1])),
				Summary:     string(tree.MustBeDString(row[2])),
				ImplicitTxn: bool(tree.MustBeDBool(row[3])),
				Database:    string(tree.MustBeDString(row[4])),
				CreatedAt:   tree.MustBeDTimestampTZ(row[5]).Time,
			}
			fingerprints = append(fingerprints, fp)
		}
		if err != nil {
			return nil, 0, err
		}

		return fingerprints, totalRowCount, nil
	}

	var fingerprints []StatementFingerprint
	var totalRowCount int64

	// Build the SQL query with COUNT(*) OVER() for total count
	if searchQuery != "" {
		// First, try ILIKE for substring matching
		query := `SELECT fingerprint, query, summary, implicit_txn, database, created_at, COUNT(*) OVER() AS total_row_count
		          FROM system.statement_fingerprints
		          WHERE query ILIKE $1
		          ORDER BY created_at DESC
		          LIMIT $2`
		qargs := []interface{}{
			"%" + searchQuery + "%",
			pageSize,
		}
		if offset > 0 {
			query += " OFFSET $3"
			qargs = append(qargs, offset)
		}

		var err error
		fingerprints, totalRowCount, err = executeQuery(query, qargs)
		if err != nil {
			srverrors.APIV2InternalError(ctx, err, w)
			return
		}

		// If ILIKE returns no results, fallback to fuzzy matching with %
		if len(fingerprints) == 0 {
			query = `SELECT fingerprint, query, summary, implicit_txn, database, created_at, COUNT(*) OVER() AS total_row_count
			         FROM system.statement_fingerprints
			         WHERE query % $1
			         ORDER BY created_at DESC
			         LIMIT $2`
			qargs = []interface{}{
				searchQuery,
				pageSize,
			}
			if offset > 0 {
				query += " OFFSET $3"
				qargs = append(qargs, offset)
			}

			fingerprints, totalRowCount, err = executeQuery(query, qargs)
			if err != nil {
				srverrors.APIV2InternalError(ctx, err, w)
				return
			}
		}
	} else {
		// No search query - return all results ordered by created_at
		query := `SELECT fingerprint, query, summary, implicit_txn, database, created_at, COUNT(*) OVER() AS total_row_count
		          FROM system.statement_fingerprints
		          ORDER BY created_at DESC
		          LIMIT $1`
		qargs := []interface{}{pageSize}
		if offset > 0 {
			query += " OFFSET $2"
			qargs = append(qargs, offset)
		}

		var err error
		fingerprints, totalRowCount, err = executeQuery(query, qargs)
		if err != nil {
			srverrors.APIV2InternalError(ctx, err, w)
			return
		}
	}

	// Use PaginatedResponse type from api_v2_databases_metadata.go
	resp := PaginatedResponse[[]StatementFingerprint]{
		Results: fingerprints,
		PaginationInfo: paginationInfo{
			TotalResults: totalRowCount,
			PageSize:     pageSize,
			PageNum:      pageNum,
		},
	}

	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}
