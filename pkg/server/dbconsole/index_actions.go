// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// DropSchemaIndexRequest contains the identifiers for the index to drop.
type DropSchemaIndexRequest struct {
	// Database is the database containing the table.
	Database string `json:"database"`
	// Schema is the schema containing the table (e.g. "public").
	Schema string `json:"schema"`
	// Table is the table name.
	Table string `json:"table"`
	// Index is the index name.
	Index string `json:"index"`
}

// CreateSchemaIndexRequest contains a fingerprint ID and a specific
// recommendation string from the statement's index_recommendations array.
type CreateSchemaIndexRequest struct {
	// FingerprintID is the hex-encoded fingerprint ID of the statement.
	FingerprintID string `json:"fingerprint_id"`
	// Recommendation is the CREATE INDEX recommendation string, which
	// must match an entry in the statement's index_recommendations array.
	Recommendation string `json:"recommendation"`
	// Database is the database context for execution.
	Database string `json:"database"`
}

// DropSchemaIndex drops a specific index identified by database, schema, table,
// and index name. The SQL statement is constructed entirely from AST nodes,
// ensuring proper quoting and preventing SQL injection.
//
// ---
// @Summary Drop a schema index
// @Description Drops a specific index by constructing a DROP INDEX statement
// from structured identifiers.
// @Tags IndexActions
// @Accept json
// @Produce json
// @Param body body DropSchemaIndexRequest true "Index identifiers"
// @Success 200 {string} string "ok"
// @Failure 400 {string} string "Invalid request"
// @Failure 500 {string} string "Internal server error"
// @Security ApiKeyAuth
// @Router /insights/schema/drop-index [post]
func (api *ApiV2DBConsole) DropSchemaIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	var req DropSchemaIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Database == "" || req.Schema == "" || req.Table == "" ||
		req.Index == "" {
		http.Error(
			w, "database, schema, table, and index are required",
			http.StatusBadRequest,
		)
		return
	}

	tableName := tree.MakeTableNameFromPrefix(
		tree.ObjectNamePrefix{
			SchemaName:     tree.Name(req.Schema),
			ExplicitSchema: true,
		},
		tree.Name(req.Table),
	)
	dropStmt := &tree.DropIndex{
		IndexList: tree.TableIndexNames{
			{
				Table: tableName,
				Index: tree.UnrestrictedName(req.Index),
			},
		},
	}

	ie := api.InternalDB.Executor()
	override := sessiondata.InternalExecutorOverride{
		User:     sqlUser,
		Database: req.Database,
	}

	_, err := ie.ExecEx(
		ctx, "dbconsole-drop-schema-index", nil, /* txn */
		override, tree.AsString(dropStmt),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, "ok")
}

// CreateSchemaIndex creates an index based on a recommendation from statement
// statistics. The recommendation is verified against the statement's persisted
// index_recommendations array, then parsed and validated as a CREATE INDEX
// statement before execution.
//
// ---
// @Summary Create a schema index from a recommendation
// @Description Looks up a statement by fingerprint ID, verifies the
// recommendation exists in the statement's index_recommendations array,
// validates it as a CREATE INDEX statement, and executes it.
// @Tags IndexActions
// @Accept json
// @Produce json
// @Param body body CreateSchemaIndexRequest true "Recommendation details"
// @Success 200 {string} string "ok"
// @Failure 400 {string} string "Invalid request"
// @Failure 500 {string} string "Internal server error"
// @Security ApiKeyAuth
// @Router /insights/schema/create-index [post]
func (api *ApiV2DBConsole) CreateSchemaIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	var req CreateSchemaIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.FingerprintID == "" || req.Recommendation == "" ||
		req.Database == "" {
		http.Error(
			w, "fingerprint_id, recommendation, and database are required",
			http.StatusBadRequest,
		)
		return
	}

	ie := api.InternalDB.Executor()
	override := sessiondata.InternalExecutorOverride{
		User:     sqlUser,
		Database: req.Database,
	}

	// Look up the statement's index recommendations by fingerprint ID.
	row, err := ie.QueryRowEx(
		ctx, "dbconsole-lookup-index-rec", nil, /* txn */
		override,
		`SELECT index_recommendations
     FROM crdb_internal.statement_statistics_persisted
     WHERE encode(fingerprint_id, 'hex') = $1
     ORDER BY aggregated_ts DESC
     LIMIT 1`,
		req.FingerprintID,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if row == nil {
		http.Error(
			w, "fingerprint not found", http.StatusBadRequest,
		)
		return
	}

	// Verify the recommendation exists in the index_recommendations array.
	if err := verifyRecommendationInArray(
		row[0], req.Recommendation,
	); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the recommendation and verify it is a CREATE INDEX statement.
	stmts, err := parser.Parse(req.Recommendation)
	if err != nil || len(stmts) != 1 {
		http.Error(
			w, "recommendation is not valid SQL", http.StatusBadRequest,
		)
		return
	}
	if _, ok := stmts[0].AST.(*tree.CreateIndex); !ok {
		http.Error(
			w, "recommendation is not a CREATE INDEX statement",
			http.StatusBadRequest,
		)
		return
	}

	_, err = ie.ExecEx(
		ctx, "dbconsole-create-schema-index", nil, /* txn */
		override, req.Recommendation,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, "ok")
}

// verifyRecommendationInArray checks that the recommendation string exists
// verbatim in the given datum, which is expected to be a string array from
// the index_recommendations column.
func verifyRecommendationInArray(datum tree.Datum, recommendation string) error {
	if datum == tree.DNull {
		return errors.New("no index recommendations found for this statement")
	}
	arr, ok := datum.(*tree.DArray)
	if !ok {
		return errors.New("unexpected type for index_recommendations")
	}
	for _, d := range arr.Array {
		if d == tree.DNull {
			continue
		}
		if string(tree.MustBeDString(d)) == recommendation {
			return nil
		}
	}
	return errors.New("recommendation not found in statement's index recommendations")
}
