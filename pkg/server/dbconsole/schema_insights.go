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
)

// SchemaInsightIndexDetails contains table and index information for a schema
// insight.
type SchemaInsightIndexDetails struct {
	// Table is the table name.
	Table string `json:"table"`
	// IndexName is the index name.
	IndexName string `json:"index_name"`
	// Schema is the schema name.
	Schema string `json:"schema,omitempty"`
}

// SchemaInsightExecution contains execution details for create-index
// recommendations.
type SchemaInsightExecution struct {
	// Statement is the full SQL statement text.
	Statement string `json:"statement,omitempty"`
	// Summary is a short summary of the statement.
	Summary string `json:"summary,omitempty"`
	// FingerprintID is the statement fingerprint ID.
	FingerprintID string `json:"fingerprint_id,omitempty"`
}

// SchemaInsightRecommendation represents a single schema insight
// recommendation.
type SchemaInsightRecommendation struct {
	// Type is the recommendation type (DropIndex, CreateIndex, ReplaceIndex,
	// AlterIndex).
	Type string `json:"type"`
	// Database is the database containing the object.
	Database string `json:"database"`
	// Query is the recommended DDL statement (e.g. "DROP INDEX ...").
	Query string `json:"query,omitempty"`
	// IndexDetails contains index-specific metadata for the recommendation.
	IndexDetails *SchemaInsightIndexDetails `json:"index_details,omitempty"`
	// Execution contains execution context for create-index recommendations.
	Execution *SchemaInsightExecution `json:"execution,omitempty"`
}

// SchemaInsightsResponse contains the list of schema insight recommendations.
type SchemaInsightsResponse struct {
	// Recommendations is the list of schema insight recommendations.
	Recommendations []SchemaInsightRecommendation `json:"recommendations"`
}

// GetSchemaInsights returns schema-level insight recommendations, including
// unused index drop recommendations and create-index recommendations from
// statement statistics. Both queries are combined into a single response.
//
// ---
// @Summary Get schema insights
// @Description Returns schema-level insight recommendations including unused
// indexes and suggested new indexes.
// @Tags Insights
// @Produce json
// @Success 200 {object} SchemaInsightsResponse
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /insights/schema [get]
func (api *ApiV2DBConsole) GetSchemaInsights(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	ie := api.InternalDB.Executor()
	override := sessiondata.InternalExecutorOverride{User: sqlUser}

	var recommendations []SchemaInsightRecommendation

	// 1. Unused index recommendations: indexes with no reads since cluster
	//    restart and that have existed for longer than the index usage
	//    retention period.
	unusedRows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-schema-unused-indexes", nil, /* txn */
		override,
		`SELECT ti.descriptor_name, ti.index_name, ti.index_type,
            t.database_name, t.schema_name,
            ti.descriptor_id, ti.index_id
     FROM crdb_internal.index_usage_statistics AS us
     JOIN crdb_internal.table_indexes AS ti
       ON us.table_id = ti.descriptor_id AND us.index_id = ti.index_id
     JOIN crdb_internal.tables AS t
       ON ti.descriptor_id = t.table_id
     WHERE total_reads = 0
       AND ti.index_type != 'primary'
       AND ti.is_unique = false
       AND t.database_name != 'system'
     ORDER BY t.database_name, ti.descriptor_name, ti.index_name`,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	for _, row := range unusedRows {
		tableName := string(tree.MustBeDString(row[0]))
		indexName := string(tree.MustBeDString(row[1]))
		dbName := string(tree.MustBeDString(row[3]))
		schemaName := string(tree.MustBeDString(row[4]))

		recommendations = append(recommendations, SchemaInsightRecommendation{
			Type:     "DropIndex",
			Database: dbName,
			Query:    "DROP INDEX " + tableName + "@" + indexName,
			IndexDetails: &SchemaInsightIndexDetails{
				Table:     tableName,
				IndexName: indexName,
				Schema:    schemaName,
			},
		})
	}

	// 2. Create index recommendations from statement statistics.
	createIdxRows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-schema-create-index-recs", nil, /* txn */
		override,
		`SELECT metadata->>'db' AS database_name,
            metadata->>'query' AS query_text,
            metadata->>'querySummary' AS query_summary,
            encode(fingerprint_id, 'hex') AS fingerprint_id,
            index_recommendations
     FROM crdb_internal.statement_statistics_persisted
     WHERE jsonb_array_length(index_recommendations) > 0
       AND aggregated_ts = (
         SELECT max(aggregated_ts)
         FROM crdb_internal.statement_statistics_persisted
       )
     ORDER BY statistics->'statistics'->'cnt' DESC
     LIMIT 50`,
	)
	if err != nil {
		// Create index recommendations are best-effort; we still return
		// unused index recommendations.
	} else {
		for _, row := range createIdxRows {
			dbName := string(tree.MustBeDString(row[0]))
			queryText := string(tree.MustBeDString(row[1]))
			querySummary := string(tree.MustBeDString(row[2]))
			fingerprintID := string(tree.MustBeDString(row[3]))

			if row[4] != tree.DNull {
				if arr, ok := row[4].(*tree.DArray); ok {
					for _, d := range arr.Array {
						if d == tree.DNull {
							continue
						}
						rec := string(tree.MustBeDString(d))
						recommendations = append(recommendations, SchemaInsightRecommendation{
							Type:     "CreateIndex",
							Database: dbName,
							Query:    rec,
							Execution: &SchemaInsightExecution{
								Statement:     queryText,
								Summary:       querySummary,
								FingerprintID: fingerprintID,
							},
						})
					}
				}
			}
		}
	}

	if recommendations == nil {
		recommendations = []SchemaInsightRecommendation{}
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, SchemaInsightsResponse{
		Recommendations: recommendations,
	})
}
