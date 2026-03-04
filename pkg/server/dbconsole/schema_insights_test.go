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
	"github.com/stretchr/testify/require"
)

func TestGetSchemaInsights(t *testing.T) {
	t.Run("success with both query types", func(t *testing.T) {
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
				case "dbconsole-schema-unused-indexes":
					// Unused index row: descriptor_name, index_name,
					// index_type, database_name, schema_name,
					// descriptor_id, index_id.
					return []tree.Datums{
						{
							dString("users"),     // descriptor_name
							dString("idx_email"), // index_name
							dString("secondary"), // index_type
							dString("mydb"),      // database_name
							dString("public"),    // schema_name
							dInt(42),             // descriptor_id
							dInt(2),              // index_id
						},
					}, nil
				case "dbconsole-schema-create-index-recs":
					// Create index row: database_name, query_text,
					// query_summary, fingerprint_id,
					// index_recommendations.
					return []tree.Datums{
						{
							dString("mydb"),
							dString("SELECT * FROM orders WHERE customer_id = $1"),
							dString("SELECT FROM orders"),
							dString("abcdef1234567890"),
							dStringArray("CREATE INDEX ON orders (customer_id)"),
						},
					}, nil
				}
				return nil, nil
			},
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/schema", nil,
		)
		w := httptest.NewRecorder()
		api.GetSchemaInsights(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result SchemaInsightsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Recommendations, 2)

		// Unused index recommendation.
		drop := result.Recommendations[0]
		require.Equal(t, "DropIndex", drop.Type)
		require.Equal(t, "mydb", drop.Database)
		require.Equal(t, "DROP INDEX users@idx_email", drop.Query)
		require.NotNil(t, drop.IndexDetails)
		require.Equal(t, "users", drop.IndexDetails.Table)
		require.Equal(t, "idx_email", drop.IndexDetails.IndexName)
		require.Equal(t, "public", drop.IndexDetails.Schema)

		// Create index recommendation.
		create := result.Recommendations[1]
		require.Equal(t, "CreateIndex", create.Type)
		require.Equal(t, "mydb", create.Database)
		require.Contains(t, create.Query, "CREATE INDEX")
		require.NotNil(t, create.Execution)
		require.Equal(t, "abcdef1234567890", create.Execution.FingerprintID)
	})

	t.Run("unused index query error returns 500", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: func(
				ctx context.Context,
				opName redact.RedactableString,
				txn *kv.Txn,
				session sessiondata.InternalExecutorOverride,
				stmt string,
				qargs ...interface{},
			) ([]tree.Datums, error) {
				if string(opName) == "dbconsole-schema-unused-indexes" {
					return nil, errors.New("fatal error")
				}
				return nil, nil
			},
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/schema", nil,
		)
		w := httptest.NewRecorder()
		api.GetSchemaInsights(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("create index query error is non-fatal", func(t *testing.T) {
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
				case "dbconsole-schema-unused-indexes":
					return []tree.Datums{
						{
							dString("users"),
							dString("idx_email"),
							dString("secondary"),
							dString("mydb"),
							dString("public"),
							dInt(42),
							dInt(2),
						},
					}, nil
				case "dbconsole-schema-create-index-recs":
					return nil, errors.New("non-fatal error")
				}
				return nil, nil
			},
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/insights/schema", nil,
		)
		w := httptest.NewRecorder()
		api.GetSchemaInsights(w, req)

		// Still returns 200 with unused index results.
		require.Equal(t, http.StatusOK, w.Code)

		var result SchemaInsightsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Recommendations, 1)
		require.Equal(t, "DropIndex", result.Recommendations[0].Type)
	})
}
