// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestGetStatementsUsingIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		// Row: fingerprint_id, query_text, query_summary, implicit_txn,
		// database_name, app_name, exec_count, index_recommendations.
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				{
					dString("abcdef1234567890"),
					dString("SELECT * FROM users WHERE name = $1"),
					dString("SELECT FROM users"),
					dBool(true),
					dString("mydb"),
					dString("myapp"),
					dInt(150),
					dStringArray("CREATE INDEX ON users (name)"),
				},
				{
					dString("1234567890abcdef"),
					dString("UPDATE users SET email = $1 WHERE id = $2"),
					dString("UPDATE users"),
					dBool(false),
					dString("mydb"),
					dString("otherapp"),
					dInt(50),
					tree.DNull,
				},
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET",
			"/api/v2/dbconsole/index-details/statements?table=users&index=users_pkey&database=mydb",
			nil,
		)
		w := httptest.NewRecorder()
		api.GetStatementsUsingIndex(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result StatementsUsingIndexResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Statements, 2)

		s := result.Statements[0]
		require.Equal(t, "abcdef1234567890", s.FingerprintID)
		require.Equal(t, "SELECT * FROM users WHERE name = $1", s.Query)
		require.Equal(t, "SELECT FROM users", s.QuerySummary)
		require.True(t, s.ImplicitTxn)
		require.Equal(t, "mydb", s.Database)
		require.Equal(t, "myapp", s.AppName)
		require.Equal(t, int64(150), s.Count)
		require.Equal(t, []string{
			"CREATE INDEX ON users (name)",
		}, s.IndexRecommendations)

		// Second row: null index_recommendations → empty array.
		s2 := result.Statements[1]
		require.Equal(t, []string{}, s2.IndexRecommendations)
	})

	t.Run("missing params", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET",
			"/api/v2/dbconsole/index-details/statements?table=users",
			nil,
		)
		w := httptest.NewRecorder()
		api.GetStatementsUsingIndex(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx(
				nil, errors.New("query failed"),
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET",
			"/api/v2/dbconsole/index-details/statements?table=users&index=users_pkey&database=mydb",
			nil,
		)
		w := httptest.NewRecorder()
		api.GetStatementsUsingIndex(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
