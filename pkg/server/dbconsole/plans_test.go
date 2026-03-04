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

func TestGetExplainPlan(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			queryRowExFn: makeStaticQueryRowEx(
				tree.Datums{dString("• scan\n  table: users@primary")}, nil,
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/explain-plan?plan_gist=AgHQAQIAiAE", nil,
		)
		w := httptest.NewRecorder()
		api.GetExplainPlan(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result ExplainPlanResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Equal(t, "• scan\n  table: users@primary", result.ExplainPlan)
	})

	t.Run("missing plan_gist", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/explain-plan", nil,
		)
		w := httptest.NewRecorder()
		api.GetExplainPlan(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("null result", func(t *testing.T) {
		exec := &mockExecutor{
			queryRowExFn: makeStaticQueryRowEx(
				tree.Datums{tree.DNull}, nil,
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/explain-plan?plan_gist=invalid", nil,
		)
		w := httptest.NewRecorder()
		api.GetExplainPlan(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result ExplainPlanResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Empty(t, result.ExplainPlan)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryRowExFn: makeStaticQueryRowEx(nil, errors.New("decode error")),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/explain-plan?plan_gist=abc", nil,
		)
		w := httptest.NewRecorder()
		api.GetExplainPlan(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestGetIndexRecommendations(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				{dStringArray(
					"CREATE INDEX ON users (name)",
					"CREATE INDEX ON users (email)",
				)},
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET",
			"/api/v2/dbconsole/index-recommendations?plan_gist=abc&app_name=test&query=SELECT+1",
			nil,
		)
		w := httptest.NewRecorder()
		api.GetIndexRecommendations(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result IndexRecommendationsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Equal(t, []string{
			"CREATE INDEX ON users (name)",
			"CREATE INDEX ON users (email)",
		}, result.Recommendations)
	})

	t.Run("missing params", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/index-recommendations?plan_gist=abc", nil,
		)
		w := httptest.NewRecorder()
		api.GetIndexRecommendations(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("no results", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET",
			"/api/v2/dbconsole/index-recommendations?plan_gist=abc&app_name=test&query=SELECT+1",
			nil,
		)
		w := httptest.NewRecorder()
		api.GetIndexRecommendations(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result IndexRecommendationsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Equal(t, []string{}, result.Recommendations)
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
			"/api/v2/dbconsole/index-recommendations?plan_gist=abc&app_name=test&query=SELECT+1",
			nil,
		)
		w := httptest.NewRecorder()
		api.GetIndexRecommendations(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
