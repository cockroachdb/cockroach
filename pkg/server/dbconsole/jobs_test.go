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

	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestCollectJobExecutionDetails(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			execExFn: makeStaticExecEx(1, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/jobs/12345/collect-execution-details", nil,
		)
		req = mux.SetURLVars(req, map[string]string{"job_id": "12345"})

		w := httptest.NewRecorder()
		api.CollectJobExecutionDetails(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result CollectExecutionDetailsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.True(t, result.Success)
	})

	t.Run("invalid job_id", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/jobs/abc/collect-execution-details", nil,
		)
		req = mux.SetURLVars(req, map[string]string{"job_id": "abc"})

		w := httptest.NewRecorder()
		api.CollectJobExecutionDetails(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("wrong method", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/jobs/12345/collect-execution-details", nil,
		)
		req = mux.SetURLVars(req, map[string]string{"job_id": "12345"})

		w := httptest.NewRecorder()
		api.CollectJobExecutionDetails(w, req)

		require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			execExFn: makeStaticExecEx(0, errors.New("execution failed")),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"POST", "/api/v2/dbconsole/jobs/12345/collect-execution-details", nil,
		)
		req = mux.SetURLVars(req, map[string]string{"job_id": "12345"})

		w := httptest.NewRecorder()
		api.CollectJobExecutionDetails(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
