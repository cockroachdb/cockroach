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

func TestGetDatabases(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				{dString("defaultdb")},
				{dString("postgres")},
				{dString("system")},
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest("GET", "/api/v2/dbconsole/databases", nil)
		w := httptest.NewRecorder()
		api.GetDatabases(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result DatabasesResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Equal(t, []string{"defaultdb", "postgres", "system"}, result.Databases)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx(
				nil, errors.New("connection refused"),
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest("GET", "/api/v2/dbconsole/databases", nil)
		w := httptest.NewRecorder()
		api.GetDatabases(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
