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

func TestGetEvents(t *testing.T) {
	makeEventRow := func(ts, eventType string, reportingID int64, info string, uid []byte) tree.Datums {
		row := tree.Datums{
			dString(ts),
			dString(eventType),
			dInt(reportingID),
			tree.DNull,
			tree.DNull,
		}
		if info != "" {
			row[3] = dString(info)
		}
		if uid != nil {
			row[4] = dBytes(uid)
		}
		return row
	}

	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				makeEventRow(
					"2024-01-15 10:00:00+00", "create_database", 1,
					`{"DatabaseName": "mydb"}`, []byte{0x01, 0x02},
				),
				makeEventRow(
					"2024-01-15 09:00:00+00", "drop_table", 2,
					"", nil,
				),
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest("GET", "/api/v2/dbconsole/events", nil)
		w := httptest.NewRecorder()
		api.GetEvents(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result EventsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Events, 2)

		require.Equal(t, "create_database", result.Events[0].EventType)
		require.Equal(t, int64(1), result.Events[0].ReportingID)
		require.Equal(t, `{"DatabaseName": "mydb"}`, result.Events[0].Info)
		require.Equal(t, []byte{0x01, 0x02}, result.Events[0].UniqueID)

		require.Equal(t, "drop_table", result.Events[1].EventType)
		require.Empty(t, result.Events[1].Info)
		require.Nil(t, result.Events[1].UniqueID)
	})

	t.Run("with type filter", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				makeEventRow(
					"2024-01-15 10:00:00+00", "create_database", 1,
					`{"DatabaseName": "mydb"}`, nil,
				),
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/events?type=create_database", nil,
		)
		w := httptest.NewRecorder()
		api.GetEvents(w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("invalid limit", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/events?limit=-1", nil,
		)
		w := httptest.NewRecorder()
		api.GetEvents(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("invalid offset", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/events?offset=-1", nil,
		)
		w := httptest.NewRecorder()
		api.GetEvents(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx(
				nil, errors.New("query failed"),
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest("GET", "/api/v2/dbconsole/events", nil)
		w := httptest.NewRecorder()
		api.GetEvents(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
