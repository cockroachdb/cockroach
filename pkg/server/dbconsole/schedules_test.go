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
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// makeScheduleRow builds a tree.Datums row matching the schedule query
// columns: id, label, schedule_status, next_run, state, recurrence,
// num_running, owner, created, command.
func makeScheduleRow(
	id int64,
	label, status, nextRun, state, recurrence string,
	numRunning int64,
	owner, created, command string,
) tree.Datums {
	row := tree.Datums{
		dInt(id),
		dString(label),
		dString(status),
		tree.DNull, // next_run
		tree.DNull, // state
		tree.DNull, // recurrence
		dInt(numRunning),
		dString(owner),
		dString(created),
		tree.DNull, // command
	}
	if nextRun != "" {
		row[3] = dString(nextRun)
	}
	if state != "" {
		row[4] = dString(state)
	}
	if recurrence != "" {
		row[5] = dString(recurrence)
	}
	if command != "" {
		row[9] = dString(command)
	}
	return row
}

func TestGetSchedules(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				makeScheduleRow(
					1, "backup-daily", "ACTIVE",
					"2024-01-16 00:00:00+00", "",
					"@daily", 0, "root",
					"2024-01-01 00:00:00+00", "BACKUP INTO ...",
				),
				makeScheduleRow(
					2, "gc-job", "PAUSED",
					"", "waiting for gc",
					"", 1, "node",
					"2024-01-01 00:00:00+00", "",
				),
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest("GET", "/api/v2/dbconsole/schedules", nil)
		w := httptest.NewRecorder()
		api.GetSchedules(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result SchedulesResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Schedules, 2)

		s := result.Schedules[0]
		require.Equal(t, int64(1), s.ID)
		require.Equal(t, "backup-daily", s.Label)
		require.Equal(t, "ACTIVE", s.ScheduleStatus)
		require.Equal(t, "@daily", s.Recurrence)
		require.Equal(t, "root", s.Owner)
		require.Equal(t, "BACKUP INTO ...", s.Command)

		s2 := result.Schedules[1]
		require.Equal(t, int64(2), s2.ID)
		require.Equal(t, "PAUSED", s2.ScheduleStatus)
		require.Equal(t, int64(1), s2.JobsRunning)
		require.Empty(t, s2.NextRun)
		require.Empty(t, s2.Command)
	})

	t.Run("with status filter", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				makeScheduleRow(
					1, "backup-daily", "ACTIVE",
					"2024-01-16 00:00:00+00", "", "@daily",
					0, "root", "2024-01-01 00:00:00+00", "",
				),
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/schedules?status=ACTIVE", nil,
		)
		w := httptest.NewRecorder()
		api.GetSchedules(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result SchedulesResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Schedules, 1)
	})

	t.Run("invalid limit", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/schedules?limit=abc", nil,
		)
		w := httptest.NewRecorder()
		api.GetSchedules(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx(
				nil, errors.New("query failed"),
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest("GET", "/api/v2/dbconsole/schedules", nil)
		w := httptest.NewRecorder()
		api.GetSchedules(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestGetSchedule(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				makeScheduleRow(
					42, "backup-daily", "ACTIVE",
					"2024-01-16 00:00:00+00", "",
					"@daily", 0, "root",
					"2024-01-01 00:00:00+00", "BACKUP INTO ...",
				),
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/schedules/42", nil,
		)
		req = mux.SetURLVars(req, map[string]string{"schedule_id": "42"})

		w := httptest.NewRecorder()
		api.GetSchedule(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result ScheduleInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Equal(t, int64(42), result.ID)
		require.Equal(t, "backup-daily", result.Label)
	})

	t.Run("not found", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/schedules/999", nil,
		)
		req = mux.SetURLVars(req, map[string]string{"schedule_id": "999"})

		w := httptest.NewRecorder()
		api.GetSchedule(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("invalid schedule_id", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/schedules/abc", nil,
		)
		req = mux.SetURLVars(req, map[string]string{"schedule_id": "abc"})

		w := httptest.NewRecorder()
		api.GetSchedule(w, req)

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
			"GET", "/api/v2/dbconsole/schedules/42", nil,
		)
		req = mux.SetURLVars(req, map[string]string{"schedule_id": "42"})

		w := httptest.NewRecorder()
		api.GetSchedule(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
