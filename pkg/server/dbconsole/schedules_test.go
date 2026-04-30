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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	crjson "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestScanScheduleRow(t *testing.T) {
	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	t.Run("all fields populated", func(t *testing.T) {
		commandJSON, err := crjson.ParseJSON(`{"backup_statement": "BACKUP INTO 'nodelocal://1/backup'"}`)
		require.NoError(t, err)

		row := tree.Datums{
			tree.NewDInt(tree.DInt(123)),                                        // id
			tree.NewDString("daily-backup"),                                     // label
			tree.NewDString("ACTIVE"),                                           // schedule_status
			tree.MustMakeDTimestampTZ(now, time.Microsecond),                    // next_run
			tree.NewDString("WAITING"),                                          // state
			tree.NewDString("@daily"),                                           // recurrence
			tree.NewDInt(tree.DInt(2)),                                          // jobsRunning
			tree.NewDString("root"),                                             // owner
			tree.MustMakeDTimestampTZ(now.Add(-24*time.Hour), time.Microsecond), // created
			tree.NewDJSON(commandJSON),                                          // command (JSONB)
		}

		info, err := scanScheduleRow(row)
		require.NoError(t, err)
		require.Equal(t, int64(123), info.ID)
		require.Equal(t, "daily-backup", info.Label)
		require.Equal(t, "ACTIVE", info.Status)
		require.Equal(t, "2024-06-15T10:30:00Z", info.NextRun)
		require.Equal(t, "WAITING", info.State)
		require.Equal(t, "@daily", info.Recurrence)
		require.Equal(t, int64(2), info.JobsRunning)
		require.Equal(t, "root", info.Owner)
		require.Equal(t, "2024-06-14T10:30:00Z", info.Created)
		require.Contains(t, info.Command, "backup_statement")
	})

	t.Run("nullable fields null", func(t *testing.T) {
		now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
		row := tree.Datums{
			tree.NewDInt(tree.DInt(456)),                     // schedule_id
			tree.NewDString("paused-backup"),                 // schedule_name
			tree.NewDString("PAUSED"),                        // schedule_status
			tree.DNull,                                       // next_run (null when paused)
			tree.DNull,                                       // state (null)
			tree.NewDString("NEVER"),                         // recurrence
			tree.NewDInt(tree.DInt(0)),                       // jobs_running
			tree.NewDString("admin"),                         // owner
			tree.MustMakeDTimestampTZ(now, time.Microsecond), // created
			tree.DNull,                                       // command (null)
		}

		info, err := scanScheduleRow(row)
		require.NoError(t, err)
		require.Equal(t, int64(456), info.ID)
		require.Equal(t, "paused-backup", info.Label)
		require.Equal(t, "PAUSED", info.Status)
		require.Empty(t, info.NextRun)
		require.Empty(t, info.State)
		require.Equal(t, "NEVER", info.Recurrence)
		require.Equal(t, int64(0), info.JobsRunning)
		require.Equal(t, "admin", info.Owner)
		require.Equal(t, "2024-06-15T10:30:00Z", info.Created)
		require.Empty(t, info.Command)
	})

	t.Run("wrong column count", func(t *testing.T) {
		row := tree.Datums{
			tree.NewDInt(tree.DInt(1)),
			tree.NewDString("test"),
		}
		_, err := scanScheduleRow(row)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected 10 columns")
	})
}

func TestGetScheduleInvalidID(t *testing.T) {
	api := &ApiV2DBConsole{}

	router := mux.NewRouter()
	router.HandleFunc(
		"/api/v2/dbconsole/schedules/{schedule_id}", api.GetSchedule,
	).Methods("GET")

	req := httptest.NewRequest("GET", "/api/v2/dbconsole/schedules/not-a-number", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
	require.Equal(t, "invalid schedule_id", errResp.Error)
}

func TestGetSchedulesInvalidLimit(t *testing.T) {
	api := &ApiV2DBConsole{}

	req := httptest.NewRequest("GET", "/api/v2/dbconsole/schedules/?limit=abc", nil)
	w := httptest.NewRecorder()
	api.GetSchedules(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
	require.Equal(t, "invalid limit parameter", errResp.Error)
}

func TestScheduleInfoJSON(t *testing.T) {
	info := ScheduleInfo{
		ID:          1234567890123456789,
		Label:       "test-schedule",
		Status:      "ACTIVE",
		NextRun:     "2024-06-15T10:30:00Z",
		State:       "WAITING",
		Recurrence:  "@daily",
		JobsRunning: 1,
		Owner:       "root",
		Created:     "2024-06-14T10:30:00Z",
		Command:     `{"backup_statement": "BACKUP"}`,
	}

	data, err := json.Marshal(info)
	require.NoError(t, err)

	// Verify int64 ID is serialized as a quoted string.
	require.Contains(t, string(data), `"id":"1234567890123456789"`)

	// Verify round-trip.
	var decoded ScheduleInfo
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, info.ID, decoded.ID)
}
