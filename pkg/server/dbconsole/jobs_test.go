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
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestScanRowToJobInfo(t *testing.T) {
	created := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	finished := time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC)
	modified := time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC)

	mustTimestampTZ := func(t time.Time) *tree.DTimestampTZ {
		d, err := tree.MakeDTimestampTZ(t, time.Microsecond)
		if err != nil {
			panic(err)
		}
		return d
	}

	t.Run("full row with all fields", func(t *testing.T) {
		// Build a highwater decimal: 1705312260.0000000000
		hwDecimal := &tree.DDecimal{}
		require.NoError(t, hwDecimal.SetString("1705312260.0000000000"))

		row := tree.Datums{
			tree.NewDInt(123),                  // job_id
			tree.NewDString("BACKUP"),          // job_type
			tree.NewDString("backing up db"),   // description
			tree.NewDString("BACKUP INTO 'x'"), // statement
			tree.NewDString("root"),            // user_name
			tree.NewDString("succeeded"),       // status
			tree.NewDString("uploading"),       // running_status
			mustTimestampTZ(created),           // created
			mustTimestampTZ(finished),          // finished
			mustTimestampTZ(modified),          // modified
			tree.NewDFloat(tree.DFloat(1.0)),   // fraction_completed
			hwDecimal,                          // high_water_timestamp
			tree.NewDString(""),                // error
			tree.NewDInt(1),                    // coordinator_id
		}

		job, err := scanRowToJobInfo(row)
		require.NoError(t, err)

		require.Equal(t, int64(123), job.ID)
		require.Equal(t, "BACKUP", job.Type)
		require.Equal(t, "backing up db", job.Description)
		require.Equal(t, "BACKUP INTO 'x'", job.Statement)
		require.Equal(t, "root", job.Username)
		require.Equal(t, "succeeded", job.Status)
		require.Equal(t, "uploading", job.RunningStatus)
		require.Equal(t, "2024-01-15T10:30:00Z", job.Created)
		require.Equal(t, "2024-01-15T11:00:00Z", job.Finished)
		require.Equal(t, "2024-01-15T11:00:00Z", job.Modified)
		require.Equal(t, float32(1.0), job.FractionCompleted)
		require.NotEmpty(t, job.HighwaterTimestamp)
		require.Equal(t, "1705312260.0000000000", job.HighwaterDecimal)
		require.Equal(t, int64(1), job.CoordinatorID)
	})

	t.Run("nullable fields as NULL", func(t *testing.T) {
		row := tree.Datums{
			tree.NewDInt(456),               // job_id
			tree.NewDString("RESTORE"),      // job_type
			tree.NewDString("restoring db"), // description
			tree.NewDString(""),             // statement
			tree.NewDString("admin"),        // user_name
			tree.NewDString("running"),      // status
			tree.DNull,                      // running_status (NULL)
			mustTimestampTZ(created),        // created
			tree.DNull,                      // finished (NULL)
			mustTimestampTZ(modified),       // modified
			tree.DNull,                      // fraction_completed (NULL)
			tree.DNull,                      // high_water_timestamp (NULL)
			tree.NewDString(""),             // error
			tree.DNull,                      // coordinator_id (NULL)
		}

		job, err := scanRowToJobInfo(row)
		require.NoError(t, err)

		require.Equal(t, int64(456), job.ID)
		require.Equal(t, "RESTORE", job.Type)
		require.Equal(t, "running", job.Status)
		require.Equal(t, "", job.RunningStatus)
		require.Equal(t, "", job.Finished)
		require.Equal(t, float32(0), job.FractionCompleted)
		require.Equal(t, "", job.HighwaterTimestamp)
		require.Equal(t, "", job.HighwaterDecimal)
		require.Equal(t, int64(0), job.CoordinatorID)
	})

	t.Run("wrong column count", func(t *testing.T) {
		row := tree.Datums{tree.NewDInt(1)}
		_, err := scanRowToJobInfo(row)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected 14 columns")
	})
}

func TestDatumToTimeStr(t *testing.T) {
	tests := []struct {
		name string
		d    tree.Datum
		want string
	}{
		{"null", tree.DNull, ""},
		{
			"valid",
			func() tree.Datum {
				d, _ := tree.MakeDTimestampTZ(
					time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
					time.Microsecond,
				)
				return d
			}(),
			"2024-01-15T10:30:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, datumToTimeStr(tt.d))
		})
	}
}

func TestGetJobInvalidID(t *testing.T) {
	api := &ApiV2DBConsole{}
	router := mux.NewRouter()
	router.HandleFunc(
		"/api/v2/dbconsole/jobs/{job_id}", api.GetJob,
	).Methods("GET")

	req := httptest.NewRequest(
		"GET", "/api/v2/dbconsole/jobs/not-a-number", nil,
	)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
	require.Equal(t, "invalid job_id parameter", errResp.Error)
}

func TestGetJobsInvalidParams(t *testing.T) {
	api := &ApiV2DBConsole{}

	t.Run("invalid type", func(t *testing.T) {
		req := httptest.NewRequest(
			"GET", "/api/v2/dbconsole/jobs?type=notanumber", nil,
		)
		w := httptest.NewRecorder()
		api.GetJobs(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
		require.Equal(t, "invalid type parameter", errResp.Error)
	})

	t.Run("invalid limit", func(t *testing.T) {
		req := httptest.NewRequest(
			"GET", "/api/v2/dbconsole/jobs?limit=notanumber", nil,
		)
		w := httptest.NewRecorder()
		api.GetJobs(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
		require.Equal(t, "invalid limit parameter", errResp.Error)
	})
}

func TestHighwaterDecimalConversion(t *testing.T) {
	// Verify that a decimal representing a CockroachDB HLC timestamp
	// converts correctly to an RFC3339 timestamp string.
	hwDecimal := &tree.DDecimal{}
	require.NoError(t, hwDecimal.SetString("1705312260.0000000000"))

	// The row has a non-null highwater timestamp.
	created := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	mustTimestampTZ := func(t time.Time) *tree.DTimestampTZ {
		d, err := tree.MakeDTimestampTZ(t, time.Microsecond)
		if err != nil {
			panic(err)
		}
		return d
	}

	row := tree.Datums{
		tree.NewDInt(1),
		tree.NewDString("CHANGEFEED"),
		tree.NewDString("desc"),
		tree.NewDString(""),
		tree.NewDString("root"),
		tree.NewDString("running"),
		tree.DNull,
		mustTimestampTZ(created),
		tree.DNull,
		mustTimestampTZ(created),
		tree.DNull,
		hwDecimal,
		tree.NewDString(""),
		tree.DNull,
	}

	job, err := scanRowToJobInfo(row)
	require.NoError(t, err)
	require.NotEmpty(t, job.HighwaterTimestamp)
	require.Equal(t, "1705312260.0000000000", job.HighwaterDecimal)

	// Verify the timestamp parses as valid RFC3339.
	_, err = time.Parse(time.RFC3339, job.HighwaterTimestamp)
	require.NoError(t, err)
}

// TestJobInfoJSONSerialization verifies the JSON output matches the expected
// shape that the frontend consumes, particularly the string-encoded job ID.
func TestJobInfoJSONSerialization(t *testing.T) {
	job := JobInfo{
		ID:                9007199254740993, // > 2^53, must serialize as string
		Type:              "BACKUP",
		Status:            "running",
		ExecutionFailures: []JobExecutionFailure{},
		Messages:          []JobMessageInfo{},
	}

	data, err := json.Marshal(job)
	require.NoError(t, err)

	// The ID should be a quoted string in JSON.
	require.Contains(t, string(data), `"id":"9007199254740993"`)

	// Verify round-trip.
	var decoded JobInfo
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, job.ID, decoded.ID)
}
