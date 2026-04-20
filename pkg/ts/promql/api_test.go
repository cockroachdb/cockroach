// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// newTestAPI creates an API backed by a mock TSDB server for testing.
func newTestAPI(mock *mockTSDBServer) *API {
	catalog := NewMetricCatalog(
		map[string]string{
			"sql.conn.count":  "cr.node.sql.conn.count",
			"sql.query.count": "cr.node.sql.query.count",
			"livebytes":       "cr.store.livebytes",
		},
		nil,
	)
	sources := &staticSourceLister{
		nodes:  []string{"1", "2"},
		stores: []string{"1"},
	}
	queryable := NewTSDBQueryable(mock, catalog, sources)
	engine := NewDefaultEngine()
	return NewAPI(engine, queryable, catalog)
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) apiResponse {
	t.Helper()
	var resp apiResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	return resp
}

func TestParseTimeParam(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		paramVal    string
		expectedTs  time.Time
		expectedErr bool
	}{
		{
			name:       "missing uses default",
			paramVal:   "",
			expectedTs: defaultTime,
		},
		{
			name:       "unix timestamp integer",
			paramVal:   "1609459200",
			expectedTs: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:       "unix timestamp float",
			paramVal:   "1609459200.5",
			expectedTs: time.Date(2021, 1, 1, 0, 0, 0, 500_000_000, time.UTC),
		},
		{
			name:       "RFC3339",
			paramVal:   "2021-01-01T00:00:00Z",
			expectedTs: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:        "invalid format",
			paramVal:    "not_a_time",
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{}
			if tc.paramVal != "" {
				form.Set("time", tc.paramVal)
			}
			r := httptest.NewRequest("GET", "/?"+form.Encode(), nil)
			result, err := parseTimeParam(r, "time", defaultTime)
			if tc.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.WithinDuration(t, tc.expectedTs, result, time.Millisecond)
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		input       string
		expected    time.Duration
		expectedErr bool
	}{
		{name: "Go format seconds", input: "30s", expected: 30 * time.Second},
		{name: "Go format minutes", input: "2m", expected: 2 * time.Minute},
		{name: "float seconds", input: "15", expected: 15 * time.Second},
		{name: "float sub-second", input: "0.5", expected: 500 * time.Millisecond},
		{name: "invalid", input: "invalid", expectedErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := parseDuration(tc.input)
			if tc.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, d)
			}
		})
	}
}

func TestInstantQuery_MissingQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	api := newTestAPI(mock)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/query", nil)
	api.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := decodeResponse(t, rec)
	require.Equal(t, "error", resp.Status)
	require.Equal(t, "bad_data", resp.ErrorType)
}

func TestInstantQuery_Success(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 42))}
	api := newTestAPI(mock)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/v1/query?query=sql_conn_count&time=1609459200", nil)
	api.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	resp := decodeResponse(t, rec)
	require.Equal(t, "success", resp.Status)
	require.NotNil(t, resp.Data)
}

func TestRangeQuery_MissingParams(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	api := newTestAPI(mock)

	tests := []struct {
		name  string
		query string
	}{
		{name: "missing query", query: "start=1&end=2&step=1s"},
		{name: "missing start", query: "query=up&end=2&step=1s"},
		{name: "missing end", query: "query=up&start=1&step=1s"},
		{name: "missing step", query: "query=up&start=1&end=2"},
		{name: "zero step", query: "query=up&start=1&end=2&step=0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET",
				"/api/v1/query_range?"+tc.query, nil)
			api.Handler().ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestRangeQuery_TooManyPoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	api := newTestAPI(mock)

	// 11001 points: start=0, end=11001, step=1s.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/v1/query_range?query=sql_conn_count&start=0&end=11001&step=1s", nil)
	api.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := decodeResponse(t, rec)
	require.Contains(t, resp.Error, "11,000")
}

func TestRangeQuery_Success(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 42))}
	api := newTestAPI(mock)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/v1/query_range?query=sql_conn_count&start=0&end=60&step=15s", nil)
	api.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	resp := decodeResponse(t, rec)
	require.Equal(t, "success", resp.Status)
}

func TestLabelNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	api := newTestAPI(mock)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/labels", nil)
	api.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	resp := decodeResponse(t, rec)
	require.Equal(t, "success", resp.Status)

	// Data should be a JSON array of label names.
	data, err := json.Marshal(resp.Data)
	require.NoError(t, err)
	var names []string
	require.NoError(t, json.Unmarshal(data, &names))
	require.Contains(t, names, "__name__")
	require.Contains(t, names, "instance_type")
	require.Contains(t, names, "node_id")
	require.Contains(t, names, "store_id")
}

func TestLabelValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	api := newTestAPI(mock)

	t.Run("__name__", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/label/__name__/values", nil)
		api.Handler().ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		resp := decodeResponse(t, rec)
		data, _ := json.Marshal(resp.Data)
		var names []string
		require.NoError(t, json.Unmarshal(data, &names))
		require.Contains(t, names, "sql_conn_count")
	})

	t.Run("node_id", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/label/node_id/values", nil)
		api.Handler().ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		resp := decodeResponse(t, rec)
		data, _ := json.Marshal(resp.Data)
		var ids []string
		require.NoError(t, json.Unmarshal(data, &ids))
		require.Equal(t, []string{"1", "2"}, ids)
	})
}

func TestMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	api := newTestAPI(mock)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/metadata", nil)
	api.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	resp := decodeResponse(t, rec)
	require.Equal(t, "success", resp.Status)

	// Data should be map[string][]MetricInfo.
	data, _ := json.Marshal(resp.Data)
	var md map[string][]MetricInfo
	require.NoError(t, json.Unmarshal(data, &md))
	require.NotEmpty(t, md)

	// Each entry should have at least one MetricInfo.
	for name, infos := range md {
		require.NotEmpty(t, infos, "metric %q has no info", name)
		require.NotEmpty(t, infos[0].Type)
	}
}

func TestSeries_MissingMatchParam(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	api := newTestAPI(mock)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/series", nil)
	api.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := decodeResponse(t, rec)
	require.Equal(t, "bad_data", resp.ErrorType)
}

// TestSeries_IgnoresMatchers documents that the /series endpoint ignores the
// match[] parameter and returns all metric names. When fixed, this test should
// verify only matched metrics are returned.
func TestSeries_IgnoresMatchers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	api := newTestAPI(mock)

	rec := httptest.NewRecorder()
	// POST with match[] for a specific metric.
	form := url.Values{}
	form.Add("match[]", "sql_conn_count")
	req := httptest.NewRequest("POST", "/api/v1/series",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	api.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	resp := decodeResponse(t, rec)

	data, _ := json.Marshal(resp.Data)
	var result []map[string]string
	require.NoError(t, json.Unmarshal(data, &result))

	// BUG: returns ALL metrics, not just the matched one.
	require.Greater(t, len(result), 1,
		"BUG: /series ignores match[] and returns all metrics")
}

func TestResponseFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 1))}
	api := newTestAPI(mock)

	t.Run("success response", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET",
			"/api/v1/query?query=sql_conn_count&time=100", nil)
		api.Handler().ServeHTTP(rec, req)

		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		resp := decodeResponse(t, rec)
		require.Equal(t, "success", resp.Status)
		require.Empty(t, resp.ErrorType)
		require.Empty(t, resp.Error)
	})

	t.Run("error response", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/query", nil) // missing query
		api.Handler().ServeHTTP(rec, req)

		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		resp := decodeResponse(t, rec)
		require.Equal(t, "error", resp.Status)
		require.NotEmpty(t, resp.ErrorType)
		require.NotEmpty(t, resp.Error)
	})
}

func TestInstantQuery_Timeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a slow mock to verify context timeout works.
	mock := &mockTSDBServer{
		queryFn: func(ctx context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
				return &tspb.TimeSeriesQueryResponse{}, nil
			}
		},
	}
	api := newTestAPI(mock)

	rec := httptest.NewRecorder()
	// Set a very short timeout.
	req := httptest.NewRequest("GET",
		"/api/v1/query?query=sql_conn_count&time=100&timeout=10ms", nil)
	api.Handler().ServeHTTP(rec, req)

	// Should get an execution error due to timeout.
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}
