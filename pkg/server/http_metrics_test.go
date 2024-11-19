// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/gorilla/mux"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRegisterMetricsMiddleware(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	t.Run("cluster settings", func(t *testing.T) {
		clusterSettings := cluster.MakeTestingClusterSettings()
		serverMetrics := NewServerHttpMetrics(metric.NewRegistry(), clusterSettings)
		router := mux.NewRouter()
		serverMetrics.registerMetricsMiddleware(router)
		router.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		server := httptest.NewServer(router)
		defer server.Close()
		rr := httptest.NewRecorder()
		req, err := http.NewRequest("GET", server.URL+"/", nil)
		require.NoError(t, err)

		router.ServeHTTP(rr, req)
		metrics := serverMetrics.RequestMetrics.ToPrometheusMetrics()
		require.Len(t, metrics, 0)

		serverHTTPMetricsEnabled.Override(context.Background(), &clusterSettings.SV, true)
		router.ServeHTTP(rr, req)
		metrics = serverMetrics.RequestMetrics.ToPrometheusMetrics()
		require.Len(t, metrics, 1)
		assertPrometheusMetrics(t, metrics, map[string]uint64{
			fmt.Sprintf("%s GET %s", strconv.Itoa(http.StatusOK), "/"): uint64(1),
		})

		serverHTTPMetricsEnabled.Override(context.Background(), &clusterSettings.SV, false)
		router.ServeHTTP(rr, req)
		metrics = serverMetrics.RequestMetrics.ToPrometheusMetrics()
		require.Len(t, metrics, 1)
		assertPrometheusMetrics(t, metrics, map[string]uint64{
			fmt.Sprintf("%s GET %s", strconv.Itoa(http.StatusOK), "/"): uint64(1),
		})

	})
	t.Run("metrics", func(t *testing.T) {
		clusterSettings := cluster.MakeTestingClusterSettings()
		serverHTTPMetricsEnabled.Override(context.Background(), &clusterSettings.SV, true)
		serverMetrics := NewServerHttpMetrics(metric.NewRegistry(), clusterSettings)
		pathUri := "/mypath/{path_var:[0-9]+}/"
		router := mux.NewRouter()
		serverMetrics.registerMetricsMiddleware(router)
		shouldFail := false
		handlerFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(time.Millisecond)
			if shouldFail {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusOK)
			}
		})
		router.Handle(pathUri, handlerFunc).Methods(http.MethodGet, http.MethodPost)
		server := httptest.NewServer(router)
		defer server.Close()
		getReq1, err := http.NewRequest("GET", server.URL+"/mypath/1/", nil)
		require.NoError(t, err)
		getReq2, err := http.NewRequest("GET", server.URL+"/mypath/2/", nil)
		require.NoError(t, err)
		postReq2, err := http.NewRequest("POST", server.URL+"/mypath/2/", nil)
		require.NoError(t, err)
		putReq3, err := http.NewRequest("PUT", server.URL+"/mypath/1/", nil)
		require.NoError(t, err)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, getReq1)
		router.ServeHTTP(rr, getReq1)
		router.ServeHTTP(rr, getReq2)
		router.ServeHTTP(rr, postReq2)
		router.ServeHTTP(rr, putReq3)

		shouldFail = true
		router.ServeHTTP(rr, postReq2)

		metrics := serverMetrics.RequestMetrics.ToPrometheusMetrics()
		// putReq3 won't be recorded because `PUT /mypath/1/` isn't a valid route
		require.Len(t, metrics, 3)
		assertPrometheusMetrics(t, metrics, map[string]uint64{
			fmt.Sprintf("%s GET %s", strconv.Itoa(http.StatusOK), formatPathVars(pathUri)):                   uint64(3),
			fmt.Sprintf("%s POST %s", strconv.Itoa(http.StatusOK), formatPathVars(pathUri)):                  uint64(1),
			fmt.Sprintf("%s POST %s", strconv.Itoa(http.StatusInternalServerError), formatPathVars(pathUri)): uint64(1),
		})
	})
}

func TestFormatPathVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testcase struct {
		name         string
		path         string
		expectedPath string
	}
	testcases := []testcase{
		{name: "no variable", path: "/testpath/", expectedPath: "/testpath/"},
		{name: "variable with regex", path: "/testpath/{param:[0-9]+}/", expectedPath: "/testpath/<param>/"},
		{name: "multiple variables with regex", path: "/testpath/{param:[0-9]+}/{other_param:[\\w]}", expectedPath: "/testpath/<param>/<other_param>"},
		{name: "variable without regex", path: "/testpath/{param}/", expectedPath: "/testpath/<param>/"},
		{name: "multiple variable without regex", path: "/testpath/{param}/{other_Param}/", expectedPath: "/testpath/<param>/<other_Param>/"},
		{name: "mixed variables", path: "/testpath/{param:[\\w]}/{otherParam}", expectedPath: "/testpath/<param>/<otherParam>"},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedPath, formatPathVars(tc.path))
		})
	}
}

func BenchmarkHTTPMetrics(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	b.Run("Metrics enabled", func(b *testing.B) {
		b.StopTimer()
		b.ResetTimer()
		clusterSettings := cluster.MakeTestingClusterSettings()
		serverHTTPMetricsEnabled.Override(context.Background(), &clusterSettings.SV, true)
		serverMetrics := NewServerHttpMetrics(metric.NewRegistry(), clusterSettings)
		server, router := newBenchmarkServer("/{param}/", serverMetrics)
		defer server.Close()
		r1, err := http.NewRequest("GET", server.URL+"/1/", nil)
		require.NoError(b, err)
		r2, err := http.NewRequest("GET", server.URL+"/2/", nil)
		require.NoError(b, err)
		r3, err := http.NewRequest("POST", server.URL+"/2/", nil)
		require.NoError(b, err)
		r4, err := http.NewRequest("PUT", server.URL+"/1/", nil)
		require.NoError(b, err)
		rr := httptest.NewRecorder()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			router.ServeHTTP(rr, r1)
			router.ServeHTTP(rr, r2)
			router.ServeHTTP(rr, r3)
			router.ServeHTTP(rr, r4)
		}
		require.Len(b, serverMetrics.RequestMetrics.ToPrometheusMetrics(), 2)
	})

	b.Run("Metrics disabled", func(b *testing.B) {
		b.StopTimer()
		b.ResetTimer()
		clusterSettings := cluster.MakeTestingClusterSettings()
		serverHTTPMetricsEnabled.Override(context.Background(), &clusterSettings.SV, false)
		serverMetrics := NewServerHttpMetrics(metric.NewRegistry(), clusterSettings)
		server, router := newBenchmarkServer("/{param}/", serverMetrics)
		defer server.Close()
		r1, err := http.NewRequest("GET", server.URL+"/1/", nil)
		require.NoError(b, err)
		r2, err := http.NewRequest("GET", server.URL+"/2/", nil)
		require.NoError(b, err)
		r3, err := http.NewRequest("POST", server.URL+"/2/", nil)
		require.NoError(b, err)
		r4, err := http.NewRequest("PUT", server.URL+"/1/", nil)
		require.NoError(b, err)
		rr := httptest.NewRecorder()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			router.ServeHTTP(rr, r1)
			router.ServeHTTP(rr, r2)
			router.ServeHTTP(rr, r3)
			router.ServeHTTP(rr, r4)
		}
		require.Len(b, serverMetrics.RequestMetrics.ToPrometheusMetrics(), 0)
	})

	b.Run("No Middleware", func(b *testing.B) {
		b.StopTimer()
		b.ResetTimer()
		server, router := newBenchmarkServer("/{param}/", nil)
		defer server.Close()
		r1, err := http.NewRequest("GET", server.URL+"/1/", nil)
		require.NoError(b, err)
		r2, err := http.NewRequest("GET", server.URL+"/2/", nil)
		require.NoError(b, err)
		r3, err := http.NewRequest("POST", server.URL+"/2/", nil)
		require.NoError(b, err)
		r4, err := http.NewRequest("PUT", server.URL+"/1/", nil)
		require.NoError(b, err)
		rr := httptest.NewRecorder()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			router.ServeHTTP(rr, r1)
			router.ServeHTTP(rr, r2)
			router.ServeHTTP(rr, r3)
			router.ServeHTTP(rr, r4)
		}
	})
}

func newBenchmarkServer(
	route string, serverMetrics *HttpServerMetrics,
) (*httptest.Server, *mux.Router) {
	router := mux.NewRouter()
	router.Handle(route, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})).Methods(http.MethodGet, http.MethodPost)
	if serverMetrics != nil {
		serverMetrics.registerMetricsMiddleware(router)
	}
	return httptest.NewServer(router), router
}
func assertPrometheusMetrics(
	t *testing.T, metrics []*prometheusgo.Metric, expected map[string]uint64,
) {
	t.Helper()
	actual := map[string]*prometheusgo.Histogram{}
	for _, m := range metrics {
		var method, path, statusCode string
		for _, l := range m.Label {
			switch *l.Name {
			case MethodLabel:
				method = *l.Value
			case PathLabel:
				path = *l.Value
			case StatusCodeLabel:
				statusCode = *l.Value
			}
		}
		histogram := m.Histogram
		require.NotNil(t, histogram, "expected histogram")
		key := fmt.Sprintf("%s %s %s", statusCode, method, path)
		actual[key] = histogram
	}

	for key, val := range expected {
		histogram, ok := actual[key]
		require.True(t, ok)
		require.Greater(t, *histogram.SampleSum, float64(0), "expected `%s` to have a SampleSum > 0", key)
		require.Equal(t, val, *histogram.SampleCount, "expected `%s` to have SampleCount of %d", key, val)
	}
}
