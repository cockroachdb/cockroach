// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// _status/vars outputted lines as of the creation of the TestStatusVarsSizeLimit test.
var sizeLimit = 9650

// TestMetricsMetadata ensures that the server's recorder return metrics and
// that each metric has a Name, Help, Unit, and DisplayUnit defined.
func TestMetricsMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	metricsMetadata, _, _ := s.MetricsRecorder().GetMetricsMetadata(true /* combine */)

	if len(metricsMetadata) < 200 {
		t.Fatal("s.recorder.GetMetricsMetadata() failed sanity check; didn't return enough metrics.")
	}

	for _, v := range metricsMetadata {
		if v.Name == "" {
			t.Fatal("metric missing name.")
		}
		if v.Help == "" {
			t.Fatalf("%s missing Help.", v.Name)
		}
		if v.Measurement == "" {
			t.Fatalf("%s missing Measurement.", v.Name)
		}
		if v.Unit == 0 {
			t.Fatalf("%s missing Unit.", v.Name)
		}
	}
}

func TestGetRecordedMetricNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	metricsMetadata, _, _ := s.MetricsRecorder().GetMetricsMetadata(true /* combine */)
	recordedNames := s.MetricsRecorder().GetRecordedMetricNames(metricsMetadata)

	for _, v := range recordedNames {
		require.True(t, strings.HasPrefix(v, "cr.node") || strings.HasPrefix(v, "cr.store"))
	}
}

func TestGetRecordedMetricNames_histogram(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	metricName := "my.metric"
	metricsMetadata := map[string]metric.Metadata{
		metricName: {
			Name:        metricName,
			Help:        "help text",
			Measurement: "measurement",
			Unit:        metric.Unit_COUNT,
			MetricType:  prometheusgo.MetricType_HISTOGRAM,
		},
	}

	recordedNames := s.MetricsRecorder().GetRecordedMetricNames(metricsMetadata)
	require.Equal(t, len(metric.HistogramMetricComputers), len(recordedNames))
	for _, histogramMetric := range metric.HistogramMetricComputers {
		_, ok := recordedNames[metricName+histogramMetric.Suffix]
		require.True(t, ok)
	}
}

func TestHistogramMetricComputers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	metricName := "my.metric"
	h := metric.NewHistogram(metric.HistogramOptions{
		Metadata: metric.Metadata{Name: metricName},
		Buckets:  []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
		Mode:     metric.HistogramModePrometheus,
	})

	sum := int64(0)
	count := 0

	for i := 1; i <= 10; i++ {
		recordedVal := int64(i) * 10
		sum += recordedVal
		count++
		h.RecordValue(recordedVal)
	}

	avg := float64(sum) / float64(count)
	snapshot := h.WindowedSnapshot()
	results := make(map[string]float64, len(metric.HistogramMetricComputers))
	for _, c := range metric.HistogramMetricComputers {
		results[metricName+c.Suffix] = c.ComputedMetric(snapshot)
	}

	expected := map[string]float64{
		metricName + "-sum":     float64(sum),
		metricName + "-avg":     avg,
		metricName + "-count":   float64(count),
		metricName + "-max":     100,
		metricName + "-p99.999": 100,
		metricName + "-p99.99":  100,
		metricName + "-p99.9":   100,
		metricName + "-p99":     100,
		metricName + "-p90":     90,
		metricName + "-p75":     80,
		metricName + "-p50":     50,
	}
	require.Equal(t, expected, results)
}

// TestStatusVars verifies that prometheus metrics are available via the
// /_status/vars and /_status/load endpoints.
func TestStatusVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	if body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String()); err != nil {
		t.Fatal(err)
	} else {
		if !bytes.Contains(body, []byte("# TYPE sql_bytesout counter\nsql_bytesout")) {
			t.Errorf("expected sql_bytesout, got: %s", body)
		}
		if !bytes.Contains(body, []byte(`# TYPE sql_insert_count counter`)) {
			t.Errorf("expected sql_insert_count, got: %s", body)
		}
	}
	if body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"load").String()); err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("# TYPE sys_cpu_user_ns gauge\nsys_cpu_user_ns")) {
		t.Errorf("expected sys_cpu_user_ns, got: %s", body)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	if body, err := srvtestutils.GetText(s, s.AdminURL().WithPath("/metrics").String()); err != nil {
		t.Fatal(err)
	} else {
		if !bytes.Contains(body, []byte(`# TYPE sql_bytesout counter`)) {
			t.Errorf("expected sql_bytesout, got: %s", body)
		}
		if !bytes.Contains(body, []byte(`# TYPE sql_count counter`)) {
			t.Errorf("expected sql_count, got: %s", body)
		}
	}
}

// TestStatusVarsTxnMetrics verifies that the metrics from the /_status/vars
// endpoint for txns and the special cockroach_restart savepoint are correct.
func TestStatusVarsTxnMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
			base.TestTenantAlwaysEnabled, 112953,
		),
	})
	defer srv.Stopper().Stop(context.Background())

	testFn := func(s serverutils.ApplicationLayerInterface, expectedLabel string) {
		db := s.SQLConn(t)

		if _, err := db.Exec("BEGIN;" +
			"SAVEPOINT cockroach_restart;" +
			"SELECT 1;" +
			"RELEASE SAVEPOINT cockroach_restart;" +
			"ROLLBACK;"); err != nil {
			t.Fatal(err)
		}

		body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
		if err != nil {
			t.Fatal(err)
		}
		if expected := []byte("sql_txn_begin_count{" + expectedLabel + "} 1"); !bytes.Contains(body, expected) {
			t.Errorf("expected %q, got: %s", expected, body)
		}
		if expected := []byte("sql_restart_savepoint_count{" + expectedLabel + "} 1"); !bytes.Contains(body, expected) {
			t.Errorf("expected %q, got: %s", expected, body)
		}
		if expected := []byte("sql_restart_savepoint_release_count{" + expectedLabel + "} 1"); !bytes.Contains(body, expected) {
			t.Errorf("expected %q, got: %s", expected, body)
		}
		if expected := []byte("sql_txn_commit_count{" + expectedLabel + "} 1"); !bytes.Contains(body, expected) {
			t.Errorf("expected %q, got: %s", expected, body)
		}
		if expected := []byte("sql_txn_rollback_count{" + expectedLabel + "} 0"); !bytes.Contains(body, expected) {
			t.Errorf("expected %q, got: %s", expected, body)
		}
	}

	t.Run("system", func(t *testing.T) {
		s := srv.SystemLayer()
		testFn(s, `node_id="1"`)
	})
	t.Run("tenant", func(t *testing.T) {
		s := srv.ApplicationLayer()
		testFn(s, `tenant="test-tenant"`)
	})
}

// TestStatusVarsSizeLimit verifies the output of _status/vars has not increased
// substantially from the time of writing this test.
// Substantial increases to _status/vars have been linked to significantly increased
// memory usage in Prometheus and OOMs so it is best to have this test act as a warning
// signal to prevent this. If the limit has been exceeded and it is not due to a bug
// please consult with the observability infrastructure team to determine a course of
// action to allow the new metrics to be available.
// TODO(santamaura): if more use cases for comparison logic between a development branch
// and master become prevalent then we should replace this with a CI job to check the amount
// of increase to _status/vars and retire this test.
func TestStatusVarsSizeLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "unrelated data race")
	skip.UnderStress(t, "unnecessary to test this scenario")
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(body), "\n")
	require.LessOrEqual(t, len(lines), int(float64(sizeLimit)*1.5))
}

func TestSpanStatsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		// We are looking at the entire keyspace below.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	httpClient, err := ts.GetAdminHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	var response roachpb.SpanStatsResponse
	span := roachpb.Span{
		Key:    roachpb.RKeyMin.AsRawKey(),
		EndKey: roachpb.RKeyMax.AsRawKey(),
	}
	request := roachpb.SpanStatsRequest{
		NodeID: "1",
		Spans:  []roachpb.Span{span},
	}

	url := ts.AdminURL().WithPath(apiconstants.StatusPrefix + "span").String()
	if err := httputil.PostJSON(httpClient, url, &request, &response); err != nil {
		t.Fatal(err)
	}
	initialRanges, err := srv.ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}
	responseSpanStats := response.SpanToStats[span.String()]
	if a, e := int(responseSpanStats.RangeCount), initialRanges; a != e {
		t.Errorf("expected %d ranges, found %d", e, a)
	}
}

// extractLinesContainingSubstring returns all lines from body that
// contain the given key substring.
func extractLinesContainingSubstring(body []byte, key string) []string {
	var result []string

	lines := bytes.Split(body, []byte("\n"))
	for _, line := range lines {
		if bytes.Contains(line, []byte(key)) {
			result = append(result, string(line))
		}
	}
	return result
}

// bodyContainsRegexMatch returns true if any line in the body matches
// the given regex pattern. It is because in local test, the metrics shows
// up with and without `tenant="system"` tag indeterministically.
func bodyContainsRegexMatch(t *testing.T, body []byte, pattern string) bool {
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatal(err)
	}

	lines := bytes.Split(body, []byte("\n"))
	for _, line := range lines {
		if re.Match(line) {
			return true
		}
	}
	return false
}

func TestStoreProcedureCallStatementMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Registry.GetLabels() has unsafe concurrent access to shared label state
	skip.UnderRace(t, "unrelated data race")
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.SystemLayer()
	db := s.SQLConn(t)

	if _, err := db.Exec(`
		CREATE TABLE tbl (id SERIAL PRIMARY KEY, t text UNIQUE);

		INSERT INTO tbl (t) VALUES ('d');

		CREATE OR REPLACE PROCEDURE inserttbl()
		LANGUAGE plpgsql
		AS $$
		BEGIN
				INSERT INTO tbl (t) VALUES ('a');
				COMMIT;
				INSERT INTO tbl (t) VALUES ('b');
				COMMIT;
				INSERT INTO tbl (t) VALUES ('c');
        INSERT INTO tbl (t) VALUES ('d'); -- this will fail due to unique constraint.
        INSERT INTO tbl (t) VALUES ('z');
		END;
		$$;`); err != nil {
		t.Fatal(err)
	}

	body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	if err != nil {
		t.Fatal(err)
	}

	if expected := `sql_udf_insert_started_count_internal{node_id="1"\S*\} 0`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_insert_started_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	// Only after the first call to inserttbl() do we expect to see the metrics.
	if _, err := db.Exec(`CALL inserttbl();`); err == nil {
		t.Fatal("expected error")
	}

	body, err = srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	if err != nil {
		t.Fatal(err)
	}

	// The actual execution will halt at `INSERT INTO tbl (t) VALUES
	// ('d');` due to violation of the unique constraint. So we expect 4
	// started insert statements, but only 3 successful insert statements.
	// The one after inserting 'd' will not be executed.
	if expected := `sql_udf_insert_started_count{node_id="1"\S*\} 4`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_insert_started_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	if expected := `sql_udf_insert_count{node_id="1"\S*\} 3`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_insert_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	// This is to show that the metrics is globally aggregated across all calls of
	// store procedures.
	if _, err := db.Exec(`
		TRUNCATE tbl;
		CALL inserttbl();
`); err != nil {
		t.Fatal(err)
	}

	body, err = srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	if err != nil {
		t.Fatal(err)
	}

	// Now with the table truncated, we expect the whole procedure body to
	// be executed without error. So there should be 5 more insert inc in
	// both started and executed insert statements.
	if expected := `sql_udf_insert_started_count{node_id="1"\S*\} 9`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_insert_started_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	if expected := `sql_udf_insert_count{node_id="1"\S*\} 8`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_insert_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	// Now we test the update, delete and select statements.
	if _, err := db.Exec(`
		CREATE OR REPLACE PROCEDURE updatetbl()
		LANGUAGE plpgsql
		AS $$
		BEGIN
				UPDATE tbl SET t = 'y' WHERE t = 'd';
				DELETE FROM tbl WHERE t = 'b';
			  SELECT t FROM tbl WHERE t > 'a';
		END;
		$$;
		CALL updatetbl();`); err != nil {
		t.Fatal(err)
	}

	body, err = srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	if err != nil {
		t.Fatal(err)
	}

	if expected := `sql_udf_update_started_count{node_id="1"\S*\} 1`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_update_started_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	if expected := `sql_udf_update_count{node_id="1"\S*\} 1`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_update_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	if expected := `sql_udf_delete_started_count{node_id="1"\S*\} 1`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_delete_started_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	if expected := `sql_udf_delete_count{node_id="1"\S*\} 1`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_delete_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	if expected := `sql_udf_select_started_count{node_id="1"\S*\} 1`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_select_started_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	if expected := `sql_udf_select_count{node_id="1"\S*\} 1`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_select_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	// Now we test the nested procedure calls.
	if _, err := db.Exec(`
		CREATE OR REPLACE PROCEDURE insertone()
		LANGUAGE plpgsql
		AS $$
		BEGIN
				INSERT INTO tbl (t) VALUES ('x');
		END;
		$$;
		
		CREATE OR REPLACE PROCEDURE nested_insertone()
		LANGUAGE plpgsql
		AS $$
		BEGIN
				CALL insertone();
		END;
		$$;
		CALL nested_insertone();`); err != nil {
		t.Fatal(err)
	}

	body, err = srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	if err != nil {
		t.Fatal(err)
	}

	if expected := `sql_udf_insert_started_count{node_id="1"\S*\} 10`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_insert_started_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

	if expected := `sql_udf_insert_count{node_id="1"\S*\} 9`; !bodyContainsRegexMatch(t, body, expected) {
		got := extractLinesContainingSubstring(body, "sql_udf_insert_count")
		t.Errorf("expected %q, got: %s", expected, got)
	}

}
