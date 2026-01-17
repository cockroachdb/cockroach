// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDebugTimeSeriesDumpCmd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	t.Run("debug tsdump --format=openmetrics", func(t *testing.T) {
		out, err := c.RunWithCapture("debug tsdump --format=openmetrics --cluster-name=test-cluster-1 --disable-cluster-name-verification")
		require.NoError(t, err)
		results := strings.Split(out, "\n")[1:] // Drop first item that contains executed command string.
		require.Equal(t, results[len(results)-1], "", "expected last string to be empty (ends with /\n)")
		require.Equal(t, results[len(results)-2], "# EOF")
		require.Greater(t, len(results), 0)
		require.Greater(t, len(results[:len(results)-2]), 0, "expected to have at least one metric")
	})

	t.Run("debug tsdump --format=raw with custom SQL and gRPC ports", func(t *testing.T) {
		//  The `NewCLITest` function we call above to setup the test, already uses custom
		//  ports to avoid conflict between concurrently running tests.

		// Make sure that the yamlFileName is unique for each concurrently running test
		_, port, err := net.SplitHostPort(c.Server.SQLAddr())
		require.NoError(t, err)
		yamlFileName := fmt.Sprintf("/tmp/tsdump_test_%s.yaml", port)

		// The `--host` flag is automatically added by the `RunWithCapture` function based on the assigned port.
		out, err := c.RunWithCapture(
			"debug tsdump --yaml=" + yamlFileName +
				" --format=raw --cluster-name=test-cluster-1 --disable-cluster-name-verification",
		)
		require.NoError(t, err)
		require.NotEmpty(t, out)

		yaml, err := os.Open(yamlFileName)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, yaml.Close())
			require.NoError(t, os.Remove(yamlFileName)) // cleanup
		}()

		yamlContents, err := io.ReadAll(yaml)
		require.NoError(t, err)
		require.NotEmpty(t, yamlContents)
	})

	t.Run("debug tsdump datadog upload with invalid upload workers", func(t *testing.T) {
		// Create a temporary gob file for testing
		tmpFile, err := os.CreateTemp("", "test_tsdump_*.gob")
		require.NoError(t, err)
		defer func(name string) {
			err := os.Remove(name)
			if err != nil {
				t.Fatalf("failed to remove temporary file %s: %v", name, err)
			}
		}(tmpFile.Name())

		// Test with upload workers > 100
		out, _ := c.RunWithCapture(fmt.Sprintf(
			"debug tsdump --format=datadog --dd-api-key=test-key --cluster-label=test --upload-workers=101 %s",
			tmpFile.Name(),
		))
		require.Contains(t, out, "--upload-workers is set to an invalid value. please select a value which between 1 and 100.")

		// Test with upload workers = 0
		out, _ = c.RunWithCapture(fmt.Sprintf(
			"debug tsdump --format=datadog --dd-api-key=test-key --cluster-label=test --upload-workers=0 %s",
			tmpFile.Name(),
		))
		require.Contains(t, out, "--upload-workers is set to an invalid value. please select a value which between 1 and 100.")

		// Test with negative upload workers
		out, _ = c.RunWithCapture(fmt.Sprintf(
			"debug tsdump --format=datadog --dd-api-key=test-key --cluster-label=test --upload-workers=-1 %s",
			tmpFile.Name(),
		))
		require.Contains(t, out, "--upload-workers is set to an invalid value. please select a value which between 1 and 100.")
	})

	t.Run("debug tsdump with --metrics-list-file flag", func(t *testing.T) {
		// Create a temporary metrics list file with specific metrics
		metricsListFile, err := os.CreateTemp("", "metrics_list_*.txt")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(metricsListFile.Name()))
		}()

		// Write a metric that should exist in any CockroachDB cluster
		// sql.mem.root.current is a gauge that always exists
		metricsContent := `# Test metrics list file
sql.mem.root.current
`
		_, err = metricsListFile.WriteString(metricsContent)
		require.NoError(t, err)
		require.NoError(t, metricsListFile.Close())

		// Run tsdump with --metrics-list-file flag
		out, err := c.RunWithCapture(fmt.Sprintf(
			"debug tsdump --format=csv --metrics-list-file=%s --cluster-name=test-cluster-1 --disable-cluster-name-verification",
			metricsListFile.Name(),
		))
		require.NoError(t, err)

		// Expected metrics: sql.mem.root.current is a gauge (not histogram),
		// so it gets cr.node. and cr.store. prefixes without histogram suffixes
		expectedMetrics := map[string]struct{}{
			"cr.node.sql.mem.root.current":  {},
			"cr.store.sql.mem.root.current": {},
		}

		// Extract unique metric names from output (CSV format: metric_name,timestamp,source,value)
		// Output contains: command echo line, then CSV data lines starting with cr.node./cr.store.
		actualMetrics := make(map[string]struct{})
		for _, line := range strings.Split(out, "\n") {
			// Skip non-CSV lines (command echo, empty lines)
			if !strings.HasPrefix(line, "cr.node.") && !strings.HasPrefix(line, "cr.store.") {
				continue
			}
			metricName := strings.Split(line, ",")[0]
			actualMetrics[metricName] = struct{}{}
		}

		for metricName := range actualMetrics {
			_, expected := expectedMetrics[metricName]
			require.True(t, expected, "unexpected metric in output: %s", metricName)
		}
	})

	t.Run("debug tsdump with --metrics-list-file flag using regex pattern", func(t *testing.T) {
		// Create a temporary metrics list file with a regex pattern
		metricsListFile, err := os.CreateTemp("", "metrics_list_regex_*.txt")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(metricsListFile.Name()))
		}()

		// Write a regex pattern that should match multiple sql.mem metrics
		// The pattern contains regex metacharacters (\. and .*) so it will be auto-detected as regex
		metricsContent := `# Test regex pattern
sql\.mem\..*
`
		_, err = metricsListFile.WriteString(metricsContent)
		require.NoError(t, err)
		require.NoError(t, metricsListFile.Close())

		// Run tsdump with --metrics-list-file flag
		out, err := c.RunWithCapture(fmt.Sprintf(
			"debug tsdump --format=csv --metrics-list-file=%s --cluster-name=test-cluster-1 --disable-cluster-name-verification",
			metricsListFile.Name(),
		))
		require.NoError(t, err)

		// Extract unique metric names from output
		actualMetrics := make(map[string]struct{})
		for _, line := range strings.Split(out, "\n") {
			// Skip non-CSV lines (command echo, empty lines)
			if !strings.HasPrefix(line, "cr.node.") && !strings.HasPrefix(line, "cr.store.") {
				continue
			}
			metricName := strings.Split(line, ",")[0]
			actualMetrics[metricName] = struct{}{}
		}

		for metricName := range actualMetrics {
			// All metrics should match sql.mem.* pattern (with cr.node. or cr.store. prefix)
			require.True(t,
				strings.HasPrefix(metricName, "cr.node.sql.mem.") || strings.HasPrefix(metricName, "cr.store.sql.mem."),
				"metric %s does not match expected pattern sql.mem.*", metricName)
		}
	})

	t.Run("debug tsdump with --metrics-list-file flag and non-existent file", func(t *testing.T) {
		out, _ := c.RunWithCapture(
			"debug tsdump --format=csv --metrics-list-file=/nonexistent/path/metrics.txt --cluster-name=test-cluster-1 --disable-cluster-name-verification",
		)
		// The command output should contain the error message
		require.Contains(t, out, "failed to open metrics list file")
	})

	t.Run("debug tsdump with --metrics-list-file flag and empty file", func(t *testing.T) {
		// Create an empty metrics list file
		emptyFile, err := os.CreateTemp("", "empty_metrics_*.txt")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(emptyFile.Name()))
		}()
		require.NoError(t, emptyFile.Close())

		out, _ := c.RunWithCapture(fmt.Sprintf(
			"debug tsdump --format=csv --metrics-list-file=%s --cluster-name=test-cluster-1 --disable-cluster-name-verification",
			emptyFile.Name(),
		))
		// The command output should contain the error message
		require.Contains(t, out, "no valid metric names or patterns")
	})

	t.Run("debug tsdump with --metrics-list-file flag and invalid regex", func(t *testing.T) {
		// Create a metrics list file with valid metrics and invalid regex patterns
		invalidRegexFile, err := os.CreateTemp("", "invalid_regex_*.txt")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(invalidRegexFile.Name()))
		}()

		// Write a mix of valid metrics and invalid regex patterns
		// Invalid regex patterns should be skipped with warning, valid metrics should still work
		metricsContent := `[invalid
sql.query.count
(unclosed_paren`
		_, err = invalidRegexFile.WriteString(metricsContent)
		require.NoError(t, err)
		require.NoError(t, invalidRegexFile.Close())

		out, _ := c.RunWithCapture(fmt.Sprintf(
			"debug tsdump --format=csv --metrics-list-file=%s --cluster-name=test-cluster-1 --disable-cluster-name-verification",
			invalidRegexFile.Name(),
		))

		// Invalid regex patterns are skipped.
		// Extract unique metric names from output
		actualMetrics := make(map[string]struct{})
		for _, line := range strings.Split(out, "\n") {
			// Skip non-CSV lines (command echo, empty lines, warnings)
			if !strings.HasPrefix(line, "cr.node.") && !strings.HasPrefix(line, "cr.store.") {
				continue
			}
			metricName := strings.Split(line, ",")[0]
			actualMetrics[metricName] = struct{}{}
		}

		// Verify valid metrics are present (with cr.node. prefix)
		expectedMetrics := map[string]struct{}{
			"cr.store.sql.query.count": {},
			"cr.node.sql.query.count":  {},
		}
		for metric := range actualMetrics {
			_, expected := expectedMetrics[metric]
			require.True(t, expected, "unexpected metric in output: %s", metric)
		}
	})
}

func TestMakeOpenMetricsWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var out bytes.Buffer
	dataPointsNum := 100
	data := makeTS("cr.test.metric", "source", dataPointsNum)
	w := makeOpenMetricsWriter(&out)
	err := w.Emit(data)
	require.NoError(t, err)
	err = w.Flush()
	require.NoError(t, err)

	var res []string
	for {
		s, err := out.ReadString('\n')
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		res = append(res, s)
	}
	require.Equal(t, dataPointsNum+1 /* datapoints + EOF final line */, len(res))
}

// TestTSDumpConversionWithEmbeddedMetadata tests the conversion of tsdump files
// containing embedded metadata to CSV format, verifying metadata extraction.
func TestTSDumpConversionWithEmbeddedMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tmpFile, err := os.CreateTemp("", "tsdump_with_metadata_*.gob")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			t.Fatalf("failed to remove temporary file %s: %v", name, err)
		}
	}(tmpFile.Name())

	metadata := tsdumpmeta.Metadata{
		Version: "v23.1.0",
		StoreToNodeMap: map[string]string{
			"1": "1",
			"2": "2",
		},
		CreatedAt: timeutil.Unix(1609459200, 0),
	}
	err = tsdumpmeta.Write(tmpFile, metadata)
	require.NoError(t, err)

	enc := gob.NewEncoder(tmpFile)
	kv, err := createMockTimeSeriesKV("cr.node.sql.query.count", "1", 1609459200000000000, 100.5)
	require.NoError(t, err)
	err = enc.Encode(kv)
	require.NoError(t, err)

	tmpFile.Close()

	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	// Convert to CSV format
	out, err := c.RunWithCapture(fmt.Sprintf(
		"debug tsdump --format=csv %s",
		tmpFile.Name(),
	))
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(out), "\n")

	require.Contains(t, out, "Found embedded store-to-node mapping with 2 entries")

	csvFound := false
	for _, line := range lines {
		if strings.Contains(line, "cr.node.sql.query.count") &&
			strings.Contains(line, "2021-01-01T00:00:00Z") {
			csvFound = true
			break
		}
	}
	require.True(t, csvFound, "should contain converted CSV data")
}

// TestTSDumpRawGenerationWithEmbeddedMetadata tests that raw format tsdump generation
// automatically includes embedded store-to-node mapping metadata in the output.
func TestTSDumpRawGenerationWithEmbeddedMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	out, err := c.RunWithCapture("debug tsdump --format=raw --cluster-name=test-cluster-1 --disable-cluster-name-verification")
	require.NoError(t, err)
	require.NotEmpty(t, out)

	// Remove the command prefix from the output to get the actual binary data
	// The output starts with something like "debug tsdump --format=raw..."
	// Find the first occurrence of gob magic bytes or skip the command line
	actualData := out
	if idx := strings.Index(out, "\n"); idx != -1 {
		actualData = out[idx+1:] // Skip the command line
	}

	// Write the cleaned binary data to file
	tmpFile, err := os.CreateTemp("", "tsdump_*.gob")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			t.Fatalf("failed to remove temporary file %s: %v", name, err)
		}
	}(tmpFile.Name())

	_, err = tmpFile.Write([]byte(actualData))
	require.NoError(t, err)
	tmpFile.Close()

	file, err := os.Open(tmpFile.Name())
	require.NoError(t, err)
	defer file.Close()

	dec := gob.NewDecoder(file)
	readMetadata, err := tsdumpmeta.Read(dec)
	require.NoError(t, err)

	// Verify store-to-node mapping is embedded
	require.NotNil(t, readMetadata.StoreToNodeMap)
	require.Equal(t, "1", readMetadata.StoreToNodeMap["1"])
}

func makeTS(name, source string, dataPointsNum int) *tspb.TimeSeriesData {
	dps := make([]tspb.TimeSeriesDatapoint, dataPointsNum)
	for i := range dps {
		dps[i] = tspb.TimeSeriesDatapoint{
			TimestampNanos: timeutil.Now().UnixNano(),
			Value:          rand.Float64(),
		}
	}
	return &tspb.TimeSeriesData{
		Name:       name,
		Source:     source,
		Datapoints: dps,
	}
}

func parseTSInput(t *testing.T, input string, w tsWriter) {
	var data *tspb.TimeSeriesData
	for _, s := range strings.Split(input, "\n") {
		nameValueTimestamp := strings.Split(s, " ")
		// Advance to a new struct anytime name or source changes
		if data == nil ||
			(data != nil && data.Name != nameValueTimestamp[0]) ||
			(data != nil && data.Source != nameValueTimestamp[1]) {
			if data != nil {
				err := w.Emit(data)
				require.NoError(t, err)
			}
			data = &tspb.TimeSeriesData{
				Name:   nameValueTimestamp[0],
				Source: nameValueTimestamp[1],
			}
		}
		value, err := strconv.ParseFloat(nameValueTimestamp[2], 64)
		require.NoError(t, err)
		ts, err := strconv.ParseInt(nameValueTimestamp[3], 10, 64)
		require.NoError(t, err)
		data.Datapoints = append(data.Datapoints, tspb.TimeSeriesDatapoint{
			Value:          value,
			TimestampNanos: ts * 10_000_000,
		})
	}
	err := w.Emit(data)
	require.NoError(t, err)
}

func parseDDInput(t *testing.T, input string, w *datadogWriter) {
	var data *datadogV2.MetricSeries
	var source, storeNodeKey string

	for _, s := range strings.Split(input, "\n") {
		nameValueTimestamp := strings.Split(s, " ")
		sl := reCrStoreNode.FindStringSubmatch(nameValueTimestamp[0])
		if len(sl) != 0 {
			storeNodeKey = sl[1]
			if storeNodeKey == "node" {
				storeNodeKey += "_id"
			}
		}
		metricName := sl[2]

		// Advance to a new struct anytime name or source changes
		if data == nil ||
			(data != nil && data.Metric != metricName ||
				(data != nil && source != nameValueTimestamp[1])) {
			if data != nil {
				_, err := w.emitDataDogMetrics([]datadogV2.MetricSeries{*data})
				require.NoError(t, err)
			}
			data = &datadogV2.MetricSeries{
				Metric: metricName,
				Type:   w.resolveMetricType(metricName),
			}
			source = nameValueTimestamp[1]
			data.Tags = append(data.Tags, fmt.Sprintf("%s:%s", storeNodeKey, nameValueTimestamp[1]))
		}
		value, err := strconv.ParseFloat(nameValueTimestamp[2], 64)
		require.NoError(t, err)
		ts, err := strconv.ParseInt(nameValueTimestamp[3], 10, 64)
		require.NoError(t, err)
		data.Points = append(data.Points, datadogV2.MetricPoint{
			Value:     &value,
			Timestamp: &ts,
		})
	}
	_, err := w.emitDataDogMetrics([]datadogV2.MetricSeries{*data})
	require.NoError(t, err)
}

func TestTsDumpFormatsDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer testutils.TestingHook(&getCurrentTime, func() time.Time {
		return time.Date(2024, 11, 14, 0, 0, 0, 0, time.UTC)
	})()
	var testReqs []*http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r2 := r.Clone(r.Context())
		// Clone the body so that it can be read again
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		r2.Body = io.NopCloser(bytes.NewReader(body))
		testReqs = append(testReqs, r2)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	datadriven.Walk(t, "testdata/tsdump", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var w tsWriter
			switch d.Cmd {
			case "format-datadog", "format-datadog-init":
				debugTimeSeriesDumpOpts.clusterLabel = "test-cluster"
				debugTimeSeriesDumpOpts.clusterID = "test-cluster-id"
				debugTimeSeriesDumpOpts.zendeskTicket = "zd-test"
				debugTimeSeriesDumpOpts.organizationName = "test-org"
				debugTimeSeriesDumpOpts.userName = "test-user"
				debugTimeSeriesDumpOpts.noOfUploadWorkers = 50
				debugTimeSeriesDumpOpts.retryFailedRequests = false
				var series int
				d.ScanArgs(t, "series-threshold", &series)
				var ddwriter, err = makeDatadogWriter(
					defaultDDSite, d.Cmd == "format-datadog-init", "api-key", series,
					server.Listener.Addr().String(), debugTimeSeriesDumpOpts.noOfUploadWorkers,
					debugTimeSeriesDumpOpts.retryFailedRequests)
				require.NoError(t, err)

				parseDDInput(t, d.Input, ddwriter)

				out := strings.Builder{}
				for _, tr := range testReqs {
					reader, err := gzip.NewReader(tr.Body)
					require.NoError(t, err)
					body, err := io.ReadAll(reader)
					require.NoError(t, err)
					out.WriteString(fmt.Sprintf("%s: %s\nDD-API-KEY: %s\nBody: %s", tr.Method, tr.URL, tr.Header.Get("DD-API-KEY"), body))
				}
				testReqs = testReqs[:0] // reset the slice
				return out.String()
			case "format-json":
				debugTimeSeriesDumpOpts.clusterLabel = "test-cluster"
				var testReqs []*http.Request
				var bytes int
				d.ScanArgs(t, "bytes-threshold", &bytes)
				w = makeJSONWriter("https://example.com/data", "test-token", bytes, func(req *http.Request) error {
					testReqs = append(testReqs, req)
					return nil
				})

				parseTSInput(t, d.Input, w)

				out := strings.Builder{}
				for _, tr := range testReqs {
					rc, err := tr.GetBody()
					require.NoError(t, err)
					body, err := io.ReadAll(rc)
					require.NoError(t, err)
					out.WriteString(fmt.Sprintf("%s: %s\nX-Crl-Token: %s\nBody: %v", tr.Method, tr.URL, tr.Header.Get("X-CRL-TOKEN"), string(body)))
				}
				return out.String()
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
				return ""
			}
		})
	})
}

// TestIsRegexPattern tests the automatic regex pattern detection.
func TestIsRegexPattern(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		input    string
		expected bool
	}{
		// Literal metric names (no regex metacharacters)
		{"sql.query.count", false},
		{"changefeed.commit_latency", false},
		{"jobs.changefeed.currently_paused", false},
		{"sql.mem.", false},

		// Regex patterns (contain metacharacters)
		{"sql\\..*", true},                   // contains \. and .*
		{".*latency.*", true},                // contains .*
		{"sql\\.query\\.(count|rate)", true}, // contains \. | ( )
		{"^sql\\..*", true},                  // contains ^ \. .*
		{"sql\\.query\\.count$", true},       // contains \. $
		{"sql\\.(query|exec)\\..*", true},    // contains \. | ( ) .*
		{"[a-z]+\\.count", true},             // contains [ ] + \.
		{"sql\\.query\\.count{1,3}", true},   // contains \. { }
		{"sql\\.query\\.count?", true},       // contains \. ?
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := isRegexPattern(tc.input)
			require.Equal(t, tc.expected, result, "isRegexPattern(%q) = %v, want %v", tc.input, result, tc.expected)
		})
	}
}

// TestReadMetricsListFile tests the metrics list file parsing functionality.
func TestReadMetricsListFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name           string
		fileContent    string
		expectedNames  []string
		expectedRegex  []bool
		expectedErrMsg string
	}{
		{
			name: "basic literal metrics",
			fileContent: `sql.query.count
changefeed.emitted_bytes
kv.dist_sender.rpc.sent`,
			expectedNames: []string{"sql.query.count", "changefeed.emitted_bytes", "kv.dist_sender.rpc.sent"},
			expectedRegex: []bool{false, false, false},
		},
		{
			name: "comments and empty lines",
			fileContent: `# This is a comment
sql.query.count

# Another comment
changefeed.emitted_bytes

`,
			expectedNames: []string{"sql.query.count", "changefeed.emitted_bytes"},
			expectedRegex: []bool{false, false},
		},
		{
			name: "inline comments",
			fileContent: `sql.query.count # this is the query counter
changefeed.emitted_bytes  # bytes emitted`,
			expectedNames: []string{"sql.query.count", "changefeed.emitted_bytes"},
			expectedRegex: []bool{false, false},
		},
		{
			name: "strip cr.node prefix",
			fileContent: `cr.node.sql.query.count
cr.node.changefeed.emitted_bytes`,
			expectedNames: []string{"sql.query.count", "changefeed.emitted_bytes"},
			expectedRegex: []bool{false, false},
		},
		{
			name: "strip cr.store prefix",
			fileContent: `cr.store.capacity
cr.store.capacity.available`,
			expectedNames: []string{"capacity", "capacity.available"},
			expectedRegex: []bool{false, false},
		},
		{
			name: "strip cockroachdb prefix",
			fileContent: `cockroachdb.sql.query.count
cockroachdb.changefeed.emitted_bytes`,
			expectedNames: []string{"sql.query.count", "changefeed.emitted_bytes"},
			expectedRegex: []bool{false, false},
		},
		{
			name: "deduplicate entries",
			fileContent: `sql.query.count
changefeed.emitted_bytes
sql.query.count`,
			expectedNames: []string{"sql.query.count", "changefeed.emitted_bytes"},
			expectedRegex: []bool{false, false},
		},
		{
			name: "whitespace trimming",
			fileContent: `   sql.query.count   
	changefeed.emitted_bytes	`,
			expectedNames: []string{"sql.query.count", "changefeed.emitted_bytes"},
			expectedRegex: []bool{false, false},
		},
		{
			name: "regex patterns auto-detected",
			fileContent: `sql.query.count
sql\..*
.*latency.*`,
			expectedNames: []string{"sql.query.count", "sql\\..*", ".*latency.*"},
			expectedRegex: []bool{false, true, true},
		},
		{
			name: "mixed literals and regex",
			fileContent: `# Literal metrics
changefeed.emitted_bytes
jobs.changefeed.currently_paused

# Regex patterns
distsender\.errors\..*
.*capacity.*`,
			expectedNames: []string{"changefeed.emitted_bytes", "jobs.changefeed.currently_paused", "distsender\\.errors\\..*", ".*capacity.*"},
			expectedRegex: []bool{false, false, true, true},
		},
		{
			name:           "empty file",
			fileContent:    "",
			expectedErrMsg: "no valid metric names or patterns",
		},
		{
			name: "only comments",
			fileContent: `# comment 1
# comment 2`,
			expectedErrMsg: "no valid metric names or patterns",
		},
		{
			name: "invalid regex pattern is skipped with warning",
			fileContent: `sql.query.count
[invalid`,
			// Invalid regex is skipped with warning, valid metric is still parsed
			expectedNames: []string{"sql.query.count"},
			expectedRegex: []bool{false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary file with test content
			tmpFile, err := os.CreateTemp("", "metrics_list_*.txt")
			require.NoError(t, err)
			defer func() {
				require.NoError(t, os.Remove(tmpFile.Name()))
			}()

			_, err = tmpFile.WriteString(tc.fileContent)
			require.NoError(t, err)
			require.NoError(t, tmpFile.Close())

			entries, err := readMetricsListFile(tmpFile.Name())

			if tc.expectedErrMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrMsg)
				return
			}

			require.NoError(t, err)
			require.Len(t, entries, len(tc.expectedNames))

			for i, entry := range entries {
				require.Equal(t, tc.expectedNames[i], entry.Value, "entry[%d].Value", i)
				require.Equal(t, tc.expectedRegex[i], entry.IsRegex, "entry[%d].IsRegex", i)
			}
		})
	}
}
