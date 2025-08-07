// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestTSDumpUploadE2E tests the end-to-end functionality of uploading a time
// series dump to Datadog from a user perspective. This runs the tsdump command
// externally. The datadog API is mocked to capture the request and verify the
// uploaded data.
func TestTSDumpUploadE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer testutils.TestingHook(&getCurrentTime, func() time.Time {
		return time.Date(2024, 11, 14, 0, 0, 0, 0, time.UTC)
	})()
	defer testutils.TestingHook(&getHostname, func() string {
		return "hostname"
	})()

	datadriven.RunTest(t, "testdata/tsdump_upload_e2e", func(t *testing.T, d *datadriven.TestData) string {
		var buf strings.Builder
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reader, err := gzip.NewReader(r.Body)
			require.NoError(t, err)
			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			fmt.Fprintln(&buf, string(body))
			w.WriteHeader(http.StatusOK)
		}))
		defer testutils.TestingHook(&hostNameOverride, server.Listener.Addr().String())()
		defer server.Close()

		c := NewCLITest(TestCLIParams{})
		defer c.Cleanup()

		switch d.Cmd {
		case "upload-datadog":
			debugTimeSeriesDumpOpts.clusterLabel = "test-cluster"
			debugTimeSeriesDumpOpts.clusterID = "test-cluster-id"
			debugTimeSeriesDumpOpts.zendeskTicket = "zd-test"
			debugTimeSeriesDumpOpts.organizationName = "test-org"
			debugTimeSeriesDumpOpts.userName = "test-user"
			dumpFilePath := generateMockTSDumpFromCSV(t, d.Input)

			var clusterLabel, apiKey string
			if d.HasArg("cluster-label") {
				d.ScanArgs(t, "cluster-label", &clusterLabel)
			} else {
				clusterLabel = "test-cluster"
			}
			if d.HasArg("api-key") {
				d.ScanArgs(t, "api-key", &apiKey)
			} else {
				apiKey = "dd-api-key"
			}

			// Run the command
			_, err := c.RunWithCapture(fmt.Sprintf(
				`debug tsdump --format=datadog --dd-api-key="%s" --cluster-label="%s" %s`,
				apiKey, clusterLabel, dumpFilePath,
			))
			require.NoError(t, err)
			return strings.TrimSpace(buf.String())

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}

// TestTSDumpPartialUploadE2E tests the end-to-end functionality of generating failed uploads
// in the file and re-upload the failed requests to Datadog.
func TestTSDumpPartialUploadE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	uploadTime := randomTimestamp()

	defer testutils.TestingHook(&getCurrentTime, func() time.Time {
		return time.Date(2024, 11, 14, 0, 0, 0, 0, time.UTC)
	})()
	defer testutils.TestingHook(&getHostname, func() string {
		return "hostname"
	})()

	datadriven.RunTest(t, "testdata/tsdump_partial_upload_e2e", func(t *testing.T, d *datadriven.TestData) string {
		var buf strings.Builder

		debugTimeSeriesDumpOpts.clusterLabel = "test-cluster"
		debugTimeSeriesDumpOpts.clusterID = "test-cluster-id"
		debugTimeSeriesDumpOpts.zendeskTicket = "zd-test"
		debugTimeSeriesDumpOpts.organizationName = "test-org"
		debugTimeSeriesDumpOpts.userName = "test-user"
		debugTimeSeriesDumpOpts.ddApiKey = "dd-api-key"
		uploadID := newTsdumpUploadID(uploadTime)
		defer testutils.TestingHook(&datadogSeriesThreshold, 1)()
		defer testutils.TestingHook(&newUploadID, func(cluster string, time time.Time) string {
			formattedTime := uploadTime.Format("20060102150405")
			return strings.ToLower(
				strings.ReplaceAll(
					fmt.Sprintf("%s-%s", debugTimeSeriesDumpOpts.clusterLabel, formattedTime), " ", "-",
				),
			)
		})()

		c := NewCLITest(TestCLIParams{})
		defer c.Cleanup()

		switch d.Cmd {
		case "upload-datadog":
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reader, err := gzip.NewReader(r.Body)
				require.NoError(t, err)
				body, err := io.ReadAll(reader)
				require.NoError(t, err)
				if strings.Contains(string(body), "cockroachdb.sql.query.count") {
					w.WriteHeader(http.StatusConflict)
					return
				}
				fmt.Fprintln(&buf, trimNonDeterministicOutput(string(body), uploadID))
				w.WriteHeader(http.StatusOK)
			}))
			defer testutils.TestingHook(&hostNameOverride, server.Listener.Addr().String())()
			defer server.Close()
			dumpFilePath := generateMockTSDumpFromCSV(t, d.Input)

			// Run the command
			_, err := c.RunWithCapture(fmt.Sprintf(
				`debug tsdump --format=datadog --dd-api-key="%s" --cluster-label=%s --upload-workers=1 %s`,
				debugTimeSeriesDumpOpts.ddApiKey, debugTimeSeriesDumpOpts.clusterLabel, dumpFilePath,
			))
			require.NoError(t, err)

			inputDir := filepath.Dir(dumpFilePath)
			failedRequestsBaseName := fmt.Sprintf(failedRequestsFileNameFormat, uploadID)
			fileContent := readFileContent(t, filepath.Join(inputDir, failedRequestsBaseName), uploadID)
			fmt.Fprintln(&buf, fileContent)

			return strings.TrimSpace(buf.String())

		case "partial-upload":
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reader, err := gzip.NewReader(r.Body)
				require.NoError(t, err)
				body, err := io.ReadAll(reader)
				require.NoError(t, err)
				fmt.Fprintln(&buf, string(body))
				w.WriteHeader(http.StatusOK)
			}))
			defer testutils.TestingHook(&hostNameOverride, server.Listener.Addr().String())()
			defer server.Close()

			filePath := generateMockJSONFromInput(t, d.Input)
			_, err := c.RunWithCapture(fmt.Sprintf(
				`debug tsdump --format=datadog --dd-api-key="%s" --cluster-label=%s --upload-workers=1 --retry-failed-requests %s`,
				debugTimeSeriesDumpOpts.ddApiKey, debugTimeSeriesDumpOpts.clusterLabel, filePath,
			))
			require.NoError(t, err)

			return strings.TrimSpace(buf.String())

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}

func randomTimestamp() time.Time {
	randomTime := rand.Int63n(time.Now().Unix())
	randomNow := time.Unix(randomTime, 0)
	return randomNow
}

// trimNonDeterministicOutput removes the upload ID from the output string.
func trimNonDeterministicOutput(out, uploadID string) string {
	re := regexp.MustCompile(uploadID)
	out = re.ReplaceAllString(out, ``)
	return out
}

func readFileContent(t *testing.T, fileName, uploadID string) string {
	// Read the content of the file
	content, err := os.ReadFile(fileName)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.Remove(fileName), "failed to remove temporary file")
	})
	return trimNonDeterministicOutput(string(content), uploadID)
}

// TestTSDumpUploadWithEmbeddedMetadataDataDriven tests the store-to-node mapping functionality
// for time series uploads. It verifies that embedded metadata takes precedence over external
// YAML files and that node_id tags are correctly applied to store metrics.
func TestTSDumpUploadWithEmbeddedMetadataDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer testutils.TestingHook(&getCurrentTime, func() time.Time {
		return time.Date(2024, 11, 14, 0, 0, 0, 0, time.UTC)
	})()
	defer testutils.TestingHook(&getHostname, func() string {
		return "hostname"
	})()

	datadriven.RunTest(t, "testdata/tsdump_upload_embedded_metadata", func(t *testing.T, d *datadriven.TestData) string {
		var buf strings.Builder
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reader, err := gzip.NewReader(r.Body)
			require.NoError(t, err)
			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			fmt.Fprintln(&buf, string(body))
			w.WriteHeader(http.StatusOK)
		}))
		defer testutils.TestingHook(&hostNameOverride, server.Listener.Addr().String())()
		defer server.Close()

		c := NewCLITest(TestCLIParams{})
		defer c.Cleanup()

		// Reset and set common debug options to ensure clean state between test cases
		debugTimeSeriesDumpOpts.storeToNodeMapYAMLFile = ""
		debugTimeSeriesDumpOpts.clusterLabel = "test-cluster"
		debugTimeSeriesDumpOpts.clusterID = "test-cluster-id"
		debugTimeSeriesDumpOpts.zendeskTicket = "zd-test"
		debugTimeSeriesDumpOpts.organizationName = "test-org"
		debugTimeSeriesDumpOpts.userName = "test-user"

		clusterLabel := "test-cluster"
		apiKey := "dd-api-key"
		if d.HasArg("cluster-label") {
			d.ScanArgs(t, "cluster-label", &clusterLabel)
		}
		if d.HasArg("api-key") {
			d.ScanArgs(t, "api-key", &apiKey)
		}

		var dumpFilePath string
		var extraFlag string

		switch d.Cmd {
		case "upload-datadog-embedded-only":
			// Embedded metadata only (no YAML file)
			// This tests that store metrics get proper node_id tags based on embedded store-to-node mapping
			dumpFilePath = generateMockTSDumpWithEmbeddedMetadata(t, d.Input)

		case "upload-datadog-yaml-only":
			// YAML file only (no embedded metadata)
			// This tests that store metrics get proper node_id tags from external YAML when no embedded metadata is present
			dumpFilePath = generateMockTSDumpFromCSV(t, d.Input)

			yamlContent := `1: "1"
2: "1" 
3: "2"`
			yamlFilePath := createTempYAMLFile(t, yamlContent)
			extraFlag = fmt.Sprintf("--store-to-node-map-file=%s", yamlFilePath)

		case "upload-datadog-embedded-priority":
			// Both embedded metadata and YAML file (embedded takes priority)
			// This tests that embedded metadata is prioritized over external YAML when both are present, proving precedence
			dumpFilePath = generateMockTSDumpWithEmbeddedMetadata(t, d.Input)

			yamlContent := `1: "99"
2: "99" 
3: "99"`
			yamlFilePath := createTempYAMLFile(t, yamlContent)
			extraFlag = fmt.Sprintf("--store-to-node-map-file=%s", yamlFilePath)

		case "upload-datadog-no-mapping":
			// No metadata and no YAML file (normal upload)
			// This tests normal upload behavior when no store-to-node mapping is available, store metrics get only store tags
			dumpFilePath = generateMockTSDumpFromCSV(t, d.Input)
			// No extraFlag needed - this tests normal upload without any mapping

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}

		cmdArgs := fmt.Sprintf(
			`debug tsdump --format=datadog --dd-api-key="%s" --cluster-label="%s"`,
			apiKey, clusterLabel,
		)
		if extraFlag != "" {
			cmdArgs += " " + extraFlag
		}
		cmdArgs += " " + dumpFilePath

		_, err := c.RunWithCapture(cmdArgs)
		require.NoError(t, err)
		return strings.TrimSpace(buf.String())
	})
}

// generateMockTSDumpWithEmbeddedMetadata creates a mock tsdump GOB file that includes
// embedded store-to-node mapping metadata along with time series data from CSV input.
func generateMockTSDumpWithEmbeddedMetadata(t *testing.T, csvInput string) string {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "mock_tsdump_with_metadata_*.gob")
	require.NoError(t, err)
	defer tmpFile.Close()

	metadata := tsdumpmeta.Metadata{
		Version: "v23.1.0",
		StoreToNodeMap: map[string]string{
			"1": "1",
			"2": "1",
			"3": "2",
		},
		CreatedAt: timeutil.Unix(1609459200, 0),
	}
	err = tsdumpmeta.Write(tmpFile, metadata)
	require.NoError(t, err)

	encoder := gob.NewEncoder(tmpFile)
	writeTimeSeriesDataFromCSV(t, csvInput, encoder)

	t.Cleanup(func() {
		require.NoError(t, os.Remove(tmpFile.Name()), "failed to remove temporary file")
	})
	return tmpFile.Name()
}

// generateMockTSDumpFromCSV creates a mock tsdump file from CSV input string.
// CSV format: metric_name,timestamp,source,value
// Example: cr.node.admission.admitted.elastic-cpu,2025-05-26T08:32:00Z,1,1
// NOTE: this is the same format generated by the `cockroach tsdump` command
// when --format=csv is used.
func generateMockTSDumpFromCSV(t *testing.T, csvInput string) string {
	t.Helper()

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "mock_tsdump_*.gob")
	require.NoError(t, err)
	defer tmpFile.Close()

	// Create gob encoder
	encoder := gob.NewEncoder(tmpFile)
	writeTimeSeriesDataFromCSV(t, csvInput, encoder)

	t.Cleanup(func() {
		require.NoError(t, os.Remove(tmpFile.Name()), "failed to remove temporary file")
	})
	return tmpFile.Name()
}

// writeTimeSeriesDataFromCSV parses CSV input and encodes time series data into GOB format.
// Each CSV row becomes a roachpb.KeyValue containing time series data.
func writeTimeSeriesDataFromCSV(t *testing.T, csvInput string, encoder *gob.Encoder) {
	t.Helper()

	// Parse CSV data from input string
	reader := csv.NewReader(strings.NewReader(csvInput))
	csvData, err := reader.ReadAll()
	require.NoError(t, err)
	require.Greater(t, len(csvData), 0, "CSV input must have at least one data row")

	// Process each row (no header expected)
	for i, row := range csvData {
		require.Len(t, row, 4, "CSV row %d must have 4 columns: metric_name,timestamp,source,value", i+1)

		metricName := row[0]
		timestampStr := row[1]
		source := row[2]
		valueStr := row[3]

		// Parse timestamp (RFC3339 format)
		timestamp, err := time.Parse(time.RFC3339, timestampStr)
		require.NoError(t, err, "invalid timestamp format in row %d: %s (expected RFC3339)", i+1, timestampStr)
		timestampNanos := timestamp.UnixNano()

		// Parse value
		value, err := strconv.ParseFloat(valueStr, 64)
		require.NoError(t, err, "invalid value in row %d: %s", i+1, valueStr)

		// Create KeyValue entry for this data point
		kv, err := createMockTimeSeriesKV(metricName, source, timestampNanos, value)
		require.NoError(t, err)

		// Encode to gob format
		err = encoder.Encode(kv)
		require.NoError(t, err)
	}
}

// createTempYAMLFile creates a temporary YAML file containing store-to-node mapping configuration.
// Used to test external mapping file functionality in tsdump uploads.
func createTempYAMLFile(t *testing.T, yamlContent string) string {
	t.Helper()

	yamlFile, err := os.CreateTemp("", "store_to_node_*.yaml")
	require.NoError(t, err)

	_, err = yamlFile.WriteString(yamlContent)
	require.NoError(t, err)
	yamlFile.Close()

	t.Cleanup(func() {
		require.NoError(t, os.Remove(yamlFile.Name()), "failed to remove temporary YAML file")
	})

	return yamlFile.Name()
}

// createMockTimeSeriesKV creates a roachpb.KeyValue entry that mimics the internal
// time series storage format used by CockroachDB's time series system.
func createMockTimeSeriesKV(
	name, source string, timestamp int64, value float64,
) (roachpb.KeyValue, error) {
	// Create TimeSeriesData
	tsData := tspb.TimeSeriesData{
		Name:   name,
		Source: source,
		Datapoints: []tspb.TimeSeriesDatapoint{
			{TimestampNanos: timestamp, Value: value},
		},
	}

	// Convert to internal format using 10s resolution
	resolution := ts.Resolution10s
	idatas, err := tsData.ToInternal(
		resolution.SlabDuration(),   // 1 hour (3600 * 10^9 ns)
		resolution.SampleDuration(), // 10 seconds (10 * 10^9 ns)
		true,                        // columnar format
	)
	if err != nil {
		return roachpb.KeyValue{}, err
	}

	// Should only be one internal data entry for a single datapoint
	if len(idatas) != 1 {
		return roachpb.KeyValue{}, fmt.Errorf("expected 1 internal data entry, got %d", len(idatas))
	}

	idata := idatas[0]

	// Create the key
	key := ts.MakeDataKey(name, source, resolution, idata.StartTimestampNanos)

	// Create the value (protobuf-encoded internal data)
	var roachValue roachpb.Value
	if err := roachValue.SetProto(&idata); err != nil {
		return roachpb.KeyValue{}, err
	}

	return roachpb.KeyValue{Key: key, Value: roachValue}, nil
}

// generateMockJSONFromInput creates a mock JSON file from input data.
// The input data is written directly to the JSON file.
// This is useful for testing JSON file generation functionality.
func generateMockJSONFromInput(t *testing.T, inputData string) string {
	t.Helper()

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "mock_json_*.json")
	require.NoError(t, err)
	defer tmpFile.Close()

	// Write input data directly to the JSON file
	for _, request := range strings.Split(inputData, "\n") {
		_, err = tmpFile.Write([]byte(request + "\n"))
		require.NoError(t, err)
	}

	// All retries are getting succeeded so command itself should remove the file.
	t.Cleanup(func() {
		require.Error(t, os.Remove(tmpFile.Name()), "file should not be removed as it is already removed in command")
	})
	return tmpFile.Name()
}

// TestDeltaCalculationForCounters tests the cumulative to delta conversion functionality
// for counter metrics across multiple dump calls.
func TestDeltaCalculationForCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, processDeltaEnvVar, "1")()

	// Create a datadogWriter with cumulative to delta processor
	writer, err := makeDatadogWriter("us5", false, "test-api-key", 100, "", 1, false)
	require.NoError(t, err)

	testCases := []struct {
		name           string
		metricName     string
		source         string
		values         []float64
		expectedValues []float64
	}{
		{
			name:           "counter with incrementing values",
			metricName:     "cr.node.test-counter-count",
			source:         "1",
			values:         []float64{100, 150, 200},
			expectedValues: []float64{100, 50, 50}, // first value, then deltas
		},
		{
			name:           "counter starting from zero",
			metricName:     "cr.node.zero-start-count",
			source:         "2",
			values:         []float64{0, 25, 75},
			expectedValues: []float64{0, 25, 50},
		},
		{
			name:           "counter with no jumps",
			metricName:     "cr.node.large-jump-count",
			source:         "3",
			values:         []float64{1000, 1000, 1000},
			expectedValues: []float64{1000, 0, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timestamp := time.Date(2024, 11, 14, 0, 0, 0, 0, time.UTC).UnixNano()

			for i, value := range tc.values {
				// Create mock KeyValue for this metric value
				kv, err := createMockTimeSeriesKV(tc.metricName, tc.source, timestamp+int64(i)*1e10, value)
				require.NoError(t, err)

				// Call dump function
				series, err := writer.dump(&kv)
				require.NoError(t, err)

				// Verify the metric type is COUNT for counter metrics
				require.NotNil(t, series.Type)
				require.Equal(t, *series.Type, datadogV2.METRICINTAKETYPE_COUNT)

				// Verify we got the expected value (first value or delta)
				require.Len(t, series.Points, 1)
				actualValue := *series.Points[0].Value
				expectedValue := tc.expectedValues[i]
				require.Equal(t, expectedValue, actualValue,
					"Call %d: expected %f, got %f", i+1, expectedValue, actualValue)
			}
		})
	}
}

// TestDeltaCalculationResetDetection tests that the cumulative to delta processor
// properly handles counter resets (when counter goes backwards due to process restart).
func TestDeltaCalculationResetDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, processDeltaEnvVar, "1")()

	writer, err := makeDatadogWriter("us5", false, "test-api-key", 100, "", 1, false)
	require.NoError(t, err)

	metricName := "cr.node.reset-test-count"
	source := "1"
	timestamp := time.Date(2024, 11, 14, 0, 0, 0, 0, time.UTC).UnixNano()

	// First call with value 100
	kv1, err := createMockTimeSeriesKV(metricName, source, timestamp, 100)
	require.NoError(t, err)
	series1, err := writer.dump(&kv1)
	require.NoError(t, err)
	require.Equal(t, 100.0, *series1.Points[0].Value)

	// Second call with value 150 (normal increment)
	kv2, err := createMockTimeSeriesKV(metricName, source, timestamp+1e10, 150)
	require.NoError(t, err)
	series2, err := writer.dump(&kv2)
	require.NoError(t, err)
	require.Equal(t, 50.0, *series2.Points[0].Value) // delta: 150 - 100

	// Third call with value 50 (reset detected - should handle gracefully)
	kv3, err := createMockTimeSeriesKV(metricName, source, timestamp+2e10, 50)
	require.NoError(t, err)
	series3, err := writer.dump(&kv3)
	require.NoError(t, err)
	require.Equal(t, 50.0, *series3.Points[0].Value) // reset: use current value as delta

	// Fourth call with value 75 (normal increment after reset)
	kv4, err := createMockTimeSeriesKV(metricName, source, timestamp+3e10, 75)
	require.NoError(t, err)
	series4, err := writer.dump(&kv4)
	require.NoError(t, err)
	require.Equal(t, 25.0, *series4.Points[0].Value) // delta: 75 - 50
}

// TestDeltaCalculationCrossBatchPersistence tests that cumulative to delta conversion
// state persists correctly when the same metric appears in different batches.
func TestDeltaCalculationCrossBatchPersistence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, processDeltaEnvVar, "1")()

	writer, err := makeDatadogWriter("us5", false, "test-api-key", 100, "", 1, false)
	require.NoError(t, err)

	metricName := "cr.node.cross-batch-test-count"
	source := "1"

	timestamp := time.Date(2025, 6, 26, 22, 49, 24, 0, time.UTC).UnixNano()

	// Simulate first batch
	kv1, err := createMockTimeSeriesKV(metricName, source, timestamp, 100)
	require.NoError(t, err)
	series1, err := writer.dump(&kv1)
	require.NoError(t, err)
	require.Equal(t, 100.0, *series1.Points[0].Value) // first value

	// Simulate processing other metrics (different batch)
	otherKv, err := createMockTimeSeriesKV("cr.node.other-metric-count", "2", timestamp+5e9, 50)
	require.NoError(t, err)
	_, err = writer.dump(&otherKv)
	require.NoError(t, err)

	// Simulate second batch with same metric - state should persist
	kv2, err := createMockTimeSeriesKV(metricName, source, timestamp+1e10, 180)
	require.NoError(t, err)
	series2, err := writer.dump(&kv2)
	require.NoError(t, err)
	require.Equal(t, 80.0, *series2.Points[0].Value) // delta: 180 - 100

	// Third batch - should still remember previous value
	kv3, err := createMockTimeSeriesKV(metricName, source, timestamp+2e10, 220)
	require.NoError(t, err)
	series3, err := writer.dump(&kv3)
	require.NoError(t, err)
	require.Equal(t, 40.0, *series3.Points[0].Value) // delta: 220 - 180
}

// TestDeltaCalculationWithUnsortedTimestamps tests that delta calculation works
// correctly even when timestamps are not in sorted order.
func TestDeltaCalculationWithUnsortedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer envutil.TestSetEnv(t, processDeltaEnvVar, "1")()

	processor := NewCumulativeToDeltaProcessor()

	// Create a series with timestamps out of order
	series := &datadogV2.MetricSeries{
		Metric: "test.counter",
		Tags:   []string{"node_id:1"},
		Type:   datadogV2.METRICINTAKETYPE_COUNT.Ptr(),
		Points: []datadogV2.MetricPoint{
			{
				Timestamp: datadog.PtrInt64(1000),  // t=1000
				Value:     datadog.PtrFloat64(300), // third point chronologically
			},
			{
				Timestamp: datadog.PtrInt64(500),   // t=500
				Value:     datadog.PtrFloat64(150), // second point chronologically
			},
			{
				Timestamp: datadog.PtrInt64(100),   // t=100
				Value:     datadog.PtrFloat64(100), // first point chronologically
			},
		},
	}

	// Process the series (it's unsorted, so pass false)
	err := processor.processCounterMetric(series, false)
	require.NoError(t, err)

	// After processing, points should be sorted by timestamp and have correct deltas
	require.Len(t, series.Points, 3)

	// Check that points are now sorted by timestamp
	require.Equal(t, int64(100), *series.Points[0].Timestamp)
	require.Equal(t, int64(500), *series.Points[1].Timestamp)
	require.Equal(t, int64(1000), *series.Points[2].Timestamp)

	// Check that delta calculation is correct (based on chronological order)
	require.Equal(t, 100.0, *series.Points[0].Value) // first point: keep original value
	require.Equal(t, 50.0, *series.Points[1].Value)  // delta: 150 - 100
	require.Equal(t, 150.0, *series.Points[2].Value) // delta: 300 - 150
}
