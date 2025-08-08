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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
				if strings.Contains(string(body), "crdb.tsdump.sql.query.count") {
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

// generateMockTSDumpFromCSV creates a mock tsdump file from CSV input string.
// CSV format: metric_name,timestamp,source,value
// Example: cr.node.admission.admitted.elastic-cpu,2025-05-26T08:32:00Z,1,1
// NOTE: this is the same format generated by the `cockroach tsdump` command
// when --format=csv is used.
func generateMockTSDumpFromCSV(t *testing.T, csvInput string) string {
	t.Helper()

	// Parse CSV data from input string
	reader := csv.NewReader(strings.NewReader(csvInput))
	csvData, err := reader.ReadAll()
	require.NoError(t, err)
	require.Greater(t, len(csvData), 0, "CSV input must have at least one data row")

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "mock_tsdump_*.gob")
	require.NoError(t, err)
	defer tmpFile.Close()

	// Create gob encoder
	encoder := gob.NewEncoder(tmpFile)

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

	t.Cleanup(func() {
		require.NoError(t, os.Remove(tmpFile.Name()), "failed to remove temporary file")
	})
	return tmpFile.Name()
}

// createMockTimeSeriesKV creates a roachpb.KeyValue entry containing time series data
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
