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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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

// TestEmbeddedMetadata tests the core functionality for writing and reading
// embedded metadata in tsdump files, including error handling for invalid data.
func TestEmbeddedMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("writeEmbeddedMetadata", func(t *testing.T) {
		var buf bytes.Buffer
		metadata := TSDumpMetadata{
			Version: "v23.1.0",
			StoreToNodeMap: map[string]string{
				"1": "1",
				"2": "1",
				"3": "2",
				"4": "2",
			},
			CreatedAt: timeutil.Unix(1609459200, 0), // 2021-01-01 00:00:00 UTC
		}

		err := writeEmbeddedMetadata(&buf, metadata)
		require.NoError(t, err)
		require.Greater(t, buf.Len(), 0, "should have written some data")
	})

	t.Run("readEmbeddedMetadata", func(t *testing.T) {
		originalMetadata := TSDumpMetadata{
			Version: "v23.2.1",
			StoreToNodeMap: map[string]string{
				"1": "1",
				"2": "2",
				"3": "3",
			},
			CreatedAt: timeutil.Unix(1640995200, 0), // 2022-01-01 00:00:00 UTC
		}

		var buf bytes.Buffer
		err := writeEmbeddedMetadata(&buf, originalMetadata)
		require.NoError(t, err)

		dec := gob.NewDecoder(&buf)
		readMetadata, err := readEmbeddedMetadata(dec)
		require.NoError(t, err)
		require.NotNil(t, readMetadata)

		require.Equal(t, originalMetadata.Version, readMetadata.Version)
		require.Equal(t, originalMetadata.StoreToNodeMap, readMetadata.StoreToNodeMap)
		require.Equal(t, originalMetadata.CreatedAt.Unix(), readMetadata.CreatedAt.Unix())
	})

	t.Run("readEmbeddedMetadata_NoMetadata", func(t *testing.T) {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		kv := roachpb.KeyValue{
			Key:   roachpb.Key("test-key"),
			Value: roachpb.MakeValueFromString("test-value"),
		}
		err := enc.Encode(kv)
		require.NoError(t, err)

		dec := gob.NewDecoder(&buf)
		metadata, err := readEmbeddedMetadata(dec)
		require.Error(t, err)
		require.Nil(t, metadata)
	})

	t.Run("readEmbeddedMetadata_InvalidMetadata", func(t *testing.T) {
		// Create a buffer with a metadata marker but invalid JSON
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		marker := metadataMarker{
			IsMetadata: true,
			Data:       []byte("invalid json"),
		}
		err := enc.Encode(marker)
		require.NoError(t, err)

		dec := gob.NewDecoder(&buf)
		metadata, err := readEmbeddedMetadata(dec)
		require.Error(t, err)
		require.Nil(t, metadata)
	})
}

// TestTSDumpConversionWithEmbeddedMetadata tests the conversion of tsdump files
// containing embedded metadata to CSV format, verifying metadata extraction.
func TestTSDumpConversionWithEmbeddedMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tmpFile, err := os.CreateTemp("", "tsdump_with_metadata_*.gob")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	metadata := TSDumpMetadata{
		Version: "v23.1.0",
		StoreToNodeMap: map[string]string{
			"1": "1",
			"2": "2",
		},
		CreatedAt: timeutil.Unix(1609459200, 0),
	}
	err = writeEmbeddedMetadata(tmpFile, metadata)
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
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte(actualData))
	require.NoError(t, err)
	tmpFile.Close()

	file, err := os.Open(tmpFile.Name())
	require.NoError(t, err)
	defer file.Close()

	dec := gob.NewDecoder(file)
	readMetadata, err := readEmbeddedMetadata(dec)
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
				var series int
				d.ScanArgs(t, "series-threshold", &series)
				var ddwriter, err = makeDatadogWriter(
					defaultDDSite, d.Cmd == "format-datadog-init", "api-key", series,
					server.Listener.Addr().String(), debugTimeSeriesDumpOpts.noOfUploadWorkers,
				)
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
