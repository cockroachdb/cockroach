// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

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
	var data *DatadogSeries
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
				_, err := w.emitDataDogMetrics([]DatadogSeries{*data})
				require.NoError(t, err)
			}
			data = &DatadogSeries{
				Metric: metricName,
			}
			source = nameValueTimestamp[1]
			data.Tags = append(data.Tags, fmt.Sprintf("%s:%s", storeNodeKey, nameValueTimestamp[1]))
		}
		value, err := strconv.ParseFloat(nameValueTimestamp[2], 64)
		require.NoError(t, err)
		ts, err := strconv.ParseInt(nameValueTimestamp[3], 10, 64)
		require.NoError(t, err)
		data.Points = append(data.Points, DatadogPoint{
			Value:     value,
			Timestamp: ts,
		})
	}
	_, err := w.emitDataDogMetrics([]DatadogSeries{*data})
	require.NoError(t, err)
}

func TestTsDumpFormatsDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer testutils.TestingHook(&getCurrentTime, func() time.Time {
		return time.Date(2024, 11, 14, 0, 0, 0, 0, time.UTC)
	})()

	datadriven.Walk(t, "testdata/tsdump", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var w tsWriter
			switch d.Cmd {
			case "format-datadog":
				debugTimeSeriesDumpOpts.clusterLabel = "test-cluster"
				debugTimeSeriesDumpOpts.clusterID = "test-cluster-id"
				debugTimeSeriesDumpOpts.zendeskTicket = "zd-test"
				debugTimeSeriesDumpOpts.organizationName = "test-org"
				debugTimeSeriesDumpOpts.userName = "test-user"
				var testReqs []*http.Request
				var series int
				d.ScanArgs(t, "series-threshold", &series)
				var ddwriter = makeDatadogWriter("https://example.com/data", false, "api-key", series, func(req *http.Request) error {
					testReqs = append(testReqs, req)
					return nil
				})

				parseDDInput(t, d.Input, ddwriter)

				out := strings.Builder{}
				for _, tr := range testReqs {
					rc, err := tr.GetBody()
					require.NoError(t, err)
					zipR, err := gzip.NewReader(rc)
					require.NoError(t, err)
					body, err := io.ReadAll(zipR)
					require.NoError(t, err)
					out.WriteString(fmt.Sprintf("%s: %s\nDD-API-KEY: %s\nBody: %s", tr.Method, tr.URL, tr.Header.Get("DD-API-KEY"), body))
				}
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
