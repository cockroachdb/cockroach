// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exporter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	mockHistogramName = "mock_histogram"
)

func TestHdrJsonExporter(t *testing.T) {
	exporter := &HdrJsonExporter{}
	t.Run("Validate", func(t *testing.T) {
		err := exporter.Validate("metrics.json")
		assert.NoError(t, err)

		err = exporter.Validate("metrics.txt")
		assert.Error(t, err)
		assert.Equal(t, "file path must end with .json", err.Error())
	})

	t.Run("Init", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)
	})

	t.Run("SnapshotAndWrite", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		start := time.Time{}
		elapsed := time.Second / 2
		name := mockHistogramName
		mockHistogram := hdrhistogram.New(0, 100, 1)
		err = exporter.SnapshotAndWrite(mockHistogram, start, elapsed, &name)
		require.NoError(t, err)
		err = exporter.Close(nil)
		require.NoError(t, err)

		var data map[string]interface{}
		buf, err := os.ReadFile(file.Name())
		require.NoError(t, err)
		err = json.Unmarshal(buf, &data)
		require.NoError(t, err)
		assert.Equal(t, "mock_histogram", data["Name"])

	})

	t.Run("Close", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		err = exporter.Close(nil)
		assert.NoError(t, err)
	})
}

func TestOpenMetricsExporter(t *testing.T) {
	exporter := &OpenMetricsExporter{}
	t.Run("Validate", func(t *testing.T) {
		err := exporter.Validate("metrics.txt")
		assert.NoError(t, err)

		err = exporter.Validate("metrics.json")
		assert.Error(t, err)
		assert.Equal(t, "file path must not end with .json", err.Error())
	})

	t.Run("Init", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)
		assert.NotNil(t, exporter.writer)
	})

	t.Run("SnapshotAndWrite", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		start := time.Time{}
		elapsed := time.Second / 2
		name := mockHistogramName
		mockHistogram := hdrhistogram.New(0, 100, 1)
		err = exporter.SnapshotAndWrite(mockHistogram, start, elapsed, &name)
		require.NoError(t, err)
		err = exporter.Close(nil)
		require.NoError(t, err)

		buf, _ := os.ReadFile(file.Name())
		assert.Contains(t, string(buf), "# TYPE")
	})

	t.Run("Close", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		err = exporter.Close(nil)
		assert.NoError(t, err)
	})
}

func removeFile(file string, t *testing.T) {
	assert.NoError(t, os.Remove(file))
}

func TestOpenMetricsFileWithJson(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		var hdrSnapshot SnapshotTick
		var buf bytes.Buffer
		exporter := &OpenMetricsExporter{}
		writer := io.Writer(&buf)
		exporter.Init(&writer)

		metricLineRegex := regexp.MustCompile(`^(\w+){([^}]*)} ([\d.e+-]+) ([\d.e+-]+)$`)

		verifyOpenMetricsWithJson := func(b []byte, hist *hdrhistogram.Histogram, name string, elapsed time.Duration) error {
			counts := hist.Export().Counts
			countIdx := 0

			sumOfCounts := int64(0)
			for _, count := range counts {
				sumOfCounts += count
			}

			scanner := bufio.NewScanner(bytes.NewReader(b))
			sumTillNow := int64(0)
			for scanner.Scan() {
				count := counts[countIdx]
				line := scanner.Text()
				if strings.Contains(line, "# EOF") {
					return nil
				}

				if strings.HasPrefix(line, "#") {
					metric := strings.Split(line[1:], " ")
					if metric[3] != "summary" && metric[3] != "gauge" {
						return errors.Errorf("invalid name and type: %s", line)
					}
					continue
				}

				if matches := metricLineRegex.FindStringSubmatch(line); matches != nil {
					metricName := matches[1]
					valueStr := matches[3]
					quantile, err := strconv.ParseFloat(strings.Replace(strings.Split(matches[2], "=")[1], "\"", "", -1), 64)
					if err != nil {
						return err
					}
					countValue, err := strconv.ParseFloat(valueStr, 64)
					if err != nil {
						return err
					}
					if strings.HasSuffix(metricName, "_sum") {
						if countValue != 0 {
							return errors.Errorf("invalid summary sum: %f", countValue)
						}
					}
					if strings.HasSuffix(metricName, "_count") {
						if int64(countValue) != hist.TotalCount() {
							return errors.Errorf("invalid summary count: %f", countValue)
						}
					}

					// openmetrics has quantiles in absolute values (i.e 0 to 1) an hdrHistogram has quantiles from 0 to 100.
					// Multiplying by 100 to make a fair comparison
					quantile *= 100
					if int64(countValue) != hist.ValueAtQuantile(quantile) {
						return errors.Errorf("invalid summary quantile: %f", quantile)
					}

					if strings.HasSuffix(metricName, "highest_trackable_value") {
						if int64(countValue) != hist.HighestTrackableValue() {
							return errors.Errorf("wrong highest trackable value: %f, expected: %d", countValue, hist.HighestTrackableValue())
						}
					}

					if strings.HasSuffix(metricName, "elapsed") {
						if int64(countValue) != elapsed.Milliseconds() {
							return errors.Errorf("wrong elapsed: %f, expected: %d", countValue, elapsed.Milliseconds())
						}
					}

					sumTillNow += count
					countIdx++
				}
			}
			return nil
		}

		datadriven.RunTest(t, path, func(t *testing.T, data *datadriven.TestData) string {
			require.NoError(t, json.Unmarshal([]byte(data.Input), &hdrSnapshot))
			hist := hdrhistogram.Import(hdrSnapshot.Hist)
			require.NoError(t, exporter.SnapshotAndWrite(hist, hdrSnapshot.Now, hdrSnapshot.Elapsed, &hdrSnapshot.Name))
			require.NoError(t, exporter.Close(nil))

			require.NoError(t, verifyOpenMetricsWithJson(buf.Bytes(), hist, hdrSnapshot.Name, hdrSnapshot.Elapsed))
			return buf.String()
		})
	})

}
