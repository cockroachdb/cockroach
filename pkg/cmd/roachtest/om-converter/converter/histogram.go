// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

// HistogramConverter handles the conversion of histogram data to OpenMetrics format
type HistogramConverter struct {
	specs []registry.TestSpec
	sink  sink.Sink
}

// NewHistogramConverter creates a new histogram converter instance
func NewHistogramConverter(sink sink.Sink, specs []registry.TestSpec) *HistogramConverter {
	return &HistogramConverter{
		sink:  sink,
		specs: specs,
	}
}

// Convert processes histogram data and converts it to OpenMetrics format
func (hc *HistogramConverter) Convert(labels []model.Label, src model.FileInfo) error {
	// Extract test information
	runDate, testName := getTestDateAndName(src)
	postProcessFn := hc.getTestPostProcessFunction(testName)
	if postProcessFn == nil {
		fmt.Printf("post-process function not found for test: %s\n", testName)
		return nil
	}

	// Get buffers from pool instead of creating new ones
	rawBuffer := getBuffer()
	aggregatedBuffer := getBuffer()

	// Ensure buffers are returned to the pool on function exit
	defer putBuffer(rawBuffer)
	defer putBuffer(aggregatedBuffer)

	metricsExporter, err := hc.initializeExporter(rawBuffer, src, labels)
	if err != nil {
		return errors.Wrap(err, "failed to initialize exporter")
	}

	// Process histogram data
	if err := hc.processHistogramData(metricsExporter, src.Content); err != nil {
		return errors.Wrap(err, "failed to process histogram data")
	}

	// Handle raw metrics output
	if err := hc.writeRawMetrics(metricsExporter, rawBuffer, src.Path); err != nil {
		return errors.Wrap(err, "failed to write raw metrics")
	}

	// Handle aggregated metrics output
	if err := hc.writeAggregatedMetrics(rawBuffer, aggregatedBuffer, postProcessFn, src.Path, runDate, testName); err != nil {
		return errors.Wrap(err, "failed to write aggregated metrics")
	}

	return nil
}

// initializeExporter sets up the OpenMetrics exporter with proper configuration
func (hc *HistogramConverter) initializeExporter(
	buf *bytes.Buffer, src model.FileInfo, labels []model.Label,
) (*exporter.OpenMetricsExporter, error) {
	omExporter := &exporter.OpenMetricsExporter{}
	writer := io.Writer(buf)
	omExporter.Init(&writer)

	labelMap := getLabelMap(src, labels)
	omExporter.SetLabels(&labelMap)

	return omExporter, nil
}

// processHistogramData reads and processes histogram data from the source content
func (hc *HistogramConverter) processHistogramData(
	metricsExporter *exporter.OpenMetricsExporter, content []byte,
) error {
	scanner := bufio.NewScanner(bytes.NewReader(content))

	for scanner.Scan() {
		snapshot, err := parseHistogramSnapshot(scanner.Text())
		if err != nil {
			return errors.Wrap(err, "failed to parse histogram snapshot")
		}

		if err := writeHistogramSnapshot(metricsExporter, snapshot); err != nil {
			return errors.Wrap(err, "failed to write histogram snapshot")
		}
	}

	return scanner.Err()
}

// parseHistogramSnapshot parses a single line of histogram data
func parseHistogramSnapshot(line string) (*exporter.SnapshotTick, error) {
	var snapshot exporter.SnapshotTick
	if err := json.Unmarshal([]byte(line), &snapshot); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal snapshot")
	}
	return &snapshot, nil
}

// writeHistogramSnapshot writes a single histogram snapshot to the exporter
func writeHistogramSnapshot(
	metricsExporter *exporter.OpenMetricsExporter, snapshot *exporter.SnapshotTick,
) error {
	hist := hdrhistogram.Import(snapshot.Hist)
	return metricsExporter.SnapshotAndWrite(hist, snapshot.Now, snapshot.Elapsed, &snapshot.Name)
}

// writeRawMetrics handles writing the raw metrics data
func (hc *HistogramConverter) writeRawMetrics(
	metricsExporter *exporter.OpenMetricsExporter, buffer *bytes.Buffer, sourcePath string,
) error {
	return metricsExporter.Close(func() error {
		path := strings.Trim(strings.TrimSuffix(sourcePath, "stats.json"), `/`)
		return hc.sink.Sink(buffer, path, RawStatsFile)
	})
}

// writeAggregatedMetrics handles the aggregation and writing of processed metrics
func (hc *HistogramConverter) writeAggregatedMetrics(
	rawBuffer *bytes.Buffer,
	aggregatedBuffer *bytes.Buffer,
	postProcessFn func(string, *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error),
	sourcePath string,
	runDate string,
	testName string,
) error {
	histograms, finalLabels, err := roachtestutil.GetHistogramMetrics(rawBuffer)
	aggregatedMetrics, err := roachtestutil.PostProcessMetrics(testName, postProcessFn, histograms)
	if err != nil {
		return errors.Wrap(err, "failed to post-process metrics")
	}

	parsedTime, err := time.Parse("20060102", strings.Split(runDate, "-")[0])
	if err != nil {
		return errors.Wrap(err, "failed to parse run date")
	}
	if err := roachtestutil.GetAggregatedMetricBytes(aggregatedMetrics, finalLabels, parsedTime, aggregatedBuffer); err != nil {
		return errors.Wrap(err, "failed to get aggregated metric bytes")
	}

	return hc.sink.Sink(aggregatedBuffer, strings.Trim(strings.TrimSuffix(sourcePath, "stats.json"), `/`), AggregatedStatsFile)
}

// getTestPostProcessFunction finds the post-process function for a given test
func (hc *HistogramConverter) getTestPostProcessFunction(
	testName string,
) func(string, *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {
	for _, spec := range hc.specs {
		if spec.Name == testName {
			return spec.GetPostProcessWorkloadMetricsFunction()
		}
	}
	return nil
}
