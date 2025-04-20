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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
)

const (
	// Reduced buffer sizes to prevent excessive memory use
	scannerBufferSize = 1 * 1024 * 1024 // 1MB initial buffer
	statsMaxBuffer    = 5 * 1024 * 1024 // 5MB max buffer
)

type StatsExporterConverter struct {
	sink                   sink.Sink
	HigherBetterMetricsSet map[string]bool
	MetricLabels           map[string][]model.Label
}

func (s StatsExporterConverter) Convert(labels []model.Label, src model.FileInfo) (err error) {
	// Get a buffer from the pool for the scanner
	scannerBuf := getBuffer()
	defer putBuffer(scannerBuf)

	// Create scanner with pooled buffer
	scanner := bufio.NewScanner(bytes.NewReader(src.Content))

	// Ensure the buffer has enough capacity
	if scannerBuf.Cap() < scannerBufferSize {
		// Log a warning about the buffer size issue
		fmt.Printf("Warning: Buffer capacity (%d) is less than required size (%d) for file %s. Creating a new buffer.\n",
			scannerBuf.Cap(), scannerBufferSize, src.Path)

		// If the buffer doesn't have enough capacity, create a new one
		scannerBuf = bytes.NewBuffer(make([]byte, 0, scannerBufferSize))
	}

	// Use the buffer with the correct size
	scanner.Buffer(scannerBuf.Bytes()[:0], statsMaxBuffer)

	// Get buffers from pool for metrics
	metricsBuf := getBuffer()
	defer putBuffer(metricsBuf)
	metricsBuffer := bufio.NewWriter(metricsBuf)

	totalMetricsBuf := getBuffer()
	defer putBuffer(totalMetricsBuf)
	totalMetricsWriter := bufio.NewWriter(totalMetricsBuf)

	// Ensure proper cleanup of writers
	defer func() {
		if flushErr := metricsBuffer.Flush(); flushErr != nil && err == nil {
			err = fmt.Errorf("failed to flush metrics buffer for %s: %w", src.Path, flushErr)
			return
		}
		if flushErr := totalMetricsWriter.Flush(); flushErr != nil && err == nil {
			err = fmt.Errorf("failed to flush total metrics buffer for %s: %w", src.Path, flushErr)
			return
		}
	}()

	// Process the file
	if err = s.processFile(scanner, labels, metricsBuffer, totalMetricsWriter, src); err != nil {
		return fmt.Errorf("failed to process file %s: %w", src.Path, err)
	}

	// Write EOF marker
	if _, err = totalMetricsWriter.WriteString("# EOF"); err != nil {
		return fmt.Errorf("failed to write EOF marker for %s: %w", src.Path, err)
	}

	// Ensure all writes are flushed before sinking
	if err = metricsBuffer.Flush(); err != nil {
		return fmt.Errorf("failed to flush metrics buffer for %s: %w", src.Path, err)
	}
	if err = totalMetricsWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush total metrics buffer for %s: %w", src.Path, err)
	}

	// Sink the buffers
	if err = s.sink.Sink(metricsBuf, src.Path, RawStatsFile); err != nil {
		return fmt.Errorf("failed to sink raw metrics for %s: %w", src.Path, err)
	}

	if err = s.sink.Sink(totalMetricsBuf, src.Path, AggregatedStatsFile); err != nil {
		return fmt.Errorf("failed to sink aggregated metrics for %s: %w", src.Path, err)
	}

	return nil
}

func (s StatsExporterConverter) processFile(
	scanner *bufio.Scanner,
	labels []model.Label,
	metricsBuffer *bufio.Writer,
	totalMetricsWriter *bufio.Writer,
	src model.FileInfo,
) error {
	for scanner.Scan() {
		line := scanner.Text()

		var clusterStatsRun clusterstats.ClusterStatRun
		if err := json.Unmarshal([]byte(line), &clusterStatsRun); err != nil {
			return err
		}

		// Process total metrics
		totalClusterRun := clusterstats.ClusterStatRun{
			Total: clusterStatsRun.Total,
		}
		clusterStatsRun.Total = nil

		// Process regular metrics
		labelString := util.LabelMapToString(getLabelMap(src, labels, false, "", nil))
		openMetricsBuffer, err := clusterstats.SerializeOpenmetricsReport(clusterStatsRun, &labelString)
		if err != nil {
			return err
		}
		// Write the buffer contents and immediately return it to the pool
		if _, err = metricsBuffer.Write(openMetricsBuffer.Bytes()); err != nil {
			putBuffer(openMetricsBuffer)
			return err
		}
		putBuffer(openMetricsBuffer)

		// Process total metrics
		for key, val := range totalClusterRun.Total {
			labelString = util.LabelMapToString(getLabelMap(src, labels, true, key, s.MetricLabels[key]))
			currentStatsMap := make(map[string]float64)
			currentStatsMap[key] = val
			currentTotalStatsRun := clusterstats.ClusterStatRun{Total: currentStatsMap}

			// Get a buffer from the pool for temporary operations
			tempBuf := getBuffer()
			tempBuffer, err := clusterstats.SerializeOpenmetricsReport(currentTotalStatsRun, &labelString)
			if err != nil {
				putBuffer(tempBuf)
				return err
			}

			lines := bytes.Split(tempBuffer.Bytes(), []byte("\n"))
			// Remove EOF markers
			if len(lines) > 0 && bytes.Equal(lines[len(lines)-1], []byte("# EOF")) {
				lines = lines[:len(lines)-1]
			} else if len(lines) > 1 && bytes.Equal(lines[len(lines)-2], []byte("# EOF")) {
				lines = lines[:len(lines)-2]
			}

			// Write lines to total metrics
			for _, line := range lines {
				if _, err = totalMetricsWriter.Write(line); err != nil {
					// putBuffer(tempBuf)
					return err
				}
				if _, err = totalMetricsWriter.Write([]byte("\n")); err != nil {
					// putBuffer(tempBuf)
					return err
				}
			}
			putBuffer(tempBuf)
		}
	}
	return scanner.Err()
}

func NewStatsExporterConverter(sink sink.Sink, metricSpec []model.Metric) Converter {
	HigherBetterMetricsSet := make(map[string]bool)
	metricLabels := make(map[string][]model.Label)

	for _, spec := range metricSpec {
		for _, val := range spec.HigherBetterMetrics {
			HigherBetterMetricsSet[val] = true
		}

		for _, val := range spec.MetricLabels {
			metricLabels[val.Name] = append(metricLabels[val.Name], val.Labels...)
		}
	}

	return &StatsExporterConverter{
		sink:                   sink,
		HigherBetterMetricsSet: HigherBetterMetricsSet,
		MetricLabels:           metricLabels,
	}
}
