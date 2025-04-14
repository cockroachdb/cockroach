// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
)

type TpceConverter struct {
	sink sink.Sink
}

func (t *TpceConverter) Convert(labels []model.Label, src model.FileInfo) (err error) {
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

	labelString := util.LabelMapToString(getLabelMap(src, labels, true, "", nil))
	runDate, _ := getTestDateAndName(src)
	timestamp, err := time.Parse("20060102", strings.Split(runDate, "-")[0])
	if err != nil {
		return fmt.Errorf("failed to parse run date for %s: %w", src.Path, err)
	}

	// Get a buffer from the pool for metrics
	metricsBuf := getBuffer()
	defer putBuffer(metricsBuf)

	for scanner.Scan() {
		line := scanner.Text()
		var metrics tests.TpceMetrics
		if err := json.Unmarshal([]byte(line), &metrics); err != nil {
			return fmt.Errorf("failed to unmarshal metrics for %s: %w", src.Path, err)
		}
		openmetricBytes := tests.GetTpceOpenmetricsBytes(metrics, "0", labelString, timestamp.Unix())
		if _, err = metricsBuf.Write(openmetricBytes); err != nil {
			return fmt.Errorf("failed to write metrics for %s: %w", src.Path, err)
		}
	}

	if err = scanner.Err(); err != nil {
		return fmt.Errorf("scanner error for %s: %w", src.Path, err)
	}

	if err = t.sink.Sink(metricsBuf, src.Path, AggregatedStatsFile); err != nil {
		return fmt.Errorf("failed to sink metrics for %s: %w", src.Path, err)
	}

	return nil
}

func NewTpceConverter(sink sink.Sink) Converter {
	return &TpceConverter{
		sink: sink,
	}
}
