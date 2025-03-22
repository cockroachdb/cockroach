// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"

	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/codahale/hdrhistogram"
)

const (
	// bufferSize is the size of the read buffer for the scanner
	bufferSize = 64 * 1024 // 64KB chunks

	// maxTokenSize is the maximum size of a JSON line
	maxTokenSize = 1024 * 1024 // 1MB max line size
)

// streamingHistogramParser handles efficient parsing of large histogram data files
type streamingHistogramParser struct {
	reader      *bufio.Reader
	exporter    *exporter.OpenMetricsExporter
	processFunc func(*exporter.SnapshotTick) error
}

// newStreamingParser creates a new streaming parser for histogram data
func newStreamingParser(
	content []byte, metricsExporter *exporter.OpenMetricsExporter,
) *streamingHistogramParser {
	reader := bufio.NewReaderSize(bytes.NewReader(content), bufferSize)

	return &streamingHistogramParser{
		reader:   reader,
		exporter: metricsExporter,
		processFunc: func(snapshot *exporter.SnapshotTick) error {
			hist := hdrhistogram.Import(snapshot.Hist)
			return metricsExporter.SnapshotAndWrite(hist, snapshot.Now, snapshot.Elapsed, &snapshot.Name)
		},
	}
}

// parse processes the histogram data in chunks
func (p *streamingHistogramParser) parse() error {
	lineCount := 0

	for {
		// Read a line (JSON object)
		line, isPrefix, err := p.reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return wrapProcessingError(err, "failed to read line %d", lineCount)
		}

		// Handle lines longer than buffer
		fullLine := line
		if isPrefix {
			var buffer bytes.Buffer
			buffer.Write(line)
			for isPrefix {
				line, isPrefix, err = p.reader.ReadLine()
				if err != nil {
					return wrapProcessingError(err, "failed to read oversized line %d", lineCount)
				}
				buffer.Write(line)
			}
			fullLine = buffer.Bytes()
		}

		// Parse and process the snapshot
		var snapshot exporter.SnapshotTick
		if err := json.Unmarshal(fullLine, &snapshot); err != nil {
			return wrapProcessingError(err, "failed to unmarshal snapshot at line %d", lineCount)
		}

		if err := p.processFunc(&snapshot); err != nil {
			return wrapProcessingError(err, "failed to process snapshot at line %d", lineCount)
		}

		lineCount++
	}

	return nil
}
