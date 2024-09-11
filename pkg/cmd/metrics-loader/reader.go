// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"errors"
	"io"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/prompb"
)

const (
	readBatchSize  = 4096
	readBufferSize = 4 * 1024 * 1024
)

type ReaderOutputFn func(*prompb.TimeSeries)

type Reader interface {
	Read(reader io.Reader, outputFn ReaderOutputFn) error
}

type openMetricsReader struct{}

func NewOpenMetricsReader() Reader {
	return &openMetricsReader{}
}

func (r *openMetricsReader) Read(reader io.Reader, outputFn ReaderOutputFn) error {
	batch := make([]byte, 0, readBufferSize+readBatchSize)
	bufReader := bufio.NewReaderSize(reader, readBufferSize)
	scanner := bufio.NewScanner(bufReader)
	scanner.Split(bufio.ScanLines)
	for i := 1; scanner.Scan(); i++ {
		batch = append(batch, scanner.Bytes()...)
		batch = append(batch, '\n')
		if i%readBatchSize == 0 {
			if err := r.parseBatch(batch, outputFn); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := r.parseBatch(batch, outputFn); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (r *openMetricsReader) parseBatch(input []byte, outputFn ReaderOutputFn) error {
	p := textparse.NewOpenMetricsParser(input)
	for {
		entryType, err := p.Next()
		if err != nil {
			// If we reached the end of the input, we're done.
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		switch entryType {
		case textparse.EntrySeries:
			_, timestamp, value := p.Series()
			var labels labels.Labels
			p.Metric(&labels)

			var timeSeries prompb.TimeSeries
			timeSeries.Labels = make([]prompb.Label, len(labels)+1)
			for i, label := range labels {
				timeSeries.Labels[i] = prompb.Label{
					Name:  label.Name,
					Value: label.Value,
				}
			}
			var sample prompb.Sample
			sample.Value = value
			if timestamp != nil {
				sample.Timestamp = *timestamp
			}
			timeSeries.Samples = []prompb.Sample{sample}
			outputFn(&timeSeries)
		default:
			// Ignore other entry types, including `EntryInvalid`.
		}
	}
}
