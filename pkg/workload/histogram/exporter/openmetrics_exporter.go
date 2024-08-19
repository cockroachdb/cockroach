// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exporter

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/prometheus/common/expfmt"
)

// OpenmetricsExporter exports metrics in openmetrics format
type OpenmetricsExporter struct {
	writer *bufio.Writer
	Labels *map[string]string
}

func (o *OpenmetricsExporter) Validate(filePath string) error {
	if strings.HasSuffix(filePath, ".json") {
		return fmt.Errorf("file path must not end with .json")
	}
	return nil
}

func (o *OpenmetricsExporter) Init(w *io.Writer) error {
	o.writer = bufio.NewWriter(*w)
	return nil
}

func (o *OpenmetricsExporter) SnapshotAndWrite(
	hist *hdrhistogram.Histogram, now time.Time, elapsed time.Duration, name *string,
) error {
	if _, err := expfmt.MetricFamilyToOpenMetrics(
		o.writer,
		// expfmt.MetricFamilyToOpenMetrics expects prometheus.MetricFamily
		// so converting HdrHistogram to MetricFamily here
		ConvertHdrHistogramToPrometheusMetricFamily(hist, name, now, o.Labels),
	); err != nil {
		return err
	}
	return nil
}

func (o *OpenmetricsExporter) Close(f func() error) error {

	// Adds the `#EOF` in the openmetrics file
	if _, err := expfmt.FinalizeOpenMetrics(o.writer); err != nil {
		return err
	}

	if err := o.writer.Flush(); err != nil {
		return err
	}

	if f != nil {
		return f()
	}
	return nil
}
