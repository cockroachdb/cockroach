// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package histogram

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/prometheus/common/expfmt"
)

// Exporter is used to export workload histogram metrics to a file that is pre-created.
type Exporter interface {
	// Validate is an optional method that can be used to do any kind of validation of the filepath provided.
	// Eg: in a json exporter, the file name extension should be json etc.
	// This should be called before Init
	Validate(filePath string) error
	// Init is used to initialize objects of the exporter. Should be called after Validation.
	Init(w *io.Writer) error
	// SnapshotAndWrite is used to take the snapshot of the histogram and write to the os.File provided in Init
	SnapshotAndWrite(Tick) error
	// Close is used to close and clean any objects that were initialized in Init. Should be called at the end of your program.
	Close(f func() error) error
}

// HdrJsonExporter exports hdr json metrics specified in SnapshotTick
type HdrJsonExporter struct {
	jsonEnc *json.Encoder
}

func (h *HdrJsonExporter) Validate(filePath string) error {
	if !strings.HasSuffix(filePath, ".json") {
		return fmt.Errorf("file path must end with .json")
	}
	return nil
}

func (h *HdrJsonExporter) Init(w *io.Writer) error {
	h.jsonEnc = json.NewEncoder(*w)
	return nil
}

func (h *HdrJsonExporter) SnapshotAndWrite(t Tick) error {
	if err := h.jsonEnc.Encode(t.Snapshot()); err != nil {
		return err
	}
	return nil
}

func (h *HdrJsonExporter) Close(f func() error) error {
	if f != nil {
		return f()
	}
	return nil
}

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

func (o *OpenmetricsExporter) SnapshotAndWrite(t Tick) error {
	if _, err := expfmt.MetricFamilyToOpenMetrics(
		o.writer,
		// expfmt.MetricFamilyToOpenMetrics expects prometheus.MetricFamily
		// so converting HdrHistogram to MetricFamily here
		ConvertHdrHistogramToPrometheusMetricFamily(t.Hist, &t.Name, t.Now, o.Labels),
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
