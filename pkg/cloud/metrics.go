// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// NilMetrics represents a nil metrics object.
var NilMetrics = (*Metrics)(nil)

// Metrics encapsulates the metrics tracking interactions with cloud storage
// providers.
type Metrics struct {
	// ReadBytes counts the bytes read from cloud storage.
	ReadBytes *metric.Counter
	// WriteBytes counts the bytes written to cloud storage.
	WriteBytes *metric.Counter
}

// MakeMetrics returns a new instance of Metrics.
func MakeMetrics() metric.Struct {
	cloudReadBytes := metric.Metadata{
		Name:        "cloud.read_bytes",
		Help:        "Bytes read from all cloud operations",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudWriteBytes := metric.Metadata{
		Name:        "cloud.write_bytes",
		Help:        "Bytes written by all cloud operations",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	return &Metrics{
		ReadBytes:  metric.NewCounter(cloudReadBytes),
		WriteBytes: metric.NewCounter(cloudWriteBytes),
	}
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct implements the metric.Struct interface.
func (m *Metrics) MetricStruct() {}

// MetricsRecorder is the interface that describes the methods that can be used
// to mutate the metrics corresponding to cloud operations.
type MetricsRecorder interface {
	// RecordReadBytes records the bytes read.
	RecordReadBytes(int64)
	// RecordWriteBytes records the bytes written.
	RecordWriteBytes(int64)
	// Metrics returns the underlying Metrics struct.
	Metrics() *Metrics
}

var _ MetricsRecorder = &Metrics{}

// RecordReadBytes implements the MetricsRecorder interface.
func (m *Metrics) RecordReadBytes(bytes int64) {
	if m == nil {
		return
	}
	m.ReadBytes.Inc(bytes)
}

// RecordWriteBytes implements the MetricsRecorder interface.
func (m *Metrics) RecordWriteBytes(bytes int64) {
	if m == nil {
		return
	}
	m.WriteBytes.Inc(bytes)
}

// Metrics implements the MetricsRecorder interface.
func (m *Metrics) Metrics() *Metrics {
	return m
}

type metricsReadWriter struct {
	metricsRecorder MetricsRecorder
}

func newMetricsReadWriter(m MetricsRecorder) ReadWriterInterceptor {
	return &metricsReadWriter{metricsRecorder: m}
}

// Reader implements the ReadWriterInterceptor interface.
func (m *metricsReadWriter) Reader(
	_ context.Context, _ ExternalStorage, r ioctx.ReadCloserCtx,
) ioctx.ReadCloserCtx {
	return &metricsReader{
		inner:           r,
		metricsRecorder: m.metricsRecorder,
	}
}

// Writer implements the ReadWriterInterceptor interface.
func (m *metricsReadWriter) Writer(
	_ context.Context, _ ExternalStorage, w io.WriteCloser,
) io.WriteCloser {
	return &metricsWriter{
		w:               w,
		metricsRecorder: m.metricsRecorder,
	}
}

var _ ReadWriterInterceptor = &metricsReadWriter{}

type metricsReader struct {
	inner           ioctx.ReadCloserCtx
	metricsRecorder MetricsRecorder
}

// Read implements the ioctx.ReadCloserCtx interface.
func (mr *metricsReader) Read(ctx context.Context, p []byte) (int, error) {
	n, err := mr.inner.Read(ctx, p)
	mr.metricsRecorder.RecordReadBytes(int64(n))
	return n, err
}

// Close implements the ioctx.ReadCloserCtx interface.
func (mr *metricsReader) Close(ctx context.Context) error {
	return mr.inner.Close(ctx)
}

type metricsWriter struct {
	w               io.WriteCloser
	metricsRecorder MetricsRecorder
}

// Write implements the WriteCloser interface.
func (mw *metricsWriter) Write(p []byte) (int, error) {
	n, err := mw.w.Write(p)
	mw.metricsRecorder.RecordWriteBytes(int64(n))
	return n, err
}

// Close implements the WriteCloser interface.
func (mw *metricsWriter) Close() error {
	return mw.w.Close()
}

var _ io.WriteCloser = &metricsWriter{}
