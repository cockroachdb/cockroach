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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// NilMetrics represents a nil metrics object.
var NilMetrics = (*Metrics)(nil)

// Metrics encapsulates the metrics tracking interactions with cloud storage
// providers.
type Metrics struct {
	// Readers counts the cloud storage readers opened.
	CreatedReaders *metric.Counter
	// OpenReaders is the number of currently open cloud readers.
	OpenReaders *metric.Gauge
	// ReadBytes counts the bytes read from cloud storage.
	ReadBytes *metric.Counter

	// Writers counts the cloud storage writers opened.
	CreatedWriters *metric.Counter
	// OpenReaders is the number of currently open cloud writers.
	OpenWriters *metric.Gauge
	// WriteBytes counts the bytes written to cloud storage.
	WriteBytes *metric.Counter

	// Listings counts the listing calls made to cloud storage.
	Listings *metric.Counter
	// ListingResults counts the listing results from cloud storage.
	ListingResults *metric.Counter

	// ConnsOpened, ConnsReused and TLSHandhakes track connection http info for cloud
	// storage when collecting this info is enabled.
	ConnsOpened, ConnsReused, TLSHandhakes *metric.Counter
}

// MakeMetrics returns a new instance of Metrics.
func MakeMetrics() metric.Struct {
	cloudReaders := metric.Metadata{
		Name:        "cloud.readers_opened",
		Help:        "Readers opened by all cloud operations",
		Measurement: "Files",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudReadBytes := metric.Metadata{
		Name:        "cloud.read_bytes",
		Help:        "Bytes read from all cloud operations",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudWriters := metric.Metadata{
		Name:        "cloud.writers_opened",
		Help:        "Writers opened by all cloud operations",
		Measurement: "Files",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudWriteBytes := metric.Metadata{
		Name:        "cloud.write_bytes",
		Help:        "Bytes written by all cloud operations",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	listings := metric.Metadata{
		Name:        "cloud.listings",
		Help:        "Listing operations by all cloud operations",
		Measurement: "Listing",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	listingResults := metric.Metadata{
		Name:        "cloud.listing_results",
		Help:        "Listing results by all cloud operations",
		Measurement: "Listing",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	connsOpened := metric.Metadata{
		Name:        "cloud.conns_opened",
		Help:        "HTTP connections opened by cloud operations",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	connsReused := metric.Metadata{
		Name:        "cloud.conns_reused",
		Help:        "HTTP connections reused by cloud operations",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	tlsHandhakes := metric.Metadata{
		Name:        "cloud.tls_handshakes",
		Help:        "TLS handshakes done by cloud operations",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	cloudOpenReaders := metric.Metadata{
		Name:        "cloud.open_readers",
		Help:        "Currently open readers for cloud IO",
		Measurement: "Files",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	cloudOpenWriters := metric.Metadata{
		Name:        "cloud.open_writers",
		Help:        "Currently open writers for cloud IO",
		Measurement: "Files",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	return &Metrics{
		CreatedReaders: metric.NewCounter(cloudReaders),
		OpenReaders:    metric.NewGauge(cloudOpenReaders),
		ReadBytes:      metric.NewCounter(cloudReadBytes),
		CreatedWriters: metric.NewCounter(cloudWriters),
		OpenWriters:    metric.NewGauge(cloudOpenWriters),
		WriteBytes:     metric.NewCounter(cloudWriteBytes),
		Listings:       metric.NewCounter(listings),
		ListingResults: metric.NewCounter(listingResults),
		ConnsOpened:    metric.NewCounter(connsOpened),
		ConnsReused:    metric.NewCounter(connsReused),
		TLSHandhakes:   metric.NewCounter(tlsHandhakes),
	}
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct implements the metric.Struct interface.
func (m *Metrics) MetricStruct() {}

// Reader implements the ReadWriterInterceptor interface.
func (m *Metrics) Reader(
	ctx context.Context, _ ExternalStorage, r ioctx.ReadCloserCtx, name string,
) ioctx.ReadCloserCtx {
	if m == nil {
		return r
	}
	m.CreatedReaders.Inc(1)
	m.OpenReaders.Inc(1)
	log.VInfof(ctx, 1, "%s reader opened", name)
	return &metricsReader{
		inner: r,
		m:     m,
		name:  name,
	}
}

// Writer implements the ReadWriterInterceptor interface.
func (m *Metrics) Writer(ctx context.Context, _ ExternalStorage, w io.WriteCloser, name string) io.WriteCloser {
	if m == nil {
		return w
	}
	m.CreatedWriters.Inc(1)
	m.OpenWriters.Inc(1)
	log.VInfof(ctx, 1, "%s writer opened", name)
	return &metricsWriter{
		w:    w,
		m:    m,
		name: name,
	}
}

type metricsReader struct {
	inner  ioctx.ReadCloserCtx
	m      *Metrics
	closed bool
	name   string
}

// Read implements the ioctx.ReadCloserCtx interface.
func (mr *metricsReader) Read(ctx context.Context, p []byte) (int, error) {
	n, err := mr.inner.Read(ctx, p)
	mr.m.ReadBytes.Inc(int64(n))
	return n, err
}

// Close implements the ioctx.ReadCloserCtx interface.
func (mr *metricsReader) Close(ctx context.Context) error {
	if !mr.closed {
		mr.m.OpenReaders.Dec(1)
		mr.closed = true
		log.Infof(ctx, "%s reader closed", mr.name)
	}
	return mr.inner.Close(ctx)
}

type metricsWriter struct {
	w      io.WriteCloser
	m      *Metrics
	closed bool
	name   string
}

// Write implements the WriteCloser interface.
func (mw *metricsWriter) Write(p []byte) (int, error) {
	n, err := mw.w.Write(p)
	mw.m.WriteBytes.Inc(int64(n))
	return n, err
}

// Close implements the WriteCloser interface.
func (mw *metricsWriter) Close() error {
	if !mw.closed {
		mw.m.OpenWriters.Dec(1)
		mw.closed = true
		log.VInfof(context.Background(), 1, "%s writer closed", mw.name)
	}
	return mw.w.Close()
}

var _ io.WriteCloser = &metricsWriter{}
