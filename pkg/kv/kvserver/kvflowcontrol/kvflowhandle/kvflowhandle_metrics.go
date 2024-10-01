// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowhandle

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	streamsConnected = metric.Metadata{
		Name:        "kvadmission.flow_handle.streams_connected",
		Help:        "Number of times we've connected to a stream, at the handle level",
		Measurement: "Streams",
		Unit:        metric.Unit_COUNT,
	}

	streamsDisconnected = metric.Metadata{
		Name:        "kvadmission.flow_handle.streams_disconnected",
		Help:        "Number of times we've disconnected from a stream, at the handle level",
		Measurement: "Streams",
		Unit:        metric.Unit_COUNT,
	}

	requestsWaiting = metric.Metadata{
		Name:        "kvadmission.flow_handle.%s_requests_waiting",
		Help:        "Number of %s requests waiting for flow tokens, at the handle level",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	requestsAdmitted = metric.Metadata{
		Name:        "kvadmission.flow_handle.%s_requests_admitted",
		Help:        "Number of %s requests admitted by the flow handle",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	requestsErrored = metric.Metadata{
		Name:        "kvadmission.flow_handle.%s_requests_errored",
		Help:        "Number of %s requests that errored out while waiting for flow tokens, at the handle level",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	waitDuration = metric.Metadata{
		Name:        "kvadmission.flow_handle.%s_wait_duration",
		Help:        "Latency histogram for time %s requests spent waiting for flow tokens, at the handle level",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// annotateMetricTemplateWithWorkClass uses the given metric template to build
// one suitable for the specific work class.
func annotateMetricTemplateWithWorkClass(
	wc admissionpb.WorkClass, tmpl metric.Metadata,
) metric.Metadata {
	rv := tmpl
	rv.Name = fmt.Sprintf(tmpl.Name, wc)
	rv.Help = fmt.Sprintf(tmpl.Help, wc)
	return rv
}

// Metrics is a metric.Struct for all kvflowcontrol.Handles.
type Metrics struct {
	StreamsConnected    *metric.Counter
	StreamsDisconnected *metric.Counter
	RequestsWaiting     [admissionpb.NumWorkClasses]*metric.Gauge
	RequestsAdmitted    [admissionpb.NumWorkClasses]*metric.Counter
	RequestsErrored     [admissionpb.NumWorkClasses]*metric.Counter
	WaitDuration        [admissionpb.NumWorkClasses]metric.IHistogram
}

var _ metric.Struct = &Metrics{}

// NewMetrics returns a new instance of Metrics.
func NewMetrics(registry *metric.Registry) *Metrics {
	m := &Metrics{
		StreamsConnected:    metric.NewCounter(streamsConnected),
		StreamsDisconnected: metric.NewCounter(streamsDisconnected),
	}

	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.RequestsWaiting[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClass(wc, requestsWaiting),
		)
		m.RequestsAdmitted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, requestsAdmitted),
		)
		m.RequestsErrored[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, requestsErrored),
		)
		m.WaitDuration[wc] = metric.NewHistogram(
			metric.HistogramOptions{
				Metadata:     annotateMetricTemplateWithWorkClass(wc, waitDuration),
				Duration:     base.DefaultHistogramWindowInterval(),
				BucketConfig: metric.IOLatencyBuckets,
				Mode:         metric.HistogramModePrometheus,
			},
		)
	}

	registry.AddMetricStruct(m)
	return m
}

func (m *Metrics) onWaiting(class admissionpb.WorkClass) {
	m.RequestsWaiting[class].Inc(1)
}

func (m *Metrics) onAdmitted(class admissionpb.WorkClass, dur time.Duration) {
	m.RequestsAdmitted[class].Inc(1)
	m.RequestsWaiting[class].Dec(1)
	m.WaitDuration[class].RecordValue(dur.Nanoseconds())
}

func (m *Metrics) onErrored(class admissionpb.WorkClass, dur time.Duration) {
	m.RequestsErrored[class].Inc(1)
	m.RequestsWaiting[class].Dec(1)
	m.WaitDuration[class].RecordValue(dur.Nanoseconds())
}

// MetricStruct implements the metric.Struct interface.
func (m *Metrics) MetricStruct() {}
