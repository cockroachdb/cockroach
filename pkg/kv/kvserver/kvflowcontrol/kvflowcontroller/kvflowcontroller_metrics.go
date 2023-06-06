// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowcontroller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	// Metric templates for the flow token controller. We make use of aggmetrics
	// to get per-tenant metrics and an aggregation across all tenants. Within a
	// tenant, we maintain flow tokens for each admissionpb.WorkClass with
	// appropriately segmented metrics. We don't export metrics at a per
	// kvflowcontrol.Stream-level; streams are identified (partly) by the store
	// ID receiving replication traffic, which is a high cardinality measure
	// (storeIDs could ratchet up arbitrarily). The similar argument technically
	// applies for per-tenant metrics, but there we'd rather eat the cost.
	//
	// TODO(irfansharif): Actually use aggmetrics.
	// TODO(irfansharif): Consider aggregated metrics per remote store,
	// aggregated across all tenants.
	// TODO(irfansharif): To improve upon the tenant*stores cardinality,
	// consider "inspectz" style pages to give token counts and queue lengths of
	// individual buckets (#66772).

	flowTokensAvailable = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_tokens_available",
		Help:        "Flow tokens available for %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	flowTokensDeducted = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_tokens_deducted",
		Help:        "Flow tokens deducted by %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	flowTokensReturned = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_tokens_returned",
		Help:        "Flow tokens returned by %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	flowTokensUnaccounted = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_tokens_unaccounted",
		Help:        "Flow tokens returned by %s requests that were unaccounted for, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	requestsWaiting = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_requests_waiting",
		Help:        "Number of %s requests waiting for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	requestsAdmitted = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_requests_admitted",
		Help:        "Number of %s requests admitted by the flow controller",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	requestsErrored = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_requests_errored",
		Help:        "Number of %s requests that errored out while waiting for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	requestsBypassed = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_requests_bypassed",
		Help:        "Number of %s waiting requests that bypassed the flow controller due to disconnecting streams",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	waitDuration = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_wait_duration",
		Help:        "Latency histogram for time %s requests spent waiting for flow tokens",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	totalStreamCount = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_stream_count",
		Help:        "Total number of replication streams for %s requests",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}

	blockedStreamCount = metric.Metadata{
		Name:        "kvadmission.flow_controller.%s_blocked_stream_count",
		Help:        "Number of replication streams with no flow tokens available for %s requests",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
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

type metrics struct {
	FlowTokensAvailable   [admissionpb.NumWorkClasses]*metric.Gauge
	FlowTokensDeducted    [admissionpb.NumWorkClasses]*metric.Counter
	FlowTokensReturned    [admissionpb.NumWorkClasses]*metric.Counter
	FlowTokensUnaccounted [admissionpb.NumWorkClasses]*metric.Counter
	RequestsWaiting       [admissionpb.NumWorkClasses]*metric.Gauge
	RequestsAdmitted      [admissionpb.NumWorkClasses]*metric.Counter
	RequestsErrored       [admissionpb.NumWorkClasses]*metric.Counter
	RequestsBypassed      [admissionpb.NumWorkClasses]*metric.Counter
	WaitDuration          [admissionpb.NumWorkClasses]metric.IHistogram
	TotalStreamCount      [admissionpb.NumWorkClasses]*metric.Gauge
	BlockedStreamCount    [admissionpb.NumWorkClasses]*metric.Gauge
}

var _ metric.Struct = &metrics{}

func newMetrics(c *Controller) *metrics {
	m := &metrics{}
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		wc := wc // copy loop variable
		m.FlowTokensAvailable[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClass(wc, flowTokensAvailable),
			func() int64 {
				sum := int64(0)
				c.mu.Lock()
				defer c.mu.Unlock()
				for _, wbc := range c.mu.buckets {
					sum += int64(wbc.tokens[wc])
				}
				return sum
			},
		)
		m.FlowTokensDeducted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, flowTokensDeducted),
		)
		m.FlowTokensReturned[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, flowTokensReturned),
		)
		m.FlowTokensUnaccounted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, flowTokensUnaccounted),
		)
		m.RequestsWaiting[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClass(wc, requestsWaiting),
		)
		m.RequestsAdmitted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, requestsAdmitted),
		)
		m.RequestsBypassed[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, requestsBypassed),
		)
		m.RequestsErrored[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, requestsErrored),
		)
		m.WaitDuration[wc] = metric.NewHistogram(
			metric.HistogramOptions{
				Metadata: annotateMetricTemplateWithWorkClass(wc, waitDuration),
				Duration: base.DefaultHistogramWindowInterval(),
				Buckets:  metric.IOLatencyBuckets,
				Mode:     metric.HistogramModePrometheus,
			},
		)
		m.TotalStreamCount[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClass(wc, totalStreamCount),
			func() int64 {
				c.mu.Lock()
				defer c.mu.Unlock()
				return int64(len(c.mu.buckets))
			},
		)

		var blockedStreamLogger = log.Every(30 * time.Second)
		var buf strings.Builder
		m.BlockedStreamCount[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClass(wc, blockedStreamCount),
			func() int64 {
				shouldLog := blockedStreamLogger.ShouldLog()

				count := int64(0)
				c.mu.Lock()
				defer c.mu.Unlock()

				for s, wbc := range c.mu.buckets {
					if wbc.tokens[wc] <= 0 {
						count += 1

						if shouldLog {
							if count > 10 {
								continue // cap output to 10 blocked streams
							}
							if count == 1 {
								buf.Reset()
							}
							if count > 1 {
								buf.WriteString(", ")
							}
							buf.WriteString(s.String())
						}
					}
				}
				if shouldLog && count > 0 {
					log.Warningf(context.Background(), "%d blocked %s replication stream(s): %s", count, wc, buf.String())
				}
				return count
			},
		)
	}
	return m
}

func (m *metrics) onWaiting(class admissionpb.WorkClass) {
	m.RequestsWaiting[class].Inc(1)
}

func (m *metrics) onAdmitted(class admissionpb.WorkClass, dur time.Duration) {
	m.RequestsAdmitted[class].Inc(1)
	m.RequestsWaiting[class].Dec(1)
	m.WaitDuration[class].RecordValue(dur.Nanoseconds())
}

func (m *metrics) onBypassed(class admissionpb.WorkClass, dur time.Duration) {
	m.RequestsBypassed[class].Inc(1)
	m.RequestsWaiting[class].Dec(1)
	m.WaitDuration[class].RecordValue(dur.Nanoseconds())
}

func (m *metrics) onErrored(class admissionpb.WorkClass, dur time.Duration) {
	m.RequestsErrored[class].Inc(1)
	m.RequestsWaiting[class].Dec(1)
	m.WaitDuration[class].RecordValue(dur.Nanoseconds())
}

func (m *metrics) onTokenAdjustment(adjustment tokensPerWorkClass) {
	for class, delta := range adjustment {
		if delta < 0 {
			m.FlowTokensDeducted[class].Inc(-int64(delta))
		} else if delta > 0 {
			m.FlowTokensReturned[class].Inc(int64(delta))
		}
	}
}

func (m *metrics) onUnaccounted(unaccounted tokensPerWorkClass) {
	for class, delta := range unaccounted {
		m.FlowTokensUnaccounted[class].Inc(int64(delta))
	}
}

// MetricStruct implements the metric.Struct interface.
func (m *metrics) MetricStruct() {}
