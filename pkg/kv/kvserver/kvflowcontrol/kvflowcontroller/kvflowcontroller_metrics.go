// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowcontroller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
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
	ElasticFlowTokensDeducted    *metric.Counter
	ElasticFlowTokensReturned    *metric.Counter
	ElasticFlowTokensUnaccounted *metric.Counter
	RegularFlowTokensDeducted    *metric.Counter
	RegularFlowTokensReturned    *metric.Counter
	RegularFlowTokensUnaccounted *metric.Counter
	FlowTokensAvailable          [admissionpb.NumWorkClasses]*metric.Gauge
	RequestsWaiting              [admissionpb.NumWorkClasses]*metric.Gauge
	RequestsAdmitted             [admissionpb.NumWorkClasses]*metric.Counter
	RequestsErrored              [admissionpb.NumWorkClasses]*metric.Counter
	RequestsBypassed             [admissionpb.NumWorkClasses]*metric.Counter
	WaitDuration                 [admissionpb.NumWorkClasses]metric.IHistogram
	TotalStreamCount             [admissionpb.NumWorkClasses]*metric.Gauge
	BlockedStreamCount           [admissionpb.NumWorkClasses]*metric.Gauge
}

var _ metric.Struct = &metrics{}

func newMetrics(c *Controller) *metrics {
	m := &metrics{}
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.FlowTokensAvailable[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClass(wc, flowTokensAvailable),
			func() int64 {
				sum := int64(0)
				c.mu.buckets.Range(func(_ kvflowcontrol.Stream, b *bucket) bool {
					sum += int64(b.tokens(wc))
					return true
				})
				return sum
			},
		)
		if wc == regular {
			m.RegularFlowTokensDeducted = metric.NewCounter(
				annotateMetricTemplateWithWorkClass(wc, flowTokensDeducted),
			)
			m.RegularFlowTokensReturned = metric.NewCounter(
				annotateMetricTemplateWithWorkClass(wc, flowTokensReturned),
			)
			m.RegularFlowTokensUnaccounted = metric.NewCounter(
				annotateMetricTemplateWithWorkClass(wc, flowTokensUnaccounted),
			)
		} else {
			m.ElasticFlowTokensDeducted = metric.NewCounter(
				annotateMetricTemplateWithWorkClass(wc, flowTokensDeducted),
			)
			m.ElasticFlowTokensReturned = metric.NewCounter(
				annotateMetricTemplateWithWorkClass(wc, flowTokensReturned),
			)
			m.ElasticFlowTokensUnaccounted = metric.NewCounter(
				annotateMetricTemplateWithWorkClass(wc, flowTokensUnaccounted),
			)
		}
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
				Metadata:     annotateMetricTemplateWithWorkClass(wc, waitDuration),
				Duration:     base.DefaultHistogramWindowInterval(),
				BucketConfig: metric.IOLatencyBuckets,
				Mode:         metric.HistogramModePrometheus,
			},
		)
		m.TotalStreamCount[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClass(wc, totalStreamCount),
			func() int64 {
				c.mu.Lock()
				defer c.mu.Unlock()
				return int64(c.mu.bucketCount)
			},
		)

		// blockedStreamLogger controls periodic logging of blocked streams in
		// WorkClass wc.
		var blockedStreamLogger = log.Every(30 * time.Second)
		var buf strings.Builder
		m.BlockedStreamCount[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClass(wc, blockedStreamCount),
			func() int64 {
				shouldLogBlocked := blockedStreamLogger.ShouldLog()
				// count is the metric value.
				count := int64(0)

				streamStatsCount := 0
				// TODO(sumeer): this cap is not ideal. Consider dynamically reducing
				// the logging frequency to maintain a mean of 400 log entries/10min.
				const streamStatsCountCap = 20
				c.mu.buckets.Range(func(stream kvflowcontrol.Stream, b *bucket) bool {
					if b.tokens(wc) <= 0 {
						count++

						if shouldLogBlocked {
							// TODO(sumeer): this cap is not ideal.
							const blockedStreamCountCap = 100
							if count == 1 {
								buf.Reset()
								buf.WriteString(stream.String())
							} else if count <= blockedStreamCountCap {
								buf.WriteString(", ")
								buf.WriteString(stream.String())
							} else if count == blockedStreamCountCap+1 {
								buf.WriteString(" omitted some due to overflow")
							}
						}
					}
					// Log stats, which reflect both elastic and regular, when handling
					// the elastic metric. The choice of wc == elastic is arbitrary.
					// Every 30s this predicate will evaluate to true, and we will log
					// all the streams (elastic and regular) that experienced some
					// blocking since the last time such logging was done. If a
					// high-enough log verbosity is specified, shouldLogBacked will
					// always be true, but since this method executes at the frequency
					// of scraping the metric, we will still log at a reasonable rate.
					if shouldLogBlocked && wc == elastic {
						// Get and reset stats regardless of whether we will log this
						// stream or not. We want stats to reflect only the last metric
						// interval.
						regularStats, elasticStats := b.getAndResetStats(c.clock.PhysicalTime())
						logStream := false
						if regularStats.noTokenDuration > 0 || elasticStats.noTokenDuration > 0 {
							logStream = true
							streamStatsCount++
						}
						if logStream {
							if streamStatsCount <= streamStatsCountCap {
								var b strings.Builder
								fmt.Fprintf(&b, "stream %s was blocked: durations:", stream.String())
								if regularStats.noTokenDuration > 0 {
									fmt.Fprintf(&b, " regular %s", regularStats.noTokenDuration.String())
								}
								if elasticStats.noTokenDuration > 0 {
									fmt.Fprintf(&b, " elastic %s", elasticStats.noTokenDuration.String())
								}
								fmt.Fprintf(&b, " tokens deducted: regular %s elastic %s",
									humanize.IBytes(uint64(regularStats.tokensDeducted)),
									humanize.IBytes(uint64(elasticStats.tokensDeducted)))
								log.Infof(context.Background(), "%s", redact.SafeString(b.String()))
							} else if streamStatsCount == streamStatsCountCap+1 {
								log.Infof(context.Background(), "skipped logging some streams that were blocked")
							}
						}
					}
					return true
				})
				if shouldLogBlocked && count > 0 {
					log.Warningf(context.Background(), "%d blocked %s replication stream(s): %s",
						count, wc, redact.SafeString(buf.String()))
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
	if adjustment.regular < 0 {
		m.RegularFlowTokensDeducted.Inc(-int64(adjustment.regular))
	} else {
		m.RegularFlowTokensReturned.Inc(int64(adjustment.regular))
	}
	if adjustment.elastic < 0 {
		m.ElasticFlowTokensDeducted.Inc(-int64(adjustment.elastic))
	} else {
		m.ElasticFlowTokensReturned.Inc(int64(adjustment.elastic))
	}
}

func (m *metrics) onUnaccounted(unaccounted tokensPerWorkClass) {
	m.RegularFlowTokensUnaccounted.Inc(int64(unaccounted.regular))
	m.ElasticFlowTokensUnaccounted.Inc(int64(unaccounted.elastic))
}

// MetricStruct implements the metric.Struct interface.
func (m *metrics) MetricStruct() {}
