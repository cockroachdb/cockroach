// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// Aliases to make the code below slightly easier to read.
const regular, elastic = admissionpb.RegularWorkClass, admissionpb.ElasticWorkClass

var (
	// TokenCounter metrics.
	flowTokensAvailable = metric.Metadata{
		Name:        "kvflowcontrol.tokens.%s.%s.available",
		Help:        "Flow %s tokens available for %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	flowTokensDeducted = metric.Metadata{
		Name:        "kvflowcontrol.tokens.%s.%s.deducted",
		Help:        "Flow %s tokens deducted by %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	flowTokensReturned = metric.Metadata{
		Name:        "kvflowcontrol.tokens.%s.%s.returned",
		Help:        "Flow %s tokens returned by %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	flowTokensUnaccounted = metric.Metadata{
		Name:        "kvflowcontrol.tokens.%s.%s.unaccounted",
		Help:        "Flow %s tokens returned by %s requests that were unaccounted for, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	flowTokensDisconnectReturn = metric.Metadata{
		Name:        "kvflowcontrol.tokens.%s.%s.returned.disconnect",
		Help:        "Flow %s tokens returned early by %s due disconnects, across all replication stream, this is a subset of returned tokens",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	// SendQueue TokenCounter metrics.
	flowTokensSendQueuePreventionDeduct = metric.Metadata{
		Name:        "kvflowcontrol.tokens.send.%s.deducted.prevent_send_queue",
		Help:        "Flow send tokens deducted by %s requests, across all replication streams to prevent forming a send queue",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	flowTokensSendQueueForceFlushDeduct = metric.Metadata{
		Name:        "kvflowcontrol.tokens.send.elastic.deducted.force_flush_send_queue",
		Help:        "Flow send tokens deducted by elastic requests, across all replication streams due to force flushing the stream's send queue",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	// TokenStream metrics.
	totalStreamCount = metric.Metadata{
		Name:        "kvflowcontrol.streams.%s.%s.total_count",
		Help:        "Total number of %s replication streams for %s requests",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	blockedStreamCount = metric.Metadata{
		Name:        "kvflowcontrol.streams.%s.%s.blocked_count",
		Help:        "Number of %s replication streams with no flow tokens available for %s requests",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}

	// WaitForEval metrics.
	requestsWaiting = metric.Metadata{
		Name:        "kvflowcontrol.eval_wait.%s.requests.waiting",
		Help:        "Number of %s requests waiting for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	requestsAdmitted = metric.Metadata{
		Name:        "kvflowcontrol.eval_wait.%s.requests.admitted",
		Help:        "Number of %s requests admitted by the flow controller",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	requestsErrored = metric.Metadata{
		Name:        "kvflowcontrol.eval_wait.%s.requests.errored",
		Help:        "Number of %s requests that errored out while waiting for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	requestsBypassed = metric.Metadata{
		Name: "kvflowcontrol.eval_wait.%s.requests.bypassed",
		Help: "Number of waiting %s requests that bypassed the flow " +
			"controller due the evaluating replica not being the leader",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	waitDuration = metric.Metadata{
		Name:        "kvflowcontrol.eval_wait.%s.duration",
		Help:        "Latency histogram for time %s requests spent waiting for flow tokens to evaluate",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// RangeController metrics.
	rangeFlowControllerCount = metric.Metadata{
		Name:        "kvflowcontrol.range_controller.count",
		Help:        "Gauge of range flow controllers currently open, this should align with the number of leaders",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}

	// SendQueue metrics.
	sendQueueBytes = metric.Metadata{
		Name:        "kvflowcontrol.send_queue.bytes",
		Help:        "Byte size of all raft entries queued for sending to followers, waiting on available elastic send tokens",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	sendQueueCount = metric.Metadata{
		Name:        "kvflowcontrol.send_queue.count",
		Help:        "Count of all raft entries queued for sending to followers, waiting on available elastic send tokens",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	sendQueueForceFlushScheduledCount = metric.Metadata{
		Name:        "kvflowcontrol.send_queue.scheduled.force_flush",
		Help:        "Gauge of replication streams scheduled to force flush their send queue",
		Measurement: "Scheduled force flushes",
		Unit:        metric.Unit_COUNT,
	}
	sendQueueDeductedForSchedulerBytes = metric.Metadata{
		Name:        "kvflowcontrol.send_queue.scheduled.deducted_bytes",
		Help:        "Gauge of elastic send token bytes already deducted by replication streams waiting on the scheduler",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	sendQueuePreventionCount = metric.Metadata{
		Name:        "kvflowcontrol.send_queue.prevent.count",
		Help:        "Counter of replication streams that were prevented from forming a send queue",
		Measurement: "Preventions",
		Unit:        metric.Unit_COUNT,
	}
)

// annotateMetricTemplateWithWorkClass uses the given metric template to build
// one suitable for the specific token type and work class.
func annotateMetricTemplateWithWorkClassAndType(
	wc admissionpb.WorkClass, tmpl metric.Metadata, t TokenType,
) metric.Metadata {
	rv := tmpl
	rv.Name = fmt.Sprintf(tmpl.Name, t, wc)
	rv.Help = fmt.Sprintf(tmpl.Help, t, wc)
	return rv
}

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

type TokenMetrics struct {
	CounterMetrics [NumTokenTypes]*TokenCounterMetrics
	StreamMetrics  [NumTokenTypes]*TokenStreamMetrics
}

var _ metric.Struct = &TokenMetrics{}

// TokenMetrics implements the metric.Struct interface.
func (m *TokenMetrics) MetricStruct() {}

func NewTokenMetrics() *TokenMetrics {
	m := &TokenMetrics{}
	for _, typ := range []TokenType{
		EvalToken,
		SendToken,
	} {
		m.CounterMetrics[typ] = newTokenCounterMetrics(typ)
		m.StreamMetrics[typ] = newTokenStreamMetrics(typ)
	}
	return m
}

// TestingClear is used in tests to reset the metrics.
func (m *TokenMetrics) TestingClear() {
	// NB: we only clear the counter metrics, as the stream metrics are gauges.
	for _, typ := range []TokenType{
		EvalToken,
		SendToken,
	} {
		for _, wc := range []admissionpb.WorkClass{
			admissionpb.RegularWorkClass,
			admissionpb.ElasticWorkClass,
		} {
			m.CounterMetrics[typ].Deducted[wc].Clear()
			m.CounterMetrics[typ].Returned[wc].Clear()
			m.CounterMetrics[typ].Unaccounted[wc].Clear()
			m.CounterMetrics[typ].Disconnected[wc].Clear()
			if typ == SendToken {
				m.CounterMetrics[typ].SendQueue[0].ForceFlushDeducted.Clear()
				for _, wc := range []admissionpb.WorkClass{
					admissionpb.RegularWorkClass,
					admissionpb.ElasticWorkClass,
				} {
					m.CounterMetrics[typ].SendQueue[0].PreventionDeducted[wc].Clear()
				}
			}
		}
	}
}

type TokenCounterMetrics struct {
	Deducted     [admissionpb.NumWorkClasses]*metric.Counter
	Returned     [admissionpb.NumWorkClasses]*metric.Counter
	Unaccounted  [admissionpb.NumWorkClasses]*metric.Counter
	Disconnected [admissionpb.NumWorkClasses]*metric.Counter
	// SendQueue is nil for the eval flow control metric type as it pertains only
	// to the send queue, using send tokens. The metric registry will ignore nil
	// values in a array but not a struct, so wrap the struct in an array to
	// avoid nil pointer panics.
	SendQueue [1]*SendQueueTokenCounterMetrics
}

var _ metric.Struct = &TokenCounterMetrics{}

// TokenCounterMetrics implements the metric.Struct interface.
func (m *TokenCounterMetrics) MetricStruct() {}

func newTokenCounterMetrics(t TokenType) *TokenCounterMetrics {
	m := &TokenCounterMetrics{}
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.Deducted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensDeducted, t),
		)
		m.Returned[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensReturned, t),
		)
		m.Unaccounted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensUnaccounted, t),
		)
		m.Disconnected[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensDisconnectReturn, t),
		)
	}
	if t == SendToken {
		// SendQueueTokenMetrics is only used for the send flow control metric type.
		m.SendQueue[0] = newSendQueueTokenMetrics()
	}
	return m
}

func (m *TokenCounterMetrics) onTokenAdjustment(
	adjustment tokensPerWorkClass, flag TokenAdjustFlag,
) {
	if adjustment.regular < 0 {
		m.Deducted[regular].Inc(-int64(adjustment.regular))
		switch flag {
		case AdjNormal:
			// Nothing to do.
		case AdjDisconnect:
			panic(errors.AssertionFailedf("unexpected disconnect deducting tokens, they should be returned"))
		case AdjForceFlush:
			panic(errors.AssertionFailedf("unexpected force flush deducting regular tokens, only elastic tokens should be deducted"))
		case AdjPreventSendQueue:
			m.SendQueue[0].PreventionDeducted[regular].Inc(-int64(adjustment.regular))
		default:
			panic(errors.AssertionFailedf("unexpected flag %v", flag))
		}
	} else if adjustment.regular > 0 {
		m.Returned[regular].Inc(int64(adjustment.regular))
		switch flag {
		case AdjNormal:
			// Nothing to do.
		case AdjDisconnect:
			m.Disconnected[regular].Inc(int64(adjustment.regular))
		case AdjForceFlush:
			panic(errors.AssertionFailedf("unexpected force flush returning tokens, they should be deducted"))
		case AdjPreventSendQueue:
			panic(errors.AssertionFailedf("unexpected send queue prevention returning tokens, they should be deducted"))
		default:
			panic(errors.AssertionFailedf("unexpected flag %v", flag))
		}
	}
	if adjustment.elastic < 0 {
		m.Deducted[elastic].Inc(-int64(adjustment.elastic))
		switch flag {
		case AdjNormal:
			// Nothing to do.
		case AdjDisconnect:
			panic(errors.AssertionFailedf("unexpected disconnect deducting tokens, they should be returned"))
		case AdjForceFlush:
			m.SendQueue[0].ForceFlushDeducted.Inc(-int64(adjustment.elastic))
		case AdjPreventSendQueue:
			m.SendQueue[0].PreventionDeducted[elastic].Inc(-int64(adjustment.elastic))
		default:
			panic(errors.AssertionFailedf("unexpected flag %v", flag))
		}
	} else if adjustment.elastic > 0 {
		m.Returned[elastic].Inc(int64(adjustment.elastic))
		switch flag {
		case AdjNormal:
			// Nothing to do.
		case AdjDisconnect:
			m.Disconnected[elastic].Inc(int64(adjustment.elastic))
		case AdjForceFlush:
			panic(errors.AssertionFailedf("unexpected force flush returning tokens, they should be deducted"))
		case AdjPreventSendQueue:
			panic(errors.AssertionFailedf("unexpected send queue prevention returning tokens, they should be deducted"))
		default:
			panic(errors.AssertionFailedf("unexpected flag %v", flag))
		}
	}
}

func (m *TokenCounterMetrics) onUnaccounted(unaccounted tokensPerWorkClass) {
	m.Unaccounted[regular].Inc(int64(unaccounted.regular))
	m.Unaccounted[elastic].Inc(int64(unaccounted.elastic))
}

type SendQueueTokenCounterMetrics struct {
	ForceFlushDeducted *metric.Counter
	PreventionDeducted [admissionpb.NumWorkClasses]*metric.Counter
}

var _ metric.Struct = &SendQueueTokenCounterMetrics{}

// SendQueueTokenCounterMetrics implements the metric.Struct interface.
func (m *SendQueueTokenCounterMetrics) MetricStruct() {}

func newSendQueueTokenMetrics() *SendQueueTokenCounterMetrics {
	m := &SendQueueTokenCounterMetrics{}
	m.ForceFlushDeducted = metric.NewCounter(flowTokensSendQueueForceFlushDeduct)
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.PreventionDeducted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, flowTokensSendQueuePreventionDeduct),
		)
	}
	return m
}

type TokenStreamMetrics struct {
	Count           [admissionpb.NumWorkClasses]*metric.Gauge
	BlockedCount    [admissionpb.NumWorkClasses]*metric.Gauge
	TokensAvailable [admissionpb.NumWorkClasses]*metric.Gauge
}

var _ metric.Struct = &TokenStreamMetrics{}

// TokenCounterMetrics implements the metric.Struct interface.
func (m *TokenStreamMetrics) MetricStruct() {}

func newTokenStreamMetrics(t TokenType) *TokenStreamMetrics {
	m := &TokenStreamMetrics{}
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.Count[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, totalStreamCount, t),
		)
		m.BlockedCount[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, blockedStreamCount, t),
		)
		m.TokensAvailable[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensAvailable, t),
		)
	}
	return m
}

type EvalWaitMetrics struct {
	Waiting  [admissionpb.NumWorkClasses]*metric.Gauge
	Admitted [admissionpb.NumWorkClasses]*metric.Counter
	Errored  [admissionpb.NumWorkClasses]*metric.Counter
	Bypassed [admissionpb.NumWorkClasses]*metric.Counter
	Duration [admissionpb.NumWorkClasses]metric.IHistogram
}

var _ metric.Struct = &EvalWaitMetrics{}

// EvalWaitMetrics implements the metric.Struct interface.
func (m *EvalWaitMetrics) MetricStruct() {}

func NewEvalWaitMetrics() *EvalWaitMetrics {
	m := &EvalWaitMetrics{}
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.Waiting[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClass(wc, requestsWaiting),
		)
		m.Admitted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, requestsAdmitted),
		)
		m.Errored[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, requestsErrored),
		)
		m.Bypassed[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, requestsBypassed),
		)
		m.Duration[wc] = metric.NewHistogram(
			metric.HistogramOptions{
				Metadata:     annotateMetricTemplateWithWorkClass(wc, waitDuration),
				Duration:     base.DefaultHistogramWindowInterval(),
				BucketConfig: metric.IOLatencyBuckets,
				Mode:         metric.HistogramModePrometheus,
			},
		)
	}
	return m
}

func (e *EvalWaitMetrics) OnWaiting(wc admissionpb.WorkClass) {
	e.Waiting[wc].Inc(1)
}

func (e *EvalWaitMetrics) OnAdmitted(wc admissionpb.WorkClass, dur time.Duration) {
	e.Admitted[wc].Inc(1)
	e.Waiting[wc].Dec(1)
	e.Duration[wc].RecordValue(dur.Nanoseconds())
}

func (e *EvalWaitMetrics) OnBypassed(wc admissionpb.WorkClass, dur time.Duration) {
	e.Bypassed[wc].Inc(1)
	e.Waiting[wc].Dec(1)
	e.Duration[wc].RecordValue(dur.Nanoseconds())
}

func (e *EvalWaitMetrics) OnErrored(wc admissionpb.WorkClass, dur time.Duration) {
	e.Errored[wc].Inc(1)
	e.Waiting[wc].Dec(1)
	e.Duration[wc].RecordValue(dur.Nanoseconds())
}

type RangeControllerMetrics struct {
	Count     *metric.Gauge
	SendQueue *SendQueueMetrics
}

var _ metric.Struct = &RangeControllerMetrics{}

// RangeControllerMetrics implements the metric.Struct interface.
func (m *RangeControllerMetrics) MetricStruct() {}

func NewRangeControllerMetrics() *RangeControllerMetrics {
	return &RangeControllerMetrics{
		Count:     metric.NewGauge(rangeFlowControllerCount),
		SendQueue: NewSendQueueMetrics(),
	}
}

var _ metric.Struct = &SendQueueMetrics{}

// SendQueueMetrics implements the metric.Struct interface.
func (m *SendQueueMetrics) MetricStruct() {}

type SendQueueMetrics struct {
	// SizeCount and SizeBytes are updated periodically, by NodeMetrics.
	SizeCount *metric.Gauge
	SizeBytes *metric.Gauge
	// The below metrics are maintained incrementally by each RangeController.
	ForceFlushedScheduledCount *metric.Gauge
	DeductedForSchedulerBytes  *metric.Gauge
	PreventionCount            *metric.Counter
}

func NewSendQueueMetrics() *SendQueueMetrics {
	return &SendQueueMetrics{
		SizeCount:                  metric.NewGauge(sendQueueCount),
		SizeBytes:                  metric.NewGauge(sendQueueBytes),
		ForceFlushedScheduledCount: metric.NewGauge(sendQueueForceFlushScheduledCount),
		DeductedForSchedulerBytes:  metric.NewGauge(sendQueueDeductedForSchedulerBytes),
		PreventionCount:            metric.NewCounter(sendQueuePreventionCount),
	}
}
