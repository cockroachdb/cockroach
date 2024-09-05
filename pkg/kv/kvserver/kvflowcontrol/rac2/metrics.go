// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/redact"
)

// TODO(kvoli):
// - hookup the metrics to the registry
// - call onBypassed etc in replica_rac2.Processor

// Aliases to make the code below slightly easier to read.
const regular, elastic = admissionpb.RegularWorkClass, admissionpb.ElasticWorkClass

var (
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
)

// annotateMetricTemplateWithWorkClass uses the given metric template to build
// one suitable for the specific token type and work class.
func annotateMetricTemplateWithWorkClassAndType(
	wc admissionpb.WorkClass, tmpl metric.Metadata, t flowControlMetricType,
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

type flowControlMetricType int

const (
	flowControlEvalMetricType flowControlMetricType = iota
	flowControlSendMetricType
	numFlowControlMetricTypes
)

func (f flowControlMetricType) String() string {
	return redact.StringWithoutMarkers(f)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (f flowControlMetricType) SafeFormat(p redact.SafePrinter, _ rune) {
	switch f {
	case flowControlEvalMetricType:
		p.SafeString("eval")
	case flowControlSendMetricType:
		p.SafeString("send")
	default:
		panic("unknown flowControlMetricType")
	}
}

type TokenMetrics struct {
	CounterMetrics [numFlowControlMetricTypes]*TokenCounterMetrics
	StreamMetrics  [numFlowControlMetricTypes]*TokenStreamMetrics
}

var _ metric.Struct = &TokenMetrics{}

// TokenMetrics implements the metric.Struct interface.
func (m *TokenMetrics) MetricStruct() {}

func NewTokenMetrics() *TokenMetrics {
	m := &TokenMetrics{}
	for _, typ := range []flowControlMetricType{
		flowControlEvalMetricType,
		flowControlSendMetricType,
	} {
		m.CounterMetrics[typ] = newTokenCounterMetrics(typ)
		m.StreamMetrics[typ] = newTokenStreamMetrics(typ)
	}
	return m
}

type TokenCounterMetrics struct {
	Deducted    [admissionpb.NumWorkClasses]*metric.Counter
	Returned    [admissionpb.NumWorkClasses]*metric.Counter
	Unaccounted [admissionpb.NumWorkClasses]*metric.Counter
}

var _ metric.Struct = &TokenCounterMetrics{}

// TokenCounterMetrics implements the metric.Struct interface.
func (m *TokenCounterMetrics) MetricStruct() {}

func newTokenCounterMetrics(t flowControlMetricType) *TokenCounterMetrics {
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
	}
	return m
}

func (m *TokenCounterMetrics) onTokenAdjustment(adjustment tokensPerWorkClass) {
	if adjustment.regular < 0 {
		m.Deducted[regular].Inc(-int64(adjustment.regular))
	} else if adjustment.regular > 0 {
		m.Returned[regular].Inc(int64(adjustment.regular))
	}
	if adjustment.elastic < 0 {
		m.Deducted[elastic].Inc(-int64(adjustment.elastic))
	} else if adjustment.elastic > 0 {
		m.Returned[elastic].Inc(int64(adjustment.elastic))
	}
}

func (m *TokenCounterMetrics) onUnaccounted(unaccounted tokensPerWorkClass) {
	m.Unaccounted[regular].Inc(int64(unaccounted.regular))
	m.Unaccounted[elastic].Inc(int64(unaccounted.elastic))
}

type TokenStreamMetrics struct {
	Count           [admissionpb.NumWorkClasses]*metric.Gauge
	BlockedCount    [admissionpb.NumWorkClasses]*metric.Gauge
	TokensAvailable [admissionpb.NumWorkClasses]*metric.Gauge
}

var _ metric.Struct = &TokenStreamMetrics{}

// TokenCounterMetrics implements the metric.Struct interface.
func (m *TokenStreamMetrics) MetricStruct() {}

func newTokenStreamMetrics(t flowControlMetricType) *TokenStreamMetrics {
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
