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

	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/redact"
)

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

type tokenMetrics struct {
	counterMetrics [numFlowControlMetricTypes]*tokenCounterMetrics
	streamMetrics  [numFlowControlMetricTypes]*tokenStreamMetrics
}

func newTokenMetrics() *tokenMetrics {
	m := &tokenMetrics{}
	for _, typ := range []flowControlMetricType{
		flowControlEvalMetricType,
		flowControlSendMetricType,
	} {
		m.counterMetrics[typ] = newTokenCounterMetrics(typ)
		m.streamMetrics[typ] = newTokenStreamMetrics(typ)
	}
	return m
}

type tokenCounterMetrics struct {
	deducted    [admissionpb.NumWorkClasses]*metric.Counter
	returned    [admissionpb.NumWorkClasses]*metric.Counter
	unaccounted [admissionpb.NumWorkClasses]*metric.Counter
}

func newTokenCounterMetrics(t flowControlMetricType) *tokenCounterMetrics {
	m := &tokenCounterMetrics{}
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.deducted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensDeducted, t),
		)
		m.returned[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensReturned, t),
		)
		m.unaccounted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensUnaccounted, t),
		)
	}
	return m
}

func (m *tokenCounterMetrics) onTokenAdjustment(adjustment tokensPerWorkClass) {
	if adjustment.regular < 0 {
		m.deducted[regular].Inc(-int64(adjustment.regular))
	} else if adjustment.regular > 0 {
		m.returned[regular].Inc(int64(adjustment.regular))
	}
	if adjustment.elastic < 0 {
		m.deducted[elastic].Inc(-int64(adjustment.elastic))
	} else if adjustment.elastic > 0 {
		m.returned[elastic].Inc(int64(adjustment.elastic))
	}
}

func (m *tokenCounterMetrics) onUnaccounted(unaccounted tokensPerWorkClass) {
	m.unaccounted[regular].Inc(int64(unaccounted.regular))
	m.unaccounted[elastic].Inc(int64(unaccounted.elastic))
}

type tokenStreamMetrics struct {
	count           [admissionpb.NumWorkClasses]*metric.Gauge
	blockedCount    [admissionpb.NumWorkClasses]*metric.Gauge
	tokensAvailable [admissionpb.NumWorkClasses]*metric.Gauge
}

func newTokenStreamMetrics(t flowControlMetricType) *tokenStreamMetrics {
	m := &tokenStreamMetrics{}
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.count[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, totalStreamCount, t),
		)
		m.blockedCount[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, blockedStreamCount, t),
		)
		m.tokensAvailable[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensAvailable, t),
		)
	}
	return m
}
