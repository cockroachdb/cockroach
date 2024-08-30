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
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StreamTokenCounterProvider is the interface for retrieving token counters
// for a given stream.
//
// TODO(kvoli): Add stream deletion upon decommissioning a store.
type StreamTokenCounterProvider struct {
	settings                   *cluster.Settings
	clock                      *hlc.Clock
	tokenMetrics               *tokenMetrics
	sendCounters, evalCounters syncutil.Map[kvflowcontrol.Stream, tokenCounter]
}

// NewStreamTokenCounterProvider creates a new StreamTokenCounterProvider.
func NewStreamTokenCounterProvider(
	settings *cluster.Settings, clock *hlc.Clock,
) *StreamTokenCounterProvider {
	return &StreamTokenCounterProvider{
		settings:     settings,
		clock:        clock,
		tokenMetrics: newTokenMetrics(),
	}
}

// Eval returns the evaluation token counter for the given stream.
func (p *StreamTokenCounterProvider) Eval(stream kvflowcontrol.Stream) *tokenCounter {
	if t, ok := p.evalCounters.Load(stream); ok {
		return t
	}
	t, _ := p.evalCounters.LoadOrStore(stream, newTokenCounter(
		p.settings, p.clock, p.tokenMetrics.counterMetrics[flowControlEvalMetricType]))
	return t
}

// Send returns the send token counter for the given stream.
func (p *StreamTokenCounterProvider) Send(stream kvflowcontrol.Stream) *tokenCounter {
	if t, ok := p.sendCounters.Load(stream); ok {
		return t
	}
	t, _ := p.sendCounters.LoadOrStore(stream, newTokenCounter(
		p.settings, p.clock, p.tokenMetrics.counterMetrics[flowControlSendMetricType]))
	return t
}

// UpdateMetricGauges updates the gauge token metrics.
func (p *StreamTokenCounterProvider) UpdateMetricGauges() {
	var (
		count           [numFlowControlMetricTypes][admissionpb.NumWorkClasses]int64
		blockedCount    [numFlowControlMetricTypes][admissionpb.NumWorkClasses]int64
		tokensAvailable [numFlowControlMetricTypes][admissionpb.NumWorkClasses]int64
	)
	now := p.clock.PhysicalTime()

	// First aggregate the metrics across all streams, by (eval|send) types and
	// (regular|elastic) work classes, then using the aggregate update the
	// gauges.
	gaugeUpdateFn := func(metricType flowControlMetricType) func(
		kvflowcontrol.Stream, *tokenCounter) bool {
		return func(stream kvflowcontrol.Stream, t *tokenCounter) bool {
			count[metricType][regular]++
			count[metricType][elastic]++
			tokensAvailable[metricType][regular] += int64(t.tokens(regular))
			tokensAvailable[metricType][elastic] += int64(t.tokens(elastic))

			regularStats, elasticStats := t.GetAndResetStats(now)
			if regularStats.noTokenDuration > 0 {
				blockedCount[metricType][regular]++
			}
			if elasticStats.noTokenDuration > 0 {
				blockedCount[metricType][elastic]++
			}
			return true
		}
	}

	p.evalCounters.Range(gaugeUpdateFn(flowControlEvalMetricType))
	p.sendCounters.Range(gaugeUpdateFn(flowControlSendMetricType))
	for _, typ := range []flowControlMetricType{
		flowControlEvalMetricType,
		flowControlSendMetricType,
	} {
		for _, wc := range []admissionpb.WorkClass{
			admissionpb.RegularWorkClass,
			admissionpb.ElasticWorkClass,
		} {
			p.tokenMetrics.streamMetrics[typ].count[wc].Update(count[typ][wc])
			p.tokenMetrics.streamMetrics[typ].blockedCount[wc].Update(blockedCount[typ][wc])
			p.tokenMetrics.streamMetrics[typ].tokensAvailable[wc].Update(tokensAvailable[typ][wc])
		}
	}
}

// SendTokenWatcherHandleID is a unique identifier for a handle that is
// watching for available elastic send tokens on a stream.
type SendTokenWatcherHandleID int64

// SendTokenWatcher is the interface for watching and waiting on available
// elastic send tokens. The watcher registers a notification, which will be
// called when elastic tokens are available for the stream this watcher is
// monitoring. Note only elastic tokens are watched as this is intended to be
// used when a send queue exists.
//
// TODO(kvoli): Consider de-interfacing if not necessary for testing.
type SendTokenWatcher interface {
	// NotifyWhenAvailable queues up for elastic tokens for the given send token
	// counter. When elastic tokens are available, the provided
	// TokenGrantNotification is called. It is the caller's responsibility to
	// call CancelHandle when tokens are no longer needed, or when the caller is
	// done.
	NotifyWhenAvailable(
		*tokenCounter,
		TokenGrantNotification,
	) SendTokenWatcherHandleID
	// CancelHandle cancels the given handle, stopping it from being notified
	// when tokens are available. CancelHandle should be called at most once.
	CancelHandle(SendTokenWatcherHandleID)
}

// TokenGrantNotification is an interface that is called when tokens are
// available.
type TokenGrantNotification interface {
	// Notify is called when tokens are available to be granted.
	Notify(context.Context)
}
