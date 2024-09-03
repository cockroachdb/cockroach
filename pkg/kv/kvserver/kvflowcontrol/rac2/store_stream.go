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
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

// StreamTokenCounterProvider is the interface for retrieving token counters
// for a given stream.
//
// TODO(kvoli): Add stream deletion upon decommissioning a store.
type StreamTokenCounterProvider struct {
	settings                   *cluster.Settings
	clock                      *hlc.Clock
	tokenMetrics               *tokenMetrics
	sendLogger, evalLogger     *blockedStreamLogger
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
		sendLogger:   newBlockedStreamLogger(flowControlSendMetricType),
		evalLogger:   newBlockedStreamLogger(flowControlEvalMetricType),
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

// UpdateMetricGauges updates the gauge token metrics and logs blocked streams.
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
			regularTokens := t.tokens(admissionpb.RegularWorkClass)
			elasticTokens := t.tokens(admissionpb.ElasticWorkClass)
			count[metricType][regular]++
			count[metricType][elastic]++
			tokensAvailable[metricType][regular] += int64(regularTokens)
			tokensAvailable[metricType][elastic] += int64(elasticTokens)

			if regularTokens <= 0 {
				blockedCount[metricType][regular]++
			}
			if elasticTokens <= 0 {
				blockedCount[metricType][elastic]++
			}

			// Pass the stream information to the logger. If either regular or
			// elastic stats indicate that the stream was blocked in the last
			// metrics interval and the logger is ready to log, then the stream
			// will be logged.
			switch metricType {
			case flowControlEvalMetricType:
				p.evalLogger.observeStream(stream, now, regularTokens, elasticTokens, t.GetAndResetStats)
			case flowControlSendMetricType:
				p.sendLogger.observeStream(stream, now, regularTokens, elasticTokens, t.GetAndResetStats)
			default:
				panic(fmt.Sprintf("unexpected metric type: %v", metricType))
			}

			return true
		}
	}

	p.evalCounters.Range(gaugeUpdateFn(flowControlEvalMetricType))
	p.evalLogger.flushLogs()
	p.sendCounters.Range(gaugeUpdateFn(flowControlSendMetricType))
	p.sendLogger.flushLogs()
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

// TODO(kvoli): Consider adjusting these limits and making them configurable.
const (
	// streamStatsCountCap is the maximum number of streams to log verbose stats
	// for. Streams are only logged if they were blocked at some point in the
	// last metrics interval.
	streamStatsCountCap = 20
	// blockedStreamCountCap is the maximum number of streams to log (compactly)
	// as currently blocked.
	blockedStreamCountCap = 100
	// blockedStreamLoggingInterval is the interval at which blocked streams are
	// logged. This interval applies independently to both eval and send streams
	// i.e., we log both eval and send streams at this interval, independent of
	// each other.
	blockedStreamLoggingInterval = 30 * time.Second
)

type blockedStreamLogger struct {
	metricType flowControlMetricType
	limiter    log.EveryN
	// blockedCount is the total number of unique streams blocked in the last
	// interval, regardless of the work class e.g., if 5 streams exist and all
	// are blocked for both elastic and regular work classes, the counts would
	// be:
	//   blockedRegularCount=5
	//   blockedElasticCount=5
	//   blockedCount=5
	blockedCount        int
	blockedElasticCount int
	blockedRegularCount int
	elaBuf, regBuf      strings.Builder
}

func newBlockedStreamLogger(metricType flowControlMetricType) *blockedStreamLogger {
	return &blockedStreamLogger{
		metricType: metricType,
		limiter:    log.Every(blockedStreamLoggingInterval),
	}
}

func (b *blockedStreamLogger) flushLogs() {
	if b.limiter.ShouldLog() {
		if b.blockedRegularCount > 0 {
			log.Warningf(context.Background(), "%d blocked %s regular replication stream(s): %s",
				b.blockedRegularCount, b.metricType, redact.SafeString(b.regBuf.String()))
		}
		if b.blockedElasticCount > 0 {
			log.Warningf(context.Background(), "%d blocked %s elastic replication stream(s): %s",
				b.blockedElasticCount, b.metricType, redact.SafeString(b.elaBuf.String()))
		}
	}
	// NB: The buffers get reset on next use, see logBlockedStream() below.
	b.blockedCount = 0
	b.blockedRegularCount = 0
	b.blockedElasticCount = 0
}

func (b *blockedStreamLogger) observeStream(
	stream kvflowcontrol.Stream,
	now time.Time,
	regularTokens, elasticTokens kvflowcontrol.Tokens,
	deltaStatFn func(time.Time) (regularStats, elasticStats deltaStats),
) {
	if !b.limiter.ShouldLog() {
		// There is nothing to do if we are not logging.
		return
	}
	// Log stats, which reflect both elastic and regular at the interval defined
	// by blockedStreamLoggingInteval. If a high-enough log verbosity is
	// specified, shouldLogBacked will always be true, but since this method
	// executes at the frequency of scraping the metric, we will still log at a
	// reasonable rate.
	logBlockedStream := func(stream kvflowcontrol.Stream, blockedCount int, buf *strings.Builder) {
		if blockedCount == 1 {
			buf.Reset()
			buf.WriteString(stream.String())
		} else if blockedCount <= blockedStreamCountCap {
			buf.WriteString(", ")
			buf.WriteString(stream.String())
		} else if blockedCount == blockedStreamCountCap+1 {
			buf.WriteString(" omitted some due to overflow")
		}
	}

	if regularTokens <= 0 {
		b.blockedRegularCount++
		logBlockedStream(stream, b.blockedRegularCount, &b.regBuf)
	}
	if elasticTokens <= 0 {
		b.blockedElasticCount++
		logBlockedStream(stream, b.blockedElasticCount, &b.elaBuf)
	}
	regularStats, elasticStats := deltaStatFn(now)
	if regularStats.noTokenDuration == 0 && elasticStats.noTokenDuration == 0 {
		return
	}

	b.blockedCount++
	if b.blockedCount <= streamStatsCountCap {
		var bb strings.Builder
		fmt.Fprintf(&bb, "%v stream %s was blocked: durations:", b.metricType, stream.String())
		if regularStats.noTokenDuration > 0 {
			fmt.Fprintf(&bb, " regular %s", regularStats.noTokenDuration.String())
		}
		if elasticStats.noTokenDuration > 0 {
			fmt.Fprintf(&bb, " elastic %s", elasticStats.noTokenDuration.String())
		}
		regularDelta := regularStats.tokensReturned - regularStats.tokensDeducted
		elasticDelta := elasticStats.tokensReturned - elasticStats.tokensDeducted
		fmt.Fprintf(&bb, " tokens delta: regular %s (%s - %s) elastic %s (%s - %s)",
			pprintTokens(regularDelta),
			pprintTokens(regularStats.tokensReturned),
			pprintTokens(regularStats.tokensDeducted),
			pprintTokens(elasticDelta),
			pprintTokens(elasticStats.tokensReturned),
			pprintTokens(elasticStats.tokensDeducted))
		log.Infof(context.Background(), "%s", redact.SafeString(bb.String()))
	} else if b.blockedCount == streamStatsCountCap+1 {
		log.Infof(context.Background(), "skipped logging some streams that were blocked")
	}
}

func pprintTokens(t kvflowcontrol.Tokens) string {
	if t < 0 {
		return fmt.Sprintf("-%s", humanize.IBytes(uint64(-t)))
	}
	return humanize.IBytes(uint64(t))
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
