// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/queue"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	tokenMetrics               *TokenMetrics
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
		tokenMetrics: NewTokenMetrics(),
		sendLogger:   newBlockedStreamLogger(SendToken),
		evalLogger:   newBlockedStreamLogger(EvalToken),
	}
}

// Eval returns the evaluation token counter for the given stream.
func (p *StreamTokenCounterProvider) Eval(stream kvflowcontrol.Stream) *tokenCounter {
	if t, ok := p.evalCounters.Load(stream); ok {
		return t
	}
	t, _ := p.evalCounters.LoadOrStore(stream, newTokenCounter(
		p.settings, p.clock, p.tokenMetrics.CounterMetrics[EvalToken], stream, EvalToken))
	return t
}

// Send returns the send token counter for the given stream.
func (p *StreamTokenCounterProvider) Send(stream kvflowcontrol.Stream) *tokenCounter {
	if t, ok := p.sendCounters.Load(stream); ok {
		return t
	}
	t, _ := p.sendCounters.LoadOrStore(stream, newTokenCounter(
		p.settings, p.clock, p.tokenMetrics.CounterMetrics[SendToken], stream, SendToken))
	return t
}

func makeInspectStream(
	stream kvflowcontrol.Stream, evalCounter, sendCounter *tokenCounter,
) kvflowinspectpb.Stream {
	evalTokens := evalCounter.tokensPerWorkClass()
	sendTokens := sendCounter.tokensPerWorkClass()
	return kvflowinspectpb.Stream{
		TenantID:                   stream.TenantID,
		StoreID:                    stream.StoreID,
		AvailableEvalRegularTokens: int64(evalTokens.regular),
		AvailableEvalElasticTokens: int64(evalTokens.elastic),
		AvailableSendRegularTokens: int64(sendTokens.regular),
		AvailableSendElasticTokens: int64(sendTokens.elastic),
	}
}

// InspectStream returns a snapshot of a specific underlying {eval,send} stream
// and its available {regular,elastic} tokens. It's used to power
// /inspectz.
func (p *StreamTokenCounterProvider) InspectStream(
	stream kvflowcontrol.Stream,
) kvflowinspectpb.Stream {
	return makeInspectStream(stream, p.Eval(stream), p.Send(stream))
}

// Inspect returns a snapshot of all underlying (eval|send) streams and their
// available {regular,elastic} tokens. It's used to power /inspectz.
func (p *StreamTokenCounterProvider) Inspect(_ context.Context) []kvflowinspectpb.Stream {
	var streams []kvflowinspectpb.Stream
	p.evalCounters.Range(func(stream kvflowcontrol.Stream, eval *tokenCounter) bool {
		streams = append(streams, makeInspectStream(stream, eval, p.Send(stream)))
		return true
	})
	// Sort the connected streams for determinism, which some tests rely on.
	slices.SortFunc(streams, func(a, b kvflowinspectpb.Stream) int {
		return cmp.Or(
			cmp.Compare(a.TenantID.ToUint64(), b.TenantID.ToUint64()),
			cmp.Compare(a.StoreID, b.StoreID),
		)
	})
	return streams
}

// UpdateMetricGauges updates the gauge token metrics and logs blocked streams.
func (p *StreamTokenCounterProvider) UpdateMetricGauges() {
	var (
		count           [NumTokenTypes][admissionpb.NumWorkClasses]int64
		blockedCount    [NumTokenTypes][admissionpb.NumWorkClasses]int64
		tokensAvailable [NumTokenTypes][admissionpb.NumWorkClasses]int64
	)
	now := p.clock.PhysicalTime()

	// First aggregate the metrics across all streams, by (eval|send) types and
	// (regular|elastic) work classes, then using the aggregate update the
	// gauges.
	gaugeUpdateFn := func(metricType TokenType) func(
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

			return true
		}
	}

	p.evalCounters.Range(gaugeUpdateFn(EvalToken))
	p.sendCounters.Range(gaugeUpdateFn(SendToken))
	for _, typ := range []TokenType{
		EvalToken,
		SendToken,
	} {
		for _, wc := range []admissionpb.WorkClass{
			admissionpb.RegularWorkClass,
			admissionpb.ElasticWorkClass,
		} {
			p.tokenMetrics.StreamMetrics[typ].Count[wc].Update(count[typ][wc])
			p.tokenMetrics.StreamMetrics[typ].BlockedCount[wc].Update(blockedCount[typ][wc])
			p.tokenMetrics.StreamMetrics[typ].TokensAvailable[wc].Update(tokensAvailable[typ][wc])
		}
	}

	// Next, check if any of the blocked stream loggers are ready to log, if so
	// we iterate over every (token|send) stream and observe the stream state.
	// When vmodule=2, the logger is always ready.
	logStreamFn := func(logger *blockedStreamLogger) func(
		stream kvflowcontrol.Stream, t *tokenCounter) bool {
		return func(stream kvflowcontrol.Stream, t *tokenCounter) bool {
			// NB: We reset each stream's stats here. The stat returned will be the
			// delta between the last stream observation and now.
			regularStats, elasticStats := t.GetAndResetStats(now)
			logger.observeStream(stream, now,
				t.tokens(regular), t.tokens(elastic), regularStats, elasticStats)
			return true
		}
	}
	if p.evalLogger.willLog() {
		p.evalCounters.Range(logStreamFn(p.evalLogger))
		p.evalLogger.flushLogs()
	}
	if p.sendLogger.willLog() {
		p.sendCounters.Range(logStreamFn(p.sendLogger))
		p.sendLogger.flushLogs()
	}
}

// Metrics returns metrics tracking the token counters and streams.
func (p *StreamTokenCounterProvider) Metrics() metric.Struct {
	return p.tokenMetrics
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
	metricType TokenType
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

func newBlockedStreamLogger(metricType TokenType) *blockedStreamLogger {
	return &blockedStreamLogger{
		metricType: metricType,
		limiter:    log.Every(blockedStreamLoggingInterval),
	}
}

func (b *blockedStreamLogger) willLog() bool {
	return b.limiter.ShouldLog()
}

func (b *blockedStreamLogger) flushLogs() {
	if b.blockedRegularCount > 0 {
		log.Warningf(context.Background(), "%d blocked %s regular replication stream(s): %s",
			b.blockedRegularCount, b.metricType, redact.SafeString(b.regBuf.String()))
	}
	if b.blockedElasticCount > 0 {
		log.Warningf(context.Background(), "%d blocked %s elastic replication stream(s): %s",
			b.blockedElasticCount, b.metricType, redact.SafeString(b.elaBuf.String()))
	}
	b.elaBuf.Reset()
	b.regBuf.Reset()
	b.blockedCount = 0
	b.blockedRegularCount = 0
	b.blockedElasticCount = 0
}

func (b *blockedStreamLogger) observeStream(
	stream kvflowcontrol.Stream,
	now time.Time,
	regularTokens, elasticTokens kvflowcontrol.Tokens,
	regularStats, elasticStats deltaStats,
) {
	// Log stats, which reflect both elastic and regular at the interval defined
	// by blockedStreamLoggingInteval. If a high-enough log verbosity is
	// specified, shouldLogBacked will always be true, but since this method
	// executes at the frequency of scraping the metric, we will still log at a
	// reasonable rate.
	logBlockedStream := func(stream kvflowcontrol.Stream, blockedCount int, buf *strings.Builder) {
		if blockedCount == 1 {
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
	if regularStats.noTokenDuration == 0 && elasticStats.noTokenDuration == 0 {
		return
	}

	b.blockedCount++
	// TODO(sumeer): should be picking the top-k and not some arbitrary subset.
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
		deductionKindFunc := func(class string, stats deltaStats) {
			if stats.tokensDeductedForceFlush == 0 && stats.tokensDeductedPreventSendQueue == 0 {
				return
			}
			fmt.Fprintf(&bb, " (%s", class)
			if stats.tokensDeductedForceFlush > 0 {
				fmt.Fprintf(&bb, " force: %s", pprintTokens(stats.tokensDeductedForceFlush))
			}
			if stats.tokensDeductedPreventSendQueue > 0 {
				fmt.Fprintf(&bb, " prevent: %s", pprintTokens(stats.tokensDeductedPreventSendQueue))
			}
			fmt.Fprintf(&bb, ")")
		}
		deductionKindFunc("regular", regularStats)
		deductionKindFunc("elastic", elasticStats)
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

// SendTokenWatcher can be used for watching and waiting on available elastic
// send tokens. The caller registers a notification, which will be called when
// elastic tokens are available for the given stream being watched. Note that
// only elastic tokens are watched, as the SendTokenWatcher is intended to be
// used when a send queue exists for a replication stream, requiring only one
// goroutine per stream.
type SendTokenWatcher struct {
	stopper  *stop.Stopper
	clock    timeutil.TimeSource
	watchers syncutil.Map[kvflowcontrol.Stream, sendStreamTokenWatcher]
}

// NewSendTokenWatcher creates a new SendTokenWatcher.
func NewSendTokenWatcher(stopper *stop.Stopper, clock timeutil.TimeSource) *SendTokenWatcher {
	return &SendTokenWatcher{stopper: stopper, clock: clock}
}

const (
	// sendTokenWatcherWC is the class of tokens the send token watcher is
	// watching.
	sendTokenWatcherWC = admissionpb.ElasticWorkClass
	// watcherIdleCloseDuration is the duration after which the watcher will stop
	// if there are no registered notifications.
	watcherIdleCloseDuration = 1 * time.Minute
)

// TokenGrantNotification is an interface that is called when tokens are
// available.
type TokenGrantNotification interface {
	// Notify is called when tokens are available to be granted.
	Notify(context.Context)
}

// SendTokenWatcherHandle is a unique handle that is watching for available
// elastic send tokens on a stream.
type SendTokenWatcherHandle struct {
	// id is the unique identifier for this handle.
	id uint64
	// stream is the stream that this handle is watching.
	stream kvflowcontrol.Stream
}

// NotifyWhenAvailable queues up for elastic tokens for the given send token
// counter. When elastic tokens are available, the provided
// TokenGrantNotification is called and the notification is deregistered. It is
// the caller's responsibility to call CancelHandle if tokens are no longer
// needed.
//
// Note the given context is used only for logging/tracing purposes and
// cancellation is not respected.
func (s *SendTokenWatcher) NotifyWhenAvailable(
	ctx context.Context, tc *tokenCounter, notify TokenGrantNotification,
) SendTokenWatcherHandle {
	return s.watcher(tc).add(ctx, notify)
}

// CancelHandle cancels the given handle, stopping it from being notified when
// tokens are available.
func (s *SendTokenWatcher) CancelHandle(ctx context.Context, handle SendTokenWatcherHandle) {
	if w, ok := s.watchers.Load(handle.stream); ok {
		w.remove(ctx, handle)
	}
}

// watcher returns the sendStreamTokenWatcher for the given stream. If the
// watcher does not exist, it is created.
func (s *SendTokenWatcher) watcher(tc *tokenCounter) *sendStreamTokenWatcher {
	if w, ok := s.watchers.Load(tc.Stream()); ok {
		return w
	}
	w, _ := s.watchers.LoadOrStore(
		tc.Stream(), newSendStreamTokenWatcher(s.stopper, tc, s.clock.NewTimer()))
	return w
}

// sendStreamTokenWatcher is a watcher for available elastic send tokens on a
// stream. It watches for available tokens and notifies the caller when tokens
// are available.
type sendStreamTokenWatcher struct {
	stopper *stop.Stopper
	tc      *tokenCounter
	// nonEmptyCh is used to signal the watcher that there are events to process.
	// When the queue is empty, the watcher will wait for the next event to be
	// added before processing, by waiting on this channel.
	nonEmptyCh chan struct{}
	// timer is used to stop the watcher if there are no more handles for the
	// stream after some duration.
	timer timeutil.TimerI
	mu    struct {
		syncutil.Mutex
		// idSeq is the unique identifier for the next handle.
		idSeq uint64
		// started is true if the watcher is running, false otherwise. It is used
		// to prevent running more than one goroutine per stream and to stop the
		// watcher if there are no more handles for the stream after some duration.
		started bool
		// queueItems maps handle ids to their grant notification.
		queueItems map[uint64]TokenGrantNotification
		// queue is the FIFO ordered queue of handle ids to be notified when tokens
		// are available.
		queue *queue.Queue[uint64]
	}
}

func newSendStreamTokenWatcher(
	stopper *stop.Stopper, tc *tokenCounter, timer timeutil.TimerI,
) *sendStreamTokenWatcher {
	w := &sendStreamTokenWatcher{
		stopper:    stopper,
		tc:         tc,
		timer:      timer,
		nonEmptyCh: make(chan struct{}, 1),
	}
	w.mu.started = false
	w.mu.queueItems = make(map[uint64]TokenGrantNotification)
	w.mu.queue, _ = queue.NewQueue[uint64]()
	return w
}

// add adds a handle to be watched for available tokens. The handle is added to
// the queue and the watcher is started if it is not already running.
func (w *sendStreamTokenWatcher) add(
	ctx context.Context, notify TokenGrantNotification,
) SendTokenWatcherHandle {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.mu.idSeq++
	wasEmpty := w.emptyLocked()
	wasStopped := !w.mu.started
	handle := SendTokenWatcherHandle{id: w.mu.idSeq, stream: w.tc.stream}
	w.mu.queueItems[handle.id] = notify
	w.mu.queue.Enqueue(handle.id)

	log.VEventf(ctx, 3, "%v (id=%d) watching stream %v", notify, handle.id, w.tc.stream)

	if wasEmpty {
		// The queue was empty, so signal the watcher that there are events to
		// process.
		log.VEventf(ctx, 3, "signaling %v non-empty", w.tc.stream)
		select {
		case w.nonEmptyCh <- struct{}{}:
		default:
		}
	}

	if wasStopped {
		// The watcher isn't running, so start it.
		log.VEventf(ctx, 2, "starting %v send stream token watcher", w.tc.stream)
		if err := w.stopper.RunAsyncTask(ctx,
			"flow-control-send-stream-token-watcher", w.run); err == nil {
			w.mu.started = true
		} else {
			log.Warningf(ctx, "failed to start send stream token watcher: %v", err)
		}
	}

	return handle
}

// remove removes the handle from being watched for available tokens.
func (w *sendStreamTokenWatcher) remove(ctx context.Context, handle SendTokenWatcherHandle) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Don't bother removing it from the queue. When the handle is dequeued, the
	// handle won't be in the queueItems map and will be ignored.
	if notification, ok := w.mu.queueItems[handle.id]; ok {
		log.VEventf(ctx, 3, "%v (id=%d) stopped watching stream %v",
			notification, handle.id, w.tc.stream)
		delete(w.mu.queueItems, handle.id)
	}
}

func (w *sendStreamTokenWatcher) run(_ context.Context) {
	ctx := context.Background()
	for {
		select {
		// Drain the nonEmptyCh, we will check if the queue is empty under the
		// lock, which is also held to signal the nonEmptyCh. If the queue later
		// becomes empty, then non-empty, nonEmptyCh be signaled again.
		case <-w.nonEmptyCh:
		default:
		}
		if w.empty() {
			w.timer.Reset(watcherIdleCloseDuration)
			// The watcher will wait here until a item is added to the queue, or
			// until the stopper is quiescing.
			select {
			case <-w.stopper.ShouldQuiesce():
				return
			case <-w.timer.Ch():
				w.timer.MarkRead()
				w.timer.Stop()
				w.mu.Lock()
				// The queue has been empty for watcherIdleCloseDuration, check if
				// this is still the case.
				//
				// Since the timer firing could have raced with an item being added to
				// the queue, so we need to check again if the queue is empty. Since
				// add() retains a lock for its lifetime, we can be sure that the queue
				// is empty if we hold the lock until the end of this method. If an
				// item is added a short time after, it will wait to acquire the lock,
				// notice the watcher is now stopped and start it again.
				if w.emptyLocked() {
					//nolint:deferloop TODO(#137605)
					defer w.mu.Unlock()
					w.mu.started = false
					return
				}
				// Otherwise, the queue is non-empty, so continue to token checking.
				w.mu.Unlock() // nolint:deferunlockcheck
			case <-w.nonEmptyCh:
			}
		}

		available, handle := w.tc.TokensAvailable(sendTokenWatcherWC)
		// When there are no tokens available, wait for token counter channel to be
		// signaled, or until the stopper is quiescing.
		if !available {
		waiting:
			for {
				select {
				case <-w.stopper.ShouldQuiesce():
					return
				case <-handle.waitChannel():
					if handle.confirmHaveTokensAndUnblockNextWaiter() {
						break waiting
					}
				}
			}
		}
		// There were either tokens available (without waiting), or we waited, were
		// unblocked and confirmed that there are tokens available. Notify the next
		// handle in line.
		if grant, found := w.nextGrant(); found {
			log.VInfof(ctx, 4,
				"notifying %v of available tokens for stream %v", grant, w.tc.stream)
			grant.Notify(ctx)
		}
	}
}

// nextGrant returns the next grant in the queue and true if a grant is
// available. If no grant is available, it returns false. If a grant is found
// and dequeued, it will be removed from the queue.
func (w *sendStreamTokenWatcher) nextGrant() (grant TokenGrantNotification, found bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, ok := w.mu.queue.Dequeue(); ok; id, ok = w.mu.queue.Dequeue() {
		// The front of the queue could be a handle that was removed, so we need to
		// check if it's still in the queueItems map.
		if grant, found = w.mu.queueItems[id]; found {
			delete(w.mu.queueItems, id)
			return grant, found
		}
	}
	// Either the queue was empty or non-empty but every handle was removed.
	return nil, false
}

// empty returns true iff there are no handles in the queue.
func (w *sendStreamTokenWatcher) empty() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.emptyLocked()
}

func (w *sendStreamTokenWatcher) emptyLocked() bool {
	return len(w.mu.queueItems) == 0
}
