// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var updateBehindNanos = metric.Metadata{
	Name: "spanconfig.kvsubscriber.update_behind_nanos",
	Help: "Difference between the current time and when the KVSubscriber received its last update" +
		" (an ever increasing number indicates that we're no longer receiving updates)",
	Measurement: "Nanoseconds",
	Unit:        metric.Unit_NANOSECONDS,
}

var protectedRecordCount = metric.Metadata{
	Name:        "spanconfig.kvsubscriber.protected_record_count",
	Help:        "Number of protected timestamp records, as seen by KV",
	Measurement: "Records",
	Unit:        metric.Unit_COUNT,
}

var oldestProtectedRecordNanos = metric.Metadata{
	Name: "spanconfig.kvsubscriber.oldest_protected_record_nanos",
	Help: "Difference between the current time and the oldest protected timestamp" +
		" (sudden drops indicate a record being released; an ever increasing" +
		" number indicates that the oldest record is around and preventing GC if > configured GC TTL)",
	Measurement: "Nanoseconds",
	Unit:        metric.Unit_NANOSECONDS,
}

// metricsPollerInterval determines the frequency at which we refresh internal
// metrics.
var metricsPollerInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"spanconfig.kvsubscriber.metrics_poller_interval",
	"the interval at which the spanconfig.kvsubscriber.* metrics are kept up-to-date; set to 0 to disable the mechanism",
	5*time.Second,
	settings.NonNegativeDuration,
)

// KVSubscriber is used to subscribe to global span configuration changes. It's
// a concrete implementation of the spanconfig.KVSubscriber interface.
//
// It's expected to Start-ed once, after which one or many subscribers can
// listen in for updates. Internally we maintain a rangefeed over the global
// store of span configurations (system.span_configurations), applying updates
// from it into an internal spanconfig.Store. A read-only view of this data
// structure (spanconfig.StoreReader) is exposed as part of the KVSubscriber
// interface. Rangefeeds used as is don't offer any ordering guarantees with
// respect to updates made over non-overlapping keys, which is something we care
// about[1]. For that reason we make use of a rangefeed buffer, accumulating raw
// rangefeed updates and flushing them out en-masse in timestamp order when the
// rangefeed frontier is bumped[2]. If the buffer overflows (as dictated by the
// memory limit the KVSubscriber is instantiated with), the old rangefeed is
// wound down and a new one re-established.
//
// When running into the internal errors described above, it's safe for us to
// re-establish the underlying rangefeeds. When re-establishing a new rangefeed
// and populating a spanconfig.Store using the contents of the initial scan[3],
// we wish to preserve the existing spanconfig.StoreReader. Discarding it would
// entail either blocking all external readers until a new
// spanconfig.StoreReader was fully populated, or presenting an inconsistent
// view of the spanconfig.Store that's currently being populated. For new
// rangefeeds what we do then is route all updates from the initial scan to a
// fresh spanconfig.Store, and once the initial scan is done, swap at the source
// for the exported spanconfig.StoreReader. During the initial scan, concurrent
// readers would continue to observe the last spanconfig.StoreReader if any.
// After the swap, it would observe the more up-to-date source instead. Future
// incremental updates will also target the new source. When this source swap
// occurs, we inform the handler of the need to possibly refresh its view of all
// configs.
//
// TODO(irfansharif): When swapping the old spanconfig.StoreReader for the new,
// instead of informing registered handlers with an everything [min,max) span,
// we could diff the two data structures and only emit targeted updates.
//
// [1]: For a given key k, it's config may be stored as part of a larger span S
// (where S.start <= k < S.end). It's possible for S to get deleted and
// replaced with sub-spans S1...SN in the same transaction if the span is
// getting split. When applying these updates, we need to make sure to
// process the deletion event for S before processing S1...SN.
//
// [2]: In our example above deleting the config for S and adding configs for
// S1...SN we want to make sure that we apply the full set of updates all
// at once -- lest we expose the intermediate state where the config for S
// was deleted but the configs for S1...SN were not yet applied.
//
// [3]: TODO(irfansharif): When tearing down the subscriber due to underlying
// errors, we could also capture a checkpoint to use the next time the
// subscriber is established. That way we can avoid the full initial scan
// over the span configuration state and simply pick up where we left off
// with our existing spanconfig.Store.
type KVSubscriber struct {
	fallback roachpb.SpanConfig
	knobs    *spanconfig.TestingKnobs
	settings *cluster.Settings

	rfc *rangefeedcache.Watcher

	mu struct { // serializes between Start and external threads
		syncutil.RWMutex
		lastUpdated hlc.Timestamp
		// internal is the internal spanconfig.Store maintained by the
		// KVSubscriber. A read-only view over this store is exposed as part of
		// the interface. When re-subscribing, a fresh spanconfig.Store is
		// populated while the exposed spanconfig.StoreReader appears static.
		// Once sufficiently caught up, the fresh spanconfig.Store is swapped in
		// and the old discarded. See type-level comment for more details.
		internal spanconfig.Store
		handlers []handler
	}

	clock   *hlc.Clock
	metrics *Metrics

	// boundsReader provides a handle to the global SpanConfigBounds state.
	boundsReader spanconfigstore.BoundsReader
}

var _ spanconfig.KVSubscriber = &KVSubscriber{}

// Metrics are the Metrics associated with an instance of the
// KVSubscriber.
type Metrics struct {
	// UpdateBehindNanos is the difference between the current time and when the
	// last update was received by the KVSubscriber. This metric should be
	// interpreted as a measure of the KVSubscribers' staleness.
	UpdateBehindNanos *metric.Gauge
	// ProtectedRecordCount is total number of protected timestamp records, as
	// seen by KV.
	ProtectedRecordCount *metric.Gauge
	// OldestProtectedRecord is  between the current time and the oldest
	// protected timestamp.
	OldestProtectedRecordNanos *metric.Gauge
}

func makeKVSubscriberMetrics() *Metrics {
	return &Metrics{
		UpdateBehindNanos:          metric.NewGauge(updateBehindNanos),
		ProtectedRecordCount:       metric.NewGauge(protectedRecordCount),
		OldestProtectedRecordNanos: metric.NewGauge(oldestProtectedRecordNanos),
	}
}

// MetricStruct implements the metric.Struct interface.
func (k *Metrics) MetricStruct() {}

var _ metric.Struct = &Metrics{}

// spanConfigurationsTableRowSize is an estimate of the size of a single row in
// the system.span_configurations table (size of start/end key, and size of a
// marshaled span config proto). The value used here was pulled out of thin air
// -- it only serves to coarsely limit how large the KVSubscriber's underlying
// rangefeed buffer can get.
const spanConfigurationsTableRowSize = 5 << 10 // 5 KB

// New instantiates a KVSubscriber.
func New(
	clock *hlc.Clock,
	rangeFeedFactory *rangefeed.Factory,
	spanConfigurationsTableID uint32,
	bufferMemLimit int64,
	fallback roachpb.SpanConfig,
	settings *cluster.Settings,
	boundsReader spanconfigstore.BoundsReader,
	knobs *spanconfig.TestingKnobs,
	registry *metric.Registry,
) *KVSubscriber {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	spanConfigTableStart := keys.SystemSQLCodec.IndexPrefix(
		spanConfigurationsTableID,
		keys.SpanConfigurationsTablePrimaryKeyIndexID,
	)
	spanConfigTableSpan := roachpb.Span{
		Key:    spanConfigTableStart,
		EndKey: spanConfigTableStart.PrefixEnd(),
	}
	spanConfigStore := spanconfigstore.New(fallback, settings, boundsReader, knobs)
	s := &KVSubscriber{
		fallback:     fallback,
		knobs:        knobs,
		settings:     settings,
		clock:        clock,
		boundsReader: boundsReader,
	}
	var rfCacheKnobs *rangefeedcache.TestingKnobs
	if knobs != nil {
		rfCacheKnobs, _ = knobs.KVSubscriberRangeFeedKnobs.(*rangefeedcache.TestingKnobs)
	}
	s.rfc = rangefeedcache.NewWatcher(
		"spanconfig-subscriber",
		clock, rangeFeedFactory,
		int(bufferMemLimit/spanConfigurationsTableRowSize),
		[]roachpb.Span{spanConfigTableSpan},
		true, // withPrevValue
		true, // withRowTSInInitialScan
		NewSpanConfigDecoder().TranslateEvent,
		s.handleUpdate,
		rfCacheKnobs,
	)
	s.mu.internal = spanConfigStore
	s.metrics = makeKVSubscriberMetrics()
	if registry != nil {
		registry.AddMetricStruct(s.metrics)
	}
	return s
}

// Start establishes a subscription (internally: rangefeed) over the global
// store of span configs. It fires off an async task to do so, re-establishing
// internally when retryable errors[1] occur and stopping only when the surround
// stopper is quiescing or the context canceled. All installed handlers are
// invoked in the single async task thread.
//
// [1]: It's possible for retryable errors to occur internally, at which point
//
//	we tear down the existing subscription and re-establish another. When
//	unsubscribed, the exposed spanconfig.StoreReader continues to be
//	readable (though no longer incrementally maintained -- the view gets
//	progressively staler overtime). Existing handlers are kept intact and
//	notified when the subscription is re-established. After re-subscribing,
//	the exported StoreReader will be up-to-date and continue to be
//	incrementally maintained.
func (s *KVSubscriber) Start(ctx context.Context, stopper *stop.Stopper) error {
	if err := stopper.RunAsyncTask(ctx, "kvsubscriber-metrics",
		func(ctx context.Context) {
			settingChangeCh := make(chan struct{}, 1)
			metricsPollerInterval.SetOnChange(
				&s.settings.SV, func(ctx context.Context) {
					select {
					case settingChangeCh <- struct{}{}:
					default:
					}
				})

			timer := timeutil.NewTimer()
			defer timer.Stop()

			for {
				interval := metricsPollerInterval.Get(&s.settings.SV)
				if interval > 0 {
					timer.Reset(interval)
				} else {
					// Disable the mechanism.
					timer.Stop()
					timer = timeutil.NewTimer()
				}
				select {
				case <-timer.C:
					timer.Read = true
					s.updateMetrics(ctx)
					continue

				case <-settingChangeCh:
					// Loop around to use the updated timer.
					continue

				case <-stopper.ShouldQuiesce():
					return
				}
			}
		}); err != nil {
		return err
	}

	return rangefeedcache.Start(ctx, stopper, s.rfc, nil /* onError */)
}

func (s *KVSubscriber) updateMetrics(ctx context.Context) {
	protectedTimestamps, lastUpdated, err := s.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	if err != nil {
		log.Errorf(ctx, "while refreshing kvsubscriber metrics: %v", err)
		return
	}

	earliestTS := hlc.Timestamp{}
	for _, protectedTimestamp := range protectedTimestamps {
		if earliestTS.IsEmpty() || protectedTimestamp.Less(earliestTS) {
			earliestTS = protectedTimestamp
		}
	}

	now := s.clock.PhysicalTime()
	s.metrics.ProtectedRecordCount.Update(int64(len(protectedTimestamps)))
	s.metrics.UpdateBehindNanos.Update(now.Sub(lastUpdated.GoTime()).Nanoseconds())
	if earliestTS.IsEmpty() {
		s.metrics.OldestProtectedRecordNanos.Update(0)
	} else {
		s.metrics.OldestProtectedRecordNanos.Update(now.Sub(earliestTS.GoTime()).Nanoseconds())
	}
}

// Subscribe installs a callback that's invoked with whatever span may have seen
// a config update.
func (s *KVSubscriber) Subscribe(fn func(context.Context, roachpb.Span)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.handlers = append(s.mu.handlers, handler{fn: fn})
}

// LastUpdated is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) LastUpdated() hlc.Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.lastUpdated
}

// NeedsSplit is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.NeedsSplit(ctx, start, end)
}

// ComputeSplitKey is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) ComputeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) (roachpb.RKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.ComputeSplitKey(ctx, start, end)
}

// GetSpanConfigForKey is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.GetSpanConfigForKey(ctx, key)
}

// GetProtectionTimestamps is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) GetProtectionTimestamps(
	ctx context.Context, sp roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, _ error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.mu.internal.ForEachOverlappingSpanConfig(ctx, sp,
		func(sp roachpb.Span, config roachpb.SpanConfig) error {
			for _, protection := range config.GCPolicy.ProtectionPolicies {
				// If the current span is a subset of the key space we exclude from full
				// cluster backups, then we ignore it. This avoids placing a protected
				// timestamp and hold up GC on spans not needed for backup (i.e.
				// NodeLiveness, Timeseries). These spans tend to be high churn,
				// accumulating high amounts of MVCC garbage. Placing a PTS on these
				// spans can thus be detrimental.
				if keys.ExcludeFromBackupSpan.Contains(sp) {
					continue
				}
				// If the SpanConfig that applies to this span indicates that the span
				// is going to be excluded from backup, and the protection policy was
				// written by a backup, then ignore it. This prevents the
				// ProtectionPolicy from holding up GC over the span.
				if config.ExcludeDataFromBackup && protection.IgnoreIfExcludedFromBackup {
					continue
				}
				protectionTimestamps = append(protectionTimestamps, protection.ProtectedTimestamp)
			}
			return nil
		}); err != nil {
		return nil, hlc.Timestamp{}, err
	}

	return protectionTimestamps, s.mu.lastUpdated, nil
}

func (s *KVSubscriber) handleUpdate(ctx context.Context, u rangefeedcache.Update) {
	switch u.Type {
	case rangefeedcache.CompleteUpdate:
		s.handleCompleteUpdate(ctx, u.Timestamp, u.Events)
	case rangefeedcache.IncrementalUpdate:
		s.handlePartialUpdate(ctx, u.Timestamp, u.Events)
	}
}

func (s *KVSubscriber) handleCompleteUpdate(
	ctx context.Context, ts hlc.Timestamp, events []rangefeedbuffer.Event,
) {
	freshStore := spanconfigstore.New(s.fallback, s.settings, s.boundsReader, s.knobs)
	for _, ev := range events {
		freshStore.Apply(ctx, false /* dryrun */, ev.(*BufferEvent).Update)
	}
	handlers := func() []handler {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.internal = freshStore
		s.setLastUpdatedLocked(ts)
		return s.mu.handlers
	}()

	for i := range handlers {
		handler := &handlers[i] // mutated by invoke
		handler.invoke(ctx, keys.EverythingSpan)
	}
}

func (s *KVSubscriber) SetLastUpdatedTest() {
	s.mu.Lock()
	s.setLastUpdatedLocked(s.clock.Now())
	defer s.mu.Unlock()
}

func (s *KVSubscriber) setLastUpdatedLocked(ts hlc.Timestamp) {
	s.mu.lastUpdated = ts
	nanos := timeutil.Since(s.mu.lastUpdated.GoTime()).Nanoseconds()
	s.metrics.UpdateBehindNanos.Update(nanos)
}

func (s *KVSubscriber) handlePartialUpdate(
	ctx context.Context, ts hlc.Timestamp, events []rangefeedbuffer.Event,
) {
	handlers := func() []handler {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, ev := range events {
			// TODO(irfansharif): We can apply a batch of updates atomically
			// now that the StoreWriter interface supports it; it'll let us
			// avoid this mutex.
			s.mu.internal.Apply(ctx, false /* dryrun */, ev.(*BufferEvent).Update)
		}
		s.setLastUpdatedLocked(ts)
		return s.mu.handlers
	}()

	for i := range handlers {
		handler := &handlers[i] // mutated by invoke
		for _, ev := range events {
			target := ev.(*BufferEvent).Update.GetTarget()
			handler.invoke(ctx, target.KeyspaceTargeted())
		}
	}
}

type handler struct {
	initialized bool // tracks whether we need to invoke with a [min,max) span first
	fn          func(ctx context.Context, update roachpb.Span)
}

func (h *handler) invoke(ctx context.Context, update roachpb.Span) {
	if !h.initialized {
		h.fn(ctx, keys.EverythingSpan)
		h.initialized = true

		if update.Equal(keys.EverythingSpan) {
			return // we can opportunistically avoid re-invoking with the same update
		}
	}

	h.fn(ctx, update)
}

type BufferEvent struct {
	spanconfig.Update
	ts hlc.Timestamp
}

// Timestamp implements the rangefeedbuffer.Event interface.
func (w *BufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}

var _ rangefeedbuffer.Event = &BufferEvent{}
