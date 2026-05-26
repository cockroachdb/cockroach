// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"encoding/hex"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obs/workloadid"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Enabled controls whether ASH sampling is active. This is a
// system-level setting because the sampler is a process-wide
// singleton.
var Enabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"obs.ash.enabled",
	"enable active session history sampling",
	false,
	settings.WithPublic,
)

// enabled caches the value of the obs.ash.enabled cluster setting so
// that callers of SetWorkState do not need to pass in cluster settings.
var enabled atomic.Bool

// SampleInterval controls how often ASH samples are taken. This is a
// system-level setting because the sampler is a process-wide
// singleton.
var SampleInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"obs.ash.sample_interval",
	"interval between ASH samples",
	time.Second,
	settings.PositiveDuration,
	settings.WithPublic,
)

// BufferSize controls the maximum number of ASH samples retained in
// memory. This is a system-level setting because the sampler is a
// process-wide singleton.
var BufferSize = settings.RegisterIntSetting(
	settings.SystemVisible,
	"obs.ash.buffer_size",
	"number of ASH samples to retain in memory",
	1_000_000,
	settings.PositiveInt,
	settings.WithPublic,
)

// LogInterval controls how often the top-N workload summary is
// logged to the OPS channel. Each summary reports the most
// frequently sampled (WorkEventType, WorkEvent, WorkloadID)
// combinations in the ring buffer since the last report.
//
// This value is also used as the lookback window by the ASH report
// profiler when writing reports alongside CPU profiles or goroutine
// dumps triggered by the env sampler.
var LogInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"obs.ash.log_interval",
	"interval between periodic ASH top-N workload summary logs; "+
		"also used as the lookback window for ASH reports written "+
		"by the env sampler profiler",
	10*time.Minute,
	settings.PositiveDuration,
	settings.WithPublic,
)

// MaxEnrichmentRetries controls the maximum number of sampler ticks
// an unenriched sample will be retried before being emitted with
// partial data.
var MaxEnrichmentRetries = settings.RegisterIntSetting(
	settings.SystemVisible,
	"obs.ash.max_enrichment_retries",
	"maximum number of sampler ticks to retry enrichment for a sample "+
		"before emitting it with partial data",
	5,
	settings.NonNegativeInt,
	settings.WithPublic,
)

// LogTopN controls the maximum number of workload entries included
// in each periodic summary. Entries are ranked by sample count
// (descending), so only the most frequently sampled workloads appear.
var LogTopN = settings.RegisterIntSetting(
	settings.SystemVisible,
	"obs.ash.log_top_n",
	"maximum number of entries in periodic ASH workload summary, "+
		"ranked by sample count descending",
	10,
	settings.PositiveInt,
	settings.WithPublic,
)

// EnrichmentResolverFn fetches enrichment ID-to-data mappings from a
// remote node. The sampler calls this when local resolution fails and
// the work state has a non-zero GatewayNodeID that differs from the
// local node.
type EnrichmentResolverFn func(ctx context.Context, nodeID roachpb.NodeID, ids []uint64) (map[uint64]EnrichmentData, error)

// globalSampler is the process-wide ASH sampler singleton. It is
// initialized once by InitGlobalSampler and read by GetSamples.
var globalSampler atomic.Pointer[Sampler]

// initSamplerOnce ensures that the global sampler is created exactly
// once per process, even when multiple SQLServers exist in a
// multi-tenant shared-process environment.
var initSamplerOnce sync.Once

// maxWorkloadIDCacheSize is the maximum number of workload ID to string
// mappings retained in the LRU cache. Each entry is small (~100 bytes),
// so 10,000 entries uses roughly 1 MB.
const maxWorkloadIDCacheSize = 10000

type pendingSample struct {
	gid           int64
	state         WorkState
	workloadIDStr string
	enrichment    EnrichmentData
	enriched      bool
}

// pendingEnrichmentSample holds an unenriched sample that will be
// retried on subsequent sampler ticks.
type pendingEnrichmentSample struct {
	sample        ASHSample
	enrichmentID  uint64
	gatewayNodeID roachpb.NodeID
	retryCount    int
}

// workloadKey groups samples for the periodic top-N summary.
type workloadKey struct {
	WorkEventType WorkEventType
	WorkEvent     string
	WorkloadID    string
}

// Sampler periodically samples the active work states and stores ASH data.
type Sampler struct {
	nodeID  roachpb.NodeID
	st      *cluster.Settings
	buffer  *RingBuffer
	stopper *stop.Stopper
	started bool
	metrics Metrics
	// interval caches the sample interval so the timer loop can read it
	// without locking, while the SetOnChange callback updates it.
	interval atomic.Int64
	// workloadIDCache caches workload ID string encodings keyed by
	// (id, type) to avoid repeated allocations for the same workload.
	workloadIDCache *cache.UnorderedCache
	// resolver, when set, fetches enrichment mappings from remote nodes
	// for work states whose enrichment could not be resolved locally.
	resolver atomic.Pointer[EnrichmentResolverFn]
	// enrichmentCache is the process-wide enrichment cache used to
	// resolve enrichment IDs to enrichment data.
	enrichmentCache *EnrichmentCache
	// pendingSamples is a reusable slice for collecting work state snapshots
	// during rangeWorkStates. Reused across samples to avoid per-sample
	// slice allocation.
	pendingSamples []pendingSample
	// pendingEnrichment holds samples from prior ticks that could not
	// be enriched and will be retried.
	pendingEnrichment []pendingEnrichmentSample
	// lastLogTime records when the last summary was logged.
	lastLogTime time.Time
}

// newSampler creates a new ASH sampler.
func newSampler(nodeID roachpb.NodeID, st *cluster.Settings, stopper *stop.Stopper) *Sampler {
	bufSize := int(BufferSize.Get(&st.SV))
	return &Sampler{
		nodeID:          nodeID,
		st:              st,
		buffer:          NewRingBuffer(bufSize),
		stopper:         stopper,
		metrics:         makeMetrics(),
		enrichmentCache: GlobalEnrichmentCache(),
		workloadIDCache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(size int, key, value interface{}) bool {
				return size > maxWorkloadIDCacheSize
			},
		}),
	}
}

// SetEnrichmentResolver sets the callback used to fetch enrichment
// mappings from remote nodes when local resolution fails.
func (s *Sampler) SetEnrichmentResolver(fn EnrichmentResolverFn) {
	s.resolver.Store(&fn)
}

// SetGlobalEnrichmentResolver sets the resolver on the global sampler.
// This is a no-op if the global sampler has not been initialized.
func SetGlobalEnrichmentResolver(fn EnrichmentResolverFn) {
	s := globalSampler.Load()
	if s == nil {
		return
	}
	s.SetEnrichmentResolver(fn)
}

// start begins the background sampling loop. It must be called at most once.
func (s *Sampler) start(ctx context.Context) error {
	if s.started {
		return errors.AssertionFailedf("ASH sampler already started")
	}
	s.started = true

	enabled.Store(Enabled.Get(&s.st.SV))
	Enabled.SetOnChange(&s.st.SV, func(ctx context.Context) {
		enabled.Store(Enabled.Get(&s.st.SV))
	})
	BufferSize.SetOnChange(&s.st.SV, func(ctx context.Context) {
		newSize := int(BufferSize.Get(&s.st.SV))
		s.buffer.Resize(newSize)
	})
	s.interval.Store(int64(SampleInterval.Get(&s.st.SV)))
	SampleInterval.SetOnChange(&s.st.SV, func(ctx context.Context) {
		s.interval.Store(int64(SampleInterval.Get(&s.st.SV)))
	})
	s.lastLogTime = timeutil.Now()

	log.Ops.Info(ctx, "starting ASH sampler")
	return s.stopper.RunAsyncTask(ctx, "ash-sampler", func(ctx context.Context) {
		s.run(ctx)
	})
}

func (s *Sampler) run(ctx context.Context) {
	var timer timeutil.Timer
	defer timer.Stop()
	timer.Reset(time.Duration(s.interval.Load()))

	for {
		select {
		case <-timer.C:
			timer.Reset(time.Duration(s.interval.Load()))
			if Enabled.Get(&s.st.SV) {
				s.takeSample(ctx)
			}
		case <-s.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

// takeSample captures a snapshot of all active work states.
func (s *Sampler) takeSample(ctx context.Context) {
	start := timeutil.Now()
	defer func() {
		elapsed := timeutil.Since(start)
		s.metrics.TakeSampleLatency.RecordValue(elapsed.Nanoseconds())
	}()

	sampleTime := start

	// Retry enrichment for samples from prior ticks.
	s.retryPendingEnrichment(ctx)

	// Collect work states. rangeWorkStates reclaims retired states after
	// iteration so pooled objects can be reused.
	s.pendingSamples = s.pendingSamples[:0]
	rangeWorkStates(func(gid int64, state WorkState) bool {
		s.pendingSamples = append(s.pendingSamples, pendingSample{gid: gid, state: state})
		return true
	})

	// First pass: resolve workload IDs and enrichment data from
	// the local cache, tracking indices where enrichment fails.
	var unresolvedIndices []int
	for i := range s.pendingSamples {
		ps := &s.pendingSamples[i]

		if ps.state.WorkloadInfo.WorkloadID != 0 {
			cacheKey := workloadCacheKey{
				id:  ps.state.WorkloadInfo.WorkloadID,
				typ: ps.state.WorkloadInfo.WorkloadType,
			}
			if cached, ok := s.workloadIDCache.Get(cacheKey); ok {
				ps.workloadIDStr = cached.(string)
			} else {
				ps.workloadIDStr = encodeWorkloadID(
					ps.state.WorkloadInfo.WorkloadID,
					ps.state.WorkloadInfo.WorkloadType,
				)
				s.workloadIDCache.Add(cacheKey, ps.workloadIDStr)
			}
		}

		if ps.state.WorkloadInfo.EnrichmentID != 0 && s.enrichmentCache != nil {
			if data, ok := s.enrichmentCache.Lookup(ps.state.WorkloadInfo.EnrichmentID); ok {
				ps.enrichment = data
				ps.enriched = true
				s.metrics.EnrichmentHits.Inc(1)
			} else {
				unresolvedIndices = append(unresolvedIndices, i)
				s.metrics.EnrichmentMisses.Inc(1)
			}
		}
	}

	// If there are unresolved enrichment IDs, try fetching from
	// remote gateway nodes.
	if len(unresolvedIndices) > 0 {
		s.resolveRemoteEnrichment(ctx, unresolvedIndices)
	}

	// Emit samples.
	maxRetries := int(MaxEnrichmentRetries.Get(&s.st.SV))
	for i := range s.pendingSamples {
		ps := &s.pendingSamples[i]
		sample := ASHSample{
			SampleTime:    sampleTime,
			NodeID:        s.nodeID,
			TenantID:      ps.state.TenantID,
			WorkloadID:    ps.workloadIDStr,
			WorkloadType:  ps.state.WorkloadInfo.WorkloadType.String(),
			WorkEventType: ps.state.WorkEventType,
			WorkEvent:     ps.state.WorkEvent,
			GoroutineID:   ps.gid,
		}
		if ps.enriched {
			applySampleEnrichment(&sample, &ps.enrichment)
			s.buffer.Add(sample)
		} else if ps.state.WorkloadInfo.EnrichmentID != 0 && maxRetries > 0 {
			s.pendingEnrichment = append(s.pendingEnrichment, pendingEnrichmentSample{
				sample:        sample,
				enrichmentID:  ps.state.WorkloadInfo.EnrichmentID,
				gatewayNodeID: ps.state.WorkloadInfo.GatewayNodeID,
			})
		} else {
			s.buffer.Add(sample)
		}
	}
	s.metrics.SamplesCollected.Inc(int64(len(s.pendingSamples)))
	s.metrics.EnrichmentPending.Update(int64(len(s.pendingEnrichment)))
	if s.enrichmentCache != nil {
		s.metrics.EnrichmentCacheEntries.Update(int64(s.enrichmentCache.Size()))
	}

	s.maybeLogSummary(ctx)
}

// applySampleEnrichment populates the enrichment fields on an ASHSample.
func applySampleEnrichment(sample *ASHSample, data *EnrichmentData) {
	sample.AppName = data.AppName
	sample.User = data.User
	sample.Database = data.Database
	sample.SessionID = data.SessionID.String()
	sample.TxnID = data.TxnID.String()
	sample.PlanHash = data.PlanHash
}

// retryPendingEnrichment attempts to enrich samples from prior ticks
// that could not be resolved. Resolved samples are emitted to the
// ring buffer; samples that exceed max retries are emitted with
// partial data.
func (s *Sampler) retryPendingEnrichment(ctx context.Context) {
	if len(s.pendingEnrichment) == 0 {
		return
	}

	// First pass: try local cache.
	maxRetries := int(MaxEnrichmentRetries.Get(&s.st.SV))
	var stillUnresolved []int
	for i := range s.pendingEnrichment {
		pe := &s.pendingEnrichment[i]
		pe.retryCount++
		if s.enrichmentCache != nil {
			if data, ok := s.enrichmentCache.Lookup(pe.enrichmentID); ok {
				applySampleEnrichment(&pe.sample, &data)
				s.buffer.Add(pe.sample)
				pe.enrichmentID = 0
				continue
			}
		}
		if pe.retryCount >= maxRetries {
			s.buffer.Add(pe.sample)
			pe.enrichmentID = 0
			continue
		}
		stillUnresolved = append(stillUnresolved, i)
	}

	// Remote resolution for still-unresolved pending samples.
	if len(stillUnresolved) > 0 {
		s.resolveRemotePendingEnrichment(ctx, stillUnresolved)
	}

	// Compact: keep only unresolved entries.
	remaining := s.pendingEnrichment[:0]
	for i := range s.pendingEnrichment {
		if s.pendingEnrichment[i].enrichmentID != 0 {
			remaining = append(remaining, s.pendingEnrichment[i])
		}
	}
	s.pendingEnrichment = remaining
}

// workloadCount pairs a workload key with its sample count for sorting.
type workloadCount struct {
	key   workloadKey
	count int
}

// maybeLogSummary emits a top-N workload summary as structured events
// to the OPS log if enough time has elapsed since the last report.
// It scans the ring buffer for samples newer than lastLogTime and
// aggregates them by workload key.
// Note(alyshan): Logging is performed at the end of sampling tick.
// So if sampling is disabled (obs.ash.enabled) then there will be no logging.
// Similarly, the logging interval is lower bounded by the sample interval (obs.ash.sample_interval).
func (s *Sampler) maybeLogSummary(ctx context.Context) {
	logInterval := LogInterval.Get(&s.st.SV)
	if timeutil.Since(s.lastLogTime) < logInterval {
		return
	}

	cutoff := s.lastLogTime
	windowDuration := timeutil.Since(cutoff)

	// Scan the ring buffer newest-to-oldest and count samples since
	// the last report, stopping as soon as we hit a sample at or
	// before the cutoff.
	counts := make(map[workloadKey]int)
	totalSamples := 0
	s.buffer.RangeReverse(func(sample ASHSample) bool {
		if !sample.SampleTime.After(cutoff) {
			return false
		}
		key := workloadKey{
			WorkEventType: sample.WorkEventType,
			WorkEvent:     sample.WorkEvent,
			WorkloadID:    sample.WorkloadID,
		}
		counts[key]++
		totalSamples++
		return true
	})

	s.lastLogTime = timeutil.Now()

	if totalSamples == 0 {
		return
	}

	// Collect and sort entries by count descending.
	entries := make([]workloadCount, 0, len(counts))
	for k, c := range counts {
		entries = append(entries, workloadCount{key: k, count: c})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count > entries[j].count
	})
	topN := int(LogTopN.Get(&s.st.SV))
	if len(entries) > topN {
		entries = entries[:topN]
	}

	// Emit one structured event per top-N entry.
	for _, e := range entries {
		event := &eventpb.AshWorkloadSummary{
			WindowDurationMillis: windowDuration.Milliseconds(),
			WorkEventType:        e.key.WorkEventType.String(),
			WorkEvent:            e.key.WorkEvent,
			WorkloadID:           e.key.WorkloadID,
			SampleCount:          int64(e.count),
		}
		log.StructuredEvent(ctx, logpb.Severity_INFO, event)
	}
}

// resolveRemoteEnrichment fetches enrichment mappings from remote
// gateway nodes for new samples that could not be resolved from the
// local cache. It deduplicates RPCs by gateway node ID, groups the
// needed enrichment IDs per node, and skips the local node.
func (s *Sampler) resolveRemoteEnrichment(ctx context.Context, unresolvedIndices []int) {
	resolverPtr := s.resolver.Load()
	if resolverPtr == nil {
		return
	}
	resolver := *resolverPtr

	// Group unresolved enrichment IDs by gateway node, skipping node
	// ID 0 (unknown) and the sampler's own node.
	//
	// For separate-process SQL pods, GatewayNodeID in the
	// BatchRequest is 0 (see kvpb.Header.GatewayNodeID), so no
	// remote resolution is attempted. This is intentional: KV nodes
	// cannot dial SQL pods, and out-of-process tenants only see
	// their own SQL-side samples where enrichment resolves locally.
	type idSet = map[uint64]struct{}
	gatewayIDs := make(map[roachpb.NodeID]idSet)
	for _, idx := range unresolvedIndices {
		ps := &s.pendingSamples[idx]
		gw := ps.state.WorkloadInfo.GatewayNodeID
		if gw == 0 || gw == s.nodeID {
			continue
		}
		ids, ok := gatewayIDs[gw]
		if !ok {
			ids = make(idSet)
			gatewayIDs[gw] = ids
		}
		ids[ps.state.WorkloadInfo.EnrichmentID] = struct{}{}
	}

	// TODO(alyshan): Resolve nodes in parallel rather than
	// sequentially to reduce total resolution latency when multiple
	// gateway nodes are involved.
	s.fetchRemoteEnrichment(ctx, resolver, gatewayIDs)

	// Re-resolve previously-unresolved samples from the now-populated
	// local cache.
	for _, idx := range unresolvedIndices {
		ps := &s.pendingSamples[idx]
		if s.enrichmentCache != nil {
			if data, ok := s.enrichmentCache.Lookup(ps.state.WorkloadInfo.EnrichmentID); ok {
				ps.enrichment = data
				ps.enriched = true
			}
		}
	}
}

// resolveRemotePendingEnrichment fetches enrichment mappings for
// pending retry samples.
func (s *Sampler) resolveRemotePendingEnrichment(ctx context.Context, indices []int) {
	resolverPtr := s.resolver.Load()
	if resolverPtr == nil {
		return
	}
	resolver := *resolverPtr

	type idSet = map[uint64]struct{}
	gatewayIDs := make(map[roachpb.NodeID]idSet)
	for _, idx := range indices {
		pe := &s.pendingEnrichment[idx]
		gw := pe.gatewayNodeID
		if gw == 0 || gw == s.nodeID {
			continue
		}
		ids, ok := gatewayIDs[gw]
		if !ok {
			ids = make(idSet)
			gatewayIDs[gw] = ids
		}
		ids[pe.enrichmentID] = struct{}{}
	}

	s.fetchRemoteEnrichment(ctx, resolver, gatewayIDs)

	// Re-resolve from the now-populated local cache.
	for _, idx := range indices {
		pe := &s.pendingEnrichment[idx]
		if s.enrichmentCache != nil {
			if data, ok := s.enrichmentCache.Lookup(pe.enrichmentID); ok {
				applySampleEnrichment(&pe.sample, &data)
				s.buffer.Add(pe.sample)
				pe.enrichmentID = 0
			}
		}
	}
}

// fetchRemoteEnrichment issues RPCs to remote gateway nodes to fetch
// enrichment mappings and stores the results in the local enrichment
// cache. Each node gets a 250ms timeout to avoid stalling the sampler
// tick.
func (s *Sampler) fetchRemoteEnrichment(
	ctx context.Context,
	resolver EnrichmentResolverFn,
	gatewayIDs map[roachpb.NodeID]map[uint64]struct{},
) {
	for nodeID, ids := range gatewayIDs {
		idSlice := make([]uint64, 0, len(ids))
		for id := range ids {
			idSlice = append(idSlice, id)
		}
		if err := timeutil.RunWithTimeout(
			ctx, "ash-resolve-enrichment", 250*time.Millisecond,
			func(resolveCtx context.Context) error {
				mappings, err := resolver(resolveCtx, nodeID, idSlice)
				if err != nil {
					return err
				}
				if s.enrichmentCache != nil {
					s.enrichmentCache.StoreImmediate(mappings)
				}
				s.metrics.EnrichmentRemoteResolutions.Inc(int64(len(mappings)))
				return nil
			},
		); err != nil {
			log.Ops.Warningf(
				ctx, "ASH: failed to resolve enrichment mappings from n%d: %v",
				nodeID, err,
			)
		}
	}
}

// TakeSample forces the sampler to take an immediate sample. This is
// intended for use in tests to avoid timing dependencies.
func (s *Sampler) TakeSample(ctx context.Context) {
	s.takeSample(ctx)
}

// GetSamples returns all samples currently in the Sampler's buffer.
func (s *Sampler) GetSamples(result []ASHSample) []ASHSample {
	return s.buffer.GetAll(result)
}

// InitGlobalSampler creates and starts the process-wide ASH sampler.
// It is idempotent: only the first call creates the sampler; subsequent
// calls are no-ops. Callers should use GlobalSamplerMetrics() to obtain
// the metrics and register them with sysRegistry. This should be called
// during process-level server initialization (not per-tenant), after
// the node ID is known.
//
// The returned bool is true when this call actually created the
// sampler (first caller) and false for subsequent no-op calls.
func InitGlobalSampler(
	ctx context.Context, nodeID roachpb.NodeID, st *cluster.Settings, stopper *stop.Stopper,
) (bool, error) {
	var initErr error
	initialized := false
	initSamplerOnce.Do(func() {
		s := newSampler(nodeID, st, stopper)
		initErr = s.start(ctx)
		if initErr == nil {
			globalSampler.Store(s)
			initialized = true
		}
	})
	return initialized, initErr
}

// ResetGlobalSamplerForTesting resets the global ASH sampler so that
// the next call to InitGlobalSampler will create a fresh sampler. This
// is needed in test suites where multiple tests start and stop servers
// in the same process: the first server's stopper quiesces the sampler
// goroutine, and the sync.Once prevents re-initialization for
// subsequent servers.
func ResetGlobalSamplerForTesting() {
	globalSampler.Store(nil)
	initSamplerOnce = sync.Once{}
}

// GlobalSamplerMetrics returns the Metrics for the process-wide ASH
// sampler, or nil if the sampler has not been initialized yet.
func GlobalSamplerMetrics() *Metrics {
	if s := globalSampler.Load(); s != nil {
		return &s.metrics
	}
	return nil
}

// workloadCacheKey is the composite key for the workload ID string
// cache. Using both the numeric ID and the type prevents collisions
// when the same numeric value appears across different workload types.
type workloadCacheKey struct {
	id  uint64
	typ workloadid.WorkloadType
}

// encodeWorkloadID returns the string representation of a workload ID,
// choosing the encoding based on workload type.
func encodeWorkloadID(id uint64, typ workloadid.WorkloadType) string {
	switch typ {
	case workloadid.WorkloadTypeJob:
		return strconv.FormatUint(id, 10)
	case workloadid.WorkloadTypeSystem:
		return workloadid.WorkloadID(id).Name()
	default: // WorkloadTypeUnknown, WorkloadTypeStatement, WorkloadTypeCommit
		return encodeStmtFingerprintIDToString(id)
	}
}

// encodeUint64ToBytes returns the []byte representation of a uint64 value.
func encodeUint64ToBytes(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}

// encodeStmtFingerprintIDToString returns the hex string representation of a
// statement fingerprint ID.
func encodeStmtFingerprintIDToString(id uint64) string {
	return hex.EncodeToString(encodeUint64ToBytes(id))
}

// GetGlobalSampler returns the process-wide ASH sampler, or nil if it
// has not been initialized. Use this when you need to call
// Sampler.GetSamples with a reusable buffer to avoid allocations.
func GetGlobalSampler() *Sampler {
	return globalSampler.Load()
}

// GetSamples returns all samples from the global ASH sampler. Returns
// nil if the global sampler has not been initialized.
func GetSamples() []ASHSample {
	s := globalSampler.Load()
	if s == nil {
		return nil
	}
	return s.GetSamples(nil)
}
