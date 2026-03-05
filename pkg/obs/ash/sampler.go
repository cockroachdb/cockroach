// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
)

// AppNameResolverFn fetches app name ID-to-string mappings from a
// remote node. The sampler calls this when local resolution fails and
// the work state has a non-zero GatewayNodeID that differs from the
// local node. Only the specified IDs are requested; the returned map
// contains entries for the subset that the remote node could resolve.
type AppNameResolverFn func(ctx context.Context, nodeID roachpb.NodeID, ids []uint64) (map[uint64]string, error)

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
	appName       string
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
	// workloadIDCache caches the result of encodeStmtFingerprintIDToString
	// to avoid repeated allocations for the same workload ID.
	workloadIDCache *cache.UnorderedCache
	// resolver, when set, fetches app name mappings from remote nodes
	// for work states whose app name could not be resolved locally.
	resolver atomic.Pointer[AppNameResolverFn]
	// pendingSamples is a reusable slice for collecting work state snapshots
	// during rangeWorkStates. Reused across samples to avoid per-sample
	// slice allocation.
	pendingSamples []pendingSample
}

// newSampler creates a new ASH sampler.
func newSampler(nodeID roachpb.NodeID, st *cluster.Settings, stopper *stop.Stopper) *Sampler {
	bufSize := int(BufferSize.Get(&st.SV))
	return &Sampler{
		nodeID:  nodeID,
		st:      st,
		buffer:  NewRingBuffer(bufSize),
		stopper: stopper,
		metrics: makeMetrics(),
		workloadIDCache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(size int, key, value interface{}) bool {
				return size > maxWorkloadIDCacheSize
			},
		}),
	}
}

// SetAppNameResolver sets the callback used to fetch app name
// mappings from remote nodes when local resolution fails.
func (s *Sampler) SetAppNameResolver(fn AppNameResolverFn) {
	s.resolver.Store(&fn)
}

// SetGlobalAppNameResolver sets the resolver on the global sampler.
// This is a no-op if the global sampler has not been initialized.
func SetGlobalAppNameResolver(fn AppNameResolverFn) {
	s := globalSampler.Load()
	if s == nil {
		return
	}
	s.SetAppNameResolver(fn)
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
			timer.Read = true
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

	// Collect work states. rangeWorkStates reclaims retired states after
	// iteration so pooled objects can be reused.
	s.pendingSamples = s.pendingSamples[:0]
	rangeWorkStates(func(gid int64, state WorkState) bool {
		s.pendingSamples = append(s.pendingSamples, pendingSample{gid: gid, state: state})
		return true
	})

	// First pass: resolve app names and workload IDs, tracking
	// indices where local app name resolution fails.
	var unresolvedIndices []int
	for i := range s.pendingSamples {
		ps := &s.pendingSamples[i]

		// Encode the workloadID to a string for the ASHSample.
		// Note(alyshan): Consider encoding at read time.
		if ps.state.WorkloadInfo.WorkloadID != 0 {
			if cached, ok := s.workloadIDCache.Get(ps.state.WorkloadInfo.WorkloadID); ok {
				ps.workloadIDStr = cached.(string)
			} else {
				ps.workloadIDStr = encodeStmtFingerprintIDToString(ps.state.WorkloadInfo.WorkloadID)
				s.workloadIDCache.Add(ps.state.WorkloadInfo.WorkloadID, ps.workloadIDStr)
			}
		}

		// Resolve app name ID to string via the node-local cache.
		if ps.state.WorkloadInfo.AppNameID != 0 {
			if name, ok := GetAppName(ps.state.WorkloadInfo.AppNameID); ok {
				ps.appName = name
			} else {
				unresolvedIndices = append(unresolvedIndices, i)
			}
		}
	}

	// If there are unresolved app names, try fetching from remote
	// gateway nodes. Group by gateway node ID to make at most one
	// RPC per remote node.
	if len(unresolvedIndices) > 0 {
		s.resolveRemoteAppNames(ctx, unresolvedIndices)
	}

	// Emit samples.
	for i := range s.pendingSamples {
		ps := &s.pendingSamples[i]
		sample := ASHSample{
			SampleTime:    sampleTime,
			NodeID:        s.nodeID,
			TenantID:      ps.state.TenantID,
			WorkloadID:    ps.workloadIDStr,
			AppName:       ps.appName,
			WorkEventType: ps.state.WorkEventType,
			WorkEvent:     ps.state.WorkEvent,
			GoroutineID:   ps.gid,
		}
		s.buffer.Add(sample)
	}
	s.metrics.SamplesCollected.Inc(int64(len(s.pendingSamples)))
}

// resolveRemoteAppNames fetches app name mappings from remote gateway
// nodes for samples that could not be resolved from the local cache.
// It deduplicates RPCs by gateway node ID, groups the needed app name
// IDs per node, and skips the local node (whose cache was already
// consulted).
func (s *Sampler) resolveRemoteAppNames(ctx context.Context, unresolvedIndices []int) {
	resolverPtr := s.resolver.Load()
	if resolverPtr == nil {
		return
	}
	resolver := *resolverPtr

	// Group unresolved app name IDs by gateway node, skipping node
	// ID 0 (unknown) and the sampler's own node. Use a map of maps
	// to deduplicate IDs within each gateway node.
	//
	// For separate-process SQL pods, GatewayNodeID in the
	// BatchRequest is 0 (see kvpb.Header.GatewayNodeID), so no
	// remote resolution is attempted. This is intentional: KV nodes
	// cannot dial SQL pods, and out-of-process tenants only see
	// their own SQL-side samples where app names resolve locally.
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
		ids[ps.state.WorkloadInfo.AppNameID] = struct{}{}
	}

	// Fetch mappings from each unique gateway node and store them
	// in the local cache. Use a short per-node timeout so that a
	// slow or unreachable node doesn't stall resolution of the
	// remaining nodes.
	for nodeID, ids := range gatewayIDs {
		idSlice := make([]uint64, 0, len(ids))
		for id := range ids {
			idSlice = append(idSlice, id)
		}
		if err := timeutil.RunWithTimeout(
			ctx, "ash-resolve-app-names", 250*time.Millisecond,
			func(resolveCtx context.Context) error {
				mappings, err := resolver(resolveCtx, nodeID, idSlice)
				if err != nil {
					return err
				}
				for id, name := range mappings {
					StoreAppNameMapping(id, name)
				}
				return nil
			},
		); err != nil {
			log.Ops.Warningf(
				ctx, "ASH: failed to resolve app name mappings from n%d: %v",
				nodeID, err,
			)
		}
	}

	// Re-resolve the previously-unresolved samples from the
	// now-populated local cache.
	for _, idx := range unresolvedIndices {
		ps := &s.pendingSamples[idx]
		ps.appName, _ = GetAppName(ps.state.WorkloadInfo.AppNameID)
	}
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

// GlobalSamplerMetrics returns the Metrics for the process-wide ASH
// sampler, or nil if the sampler has not been initialized yet.
func GlobalSamplerMetrics() *Metrics {
	if s := globalSampler.Load(); s != nil {
		return &s.metrics
	}
	return nil
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

// GetSamples returns all samples from the global ASH sampler. Returns
// nil if the global sampler has not been initialized.
func GetSamples() []ASHSample {
	s := globalSampler.Load()
	if s == nil {
		return nil
	}
	return s.GetSamples(nil)
}
