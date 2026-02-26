// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Enabled controls whether ASH sampling is active. This is a
// system-level setting because the sampler is a process-wide
// singleton.
var Enabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"sql.ash.enabled",
	"enable active session history sampling",
	false,
)

// enabled caches the value of the sql.ash.enabled cluster setting so
// that callers of SetWorkState do not need to pass in cluster settings.
var enabled atomic.Bool

// SampleInterval controls how often ASH samples are taken. This is a
// system-level setting because the sampler is a process-wide
// singleton.
var SampleInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"sql.ash.sample_interval",
	"interval between ASH samples",
	time.Second,
	settings.PositiveDuration,
)

// BufferSize controls the maximum number of ASH samples retained in
// memory. This is a system-level setting because the sampler is a
// process-wide singleton.
var BufferSize = settings.RegisterIntSetting(
	settings.SystemVisible,
	"sql.ash.buffer_size",
	"number of ASH samples to retain in memory",
	1_000_000,
	settings.PositiveInt,
)

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
	gid   int64
	state WorkState
}

// Sampler periodically samples the active work states and stores ASH data.
type Sampler struct {
	nodeID  roachpb.NodeID
	st      *cluster.Settings
	buffer  *RingBuffer
	stopper *stop.Stopper
	ctx     context.Context
	// workloadIDCache caches the result of EncodeStmtFingerprintIDToString
	// to avoid repeated allocations for the same workload ID.
	workloadIDCache *cache.UnorderedCache
	// pendingSamples is a reusable slice for collecting work state snapshots
	// during RangeWorkStates. Reused across samples to avoid per-sample
	// slice allocation.
	pendingSamples []pendingSample
}

// NewSampler creates a new ASH sampler.
func NewSampler(nodeID roachpb.NodeID, st *cluster.Settings, stopper *stop.Stopper) *Sampler {
	bufSize := int(BufferSize.Get(&st.SV))
	return &Sampler{
		nodeID:  nodeID,
		st:      st,
		buffer:  NewRingBuffer(bufSize),
		stopper: stopper,
		workloadIDCache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(size int, key, value interface{}) bool {
				return size > maxWorkloadIDCacheSize
			},
		}),
	}
}

// Start begins the background sampling loop.
func (s *Sampler) Start(ctx context.Context) error {
	log.Ops.Info(ctx, "starting ASH sampler")
	return s.stopper.RunAsyncTask(ctx, "ash-sampler", func(ctx context.Context) {
		s.run(ctx)
	})
}

func (s *Sampler) run(ctx context.Context) {
	s.ctx = ctx
	enabled.Store(Enabled.Get(&s.st.SV))
	Enabled.SetOnChange(&s.st.SV, func(ctx context.Context) {
		enabled.Store(Enabled.Get(&s.st.SV))
	})

	BufferSize.SetOnChange(&s.st.SV, func(ctx context.Context) {
		newSize := int(BufferSize.Get(&s.st.SV))
		s.buffer.Resize(newSize)
	})

	var timer timeutil.Timer
	defer timer.Stop()

	var interval atomic.Int64
	interval.Store(int64(SampleInterval.Get(&s.st.SV)))
	timer.Reset(time.Duration(interval.Load()))

	SampleInterval.SetOnChange(&s.st.SV, func(ctx context.Context) {
		interval.Store(int64(SampleInterval.Get(&s.st.SV)))
	})

	for {
		select {
		case <-timer.C:
			timer.Read = true
			timer.Reset(time.Duration(interval.Load()))
			if Enabled.Get(&s.st.SV) {
				s.takeSample()
			}
		case <-s.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

// takeSample captures a snapshot of all active work states.
func (s *Sampler) takeSample() {
	sampleTime := timeutil.Now()

	// Collect work states and then retire them so they can be reused.
	s.pendingSamples = s.pendingSamples[:0]
	RangeWorkStates(func(gid int64, state WorkState) bool {
		s.pendingSamples = append(s.pendingSamples, pendingSample{gid: gid, state: state})
		return true
	})
	reclaimRetiredWorkStates()

	// Process collected samples.
	for i := range s.pendingSamples {
		ps := &s.pendingSamples[i]

		// Encode the workloadID to a string for the ASHSample.
		var workloadIDStr string
		if ps.state.WorkloadID != 0 {
			if cached, ok := s.workloadIDCache.Get(ps.state.WorkloadID); ok {
				workloadIDStr = cached.(string)
			} else {
				workloadIDStr = EncodeStmtFingerprintIDToString(ps.state.WorkloadID)
				s.workloadIDCache.Add(ps.state.WorkloadID, workloadIDStr)
			}
		}

		sample := ASHSample{
			SampleTime:    sampleTime,
			NodeID:        s.nodeID,
			TenantID:      ps.state.TenantID,
			WorkloadID:    workloadIDStr,
			WorkEventType: ps.state.WorkEventType,
			WorkEvent:     ps.state.WorkEvent,
			GoroutineID:   ps.gid,
		}
		s.buffer.Add(sample)
	}
}

// GetSamples returns all samples currently in the Sampler's buffer.
func (s *Sampler) GetSamples() []ASHSample {
	return s.buffer.GetAll()
}

// InitGlobalSampler creates and starts the process-wide ASH sampler.
// It is idempotent: only the first call creates the sampler; subsequent
// calls are no-ops. This should be called during process-level server
// initialization (not per-tenant), after the node ID is known.
func InitGlobalSampler(
	ctx context.Context, nodeID roachpb.NodeID, st *cluster.Settings, stopper *stop.Stopper,
) error {
	var initErr error
	initSamplerOnce.Do(func() {
		s := NewSampler(nodeID, st, stopper)
		initErr = s.Start(ctx)
		if initErr == nil {
			globalSampler.Store(s)
		}
	})
	return initErr
}

// GetSamples returns all samples from the global ASH sampler. Returns
// nil if the global sampler has not been initialized.
func GetSamples() []ASHSample {
	s := globalSampler.Load()
	if s == nil {
		return nil
	}
	return s.GetSamples()
}
