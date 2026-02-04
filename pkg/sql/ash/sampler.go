// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Enabled controls whether ASH sampling is active.
var Enabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.ash.enabled",
	"enable active session history sampling",
	false,
)

// enabled caches the value of the sql.ash.enabled cluster setting so
// that callers of SetWorkState do not need to pass in cluster settings.
var enabled atomic.Bool

// SampleInterval controls how often ASH samples are taken.
var SampleInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.ash.sample_interval",
	"interval between ASH samples",
	time.Second,
	settings.PositiveDuration,
)

// BufferSize controls the maximum number of ASH samples retained in memory.
var BufferSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.ash.buffer_size",
	"number of ASH samples to retain in memory",
	1_000_000,
	settings.PositiveInt,
)

// pendingSample holds a snapshot of a goroutine's work state collected
// during RangeWorkStates, for processing after the lock is released.
type pendingSample struct {
	gid   int64
	state WorkState
}

// Sampler periodically samples the active work states and stores ASH data.
type Sampler struct {
	nodeID           roachpb.NodeID
	st               *cluster.Settings
	buffer           *RingBuffer
	stopper          *stop.Stopper
	resolveAppNameID ResolveAppNameID
	ctx              context.Context
	// workloadIDCache caches the result of EncodeStmtFingerprintIDToString
	// to avoid repeated allocations for the same workload ID. Only accessed
	// from the single sampler goroutine, so no synchronization is needed.
	workloadIDCache map[uint64]string
	// pendingSamples is a reusable slice for collecting work state snapshots
	// during RangeWorkStates. Reused across samples to avoid per-sample
	// slice allocation. Only accessed from the single sampler goroutine.
	pendingSamples []pendingSample
}

// NewSampler creates a new ASH sampler.
func NewSampler(nodeID roachpb.NodeID, st *cluster.Settings, stopper *stop.Stopper) *Sampler {
	return NewSamplerWithAppNameFetcher(nodeID, st, stopper, nil)
}

// NewSamplerWithAppNameFetcher creates a new ASH sampler with an optional
// function to fetch app name mappings from remote nodes.
func NewSamplerWithAppNameFetcher(
	nodeID roachpb.NodeID,
	st *cluster.Settings,
	stopper *stop.Stopper,
	resolveAppNameID ResolveAppNameID,
) *Sampler {
	bufSize := int(BufferSize.Get(&st.SV))
	return &Sampler{
		nodeID:           nodeID,
		st:               st,
		buffer:           NewRingBuffer(bufSize),
		stopper:          stopper,
		resolveAppNameID: resolveAppNameID,
		workloadIDCache:  make(map[uint64]string),
	}
}

// Start begins the background sampling loop.
func (s *Sampler) Start(ctx context.Context) error {
	log.Ops.Info(ctx, "Starting ASH sampler")
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

	// Collect work state snapshots during RangeWorkStates, then process
	// them afterwards so that app name resolution RPCs and buffer
	// writes don't hold iteration open.
	s.pendingSamples = s.pendingSamples[:0]
	RangeWorkStates(func(gid int64, state WorkState) bool {
		s.pendingSamples = append(s.pendingSamples, pendingSample{gid: gid, state: state})
		return true
	})

	// Return retired WorkState objects to the pool now that Range is
	// complete and the sampler no longer holds any stale pointers.
	reclaimRetiredWorkStates()

	// Process collected snapshots outside the iteration so that app
	// name resolution RPCs and buffer writes don't block new work.
	for i := range s.pendingSamples {
		ps := &s.pendingSamples[i]

		// Encode the workloadID to a string for the ASHSample.
		var workloadIDStr string
		switch ps.state.WorkloadID {
		case 0:
			// No workload ID.
		default:
			if name, ok := LookupSystemWorkloadName(ps.state.WorkloadID); ok {
				workloadIDStr = name
			} else {
				if cached, ok := s.workloadIDCache[ps.state.WorkloadID]; ok {
					workloadIDStr = cached
				} else {
					workloadIDStr = EncodeStmtFingerprintIDToString(ps.state.WorkloadID)
					s.workloadIDCache[ps.state.WorkloadID] = workloadIDStr
				}
			}
		}

		// Look up app_name from AppNameID if present.
		var appName string
		if ps.state.AppNameID != 0 {
			var found bool
			appName, found = GetAppName(ps.state.AppNameID)
			if !found {
				appName = "unknown"
				if ps.state.GatewayNodeID != 0 && s.resolveAppNameID != nil {
					resp, err := s.resolveAppNameID(s.ctx, &AppNameMappingsRequest{
						NodeID: ps.state.GatewayNodeID,
					})
					if err == nil {
						StoreAppNameMappings(resp.Mappings)
						appName, found = GetAppName(ps.state.AppNameID)
						if found {
							// Successfully resolved.
						}
					}
				}
			}
		}

		sample := ASHSample{
			SampleTime:    sampleTime,
			NodeID:        s.nodeID,
			WorkloadID:    workloadIDStr,
			WorkEventType: ps.state.WorkEventType,
			WorkEvent:     ps.state.WorkEvent,
			GoroutineID:   ps.gid,
			AppName:       appName,
			GatewayNodeID: ps.state.GatewayNodeID,
		}
		s.buffer.Add(sample)
	}

	// TODO(alyshan): Add metrics for sample count, processing time, etc.
}

// GetSamples returns all samples currently in the buffer.
func (s *Sampler) GetSamples() []ASHSample {
	return s.buffer.GetAll()
}
