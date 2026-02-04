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

// Sampler periodically samples the active work states and stores ASH data.
type Sampler struct {
	nodeID           roachpb.NodeID
	st               *cluster.Settings
	buffer           *RingBuffer
	stopper          *stop.Stopper
	resolveAppNameID ResolveAppNameID
	ctx              context.Context
}

// NewSampler creates a new ASH sampler.
func NewSampler(
	nodeID roachpb.NodeID, st *cluster.Settings, stopper *stop.Stopper,
) *Sampler {
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
	}
}

// Start begins the background sampling loop.
func (s *Sampler) Start(ctx context.Context) error {
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

	interval := SampleInterval.Get(&s.st.SV)
	timer.Reset(interval)

	SampleInterval.SetOnChange(&s.st.SV, func(ctx context.Context) {
		interval = SampleInterval.Get(&s.st.SV)
	})

	for {
		select {
		case <-timer.C:
			timer.Read = true
			timer.Reset(interval)
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

	// Iterate over all registered work states and create samples.
	RangeWorkStates(func(gid int64, state WorkState) bool {
		// Encode the workloadID to a string for the ASHSample.
		// First check if it's a known system workload ID, otherwise encode as hex.
		var workloadIDStr string
		switch state.WorkloadID {
		case 0:
			// No workload ID.
		default:
			// Check if this is a known system workload ID (e.g., TXN_HEARTBEAT).
			if name, ok := LookupSystemWorkloadName(state.WorkloadID); ok {
				workloadIDStr = name
			} else {
				// TODO(alyshan): Cache these mappings to reduce allocations.
				// TODO(alyshan): Use StringInterner(?).
				workloadIDStr = EncodeStmtFingerprintIDToString(state.WorkloadID)
			}
		}

		// Look up app_name from AppNameID if present.
		var appName string
		if state.AppNameID != 0 {
			var found bool
			appName, found = GetAppName(state.AppNameID)
			if !found {
				appName = "unknown"
				if state.GatewayNodeID != 0 && s.resolveAppNameID != nil {
					// Fetch all mappings from the gateway node and store them locally.
					log.Ops.Infof(s.ctx, "ASH sampler: fetching app name mappings from gateway node %d", state.GatewayNodeID)
					resp, err := s.resolveAppNameID(s.ctx, &AppNameMappingsRequest{
						NodeID: state.GatewayNodeID,
					})
					if err == nil {
						StoreAppNameMappings(resp.Mappings)
						// Try to get the app name again after storing mappings.
						appName, found = GetAppName(state.AppNameID)
						if found {
							log.Ops.Info(s.ctx, "ASH sampler: successfully resolved app name from gateway node")
						}
					} else {
						log.Ops.Infof(s.ctx, "ASH sampler: failed to resolve app name from gateway node: %v", err)
					}
				}
			}
		}

		sample := ASHSample{
			SampleTime:    sampleTime,
			NodeID:        s.nodeID,
			WorkloadID:    workloadIDStr,
			WorkEventType: state.WorkEventType,
			WorkEvent:     state.WorkEvent,
			GoroutineID:   gid,
			AppName:       appName,
			GatewayNodeID: state.GatewayNodeID,
		}
		s.buffer.Add(sample)
		return true // continue iteration
	})

	// TODO(alyshan): Add metrics for sample count, processing time, etc.
}

// GetSamples returns all samples currently in the buffer.
func (s *Sampler) GetSamples() []ASHSample {
	return s.buffer.GetAll()
}
