// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var cpuTimeTokenACEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.cpu_time_tokens.enabled",
	"if true, CPU time token AC will be used for foreground KVWork, instead of slots-based AC -- "+
		"note that this is not supported in production except on multi-tenant Serverless clusters",
	false)

// CPUGrantCoordinators's main purpose is to act as a shim. Depending on
// whether admission.cpu_time_tokens.enabled is true or false, a WorkQueue
// that does slot-based or CPU time token AC is returned from
// GetKVWorkQueue. This way, we support both, without requiring a process
// restart.
type CPUGrantCoordinators struct {
	st           *cluster.Settings
	slotsCoord   *GrantCoordinator
	cpuTimeCoord *cpuTimeTokenGrantCoordinator
}

// GetKVWorkQueue returns a WorkQueue to use for KVWork. If
// admission.cpu_time_tokens.enabled is true, it returns a WorkQueue that
// implements CPU time token AC. Else it returns a WorkQueue that does
// slots-based AC. If CPU time token AC, there is one WorkQueue for system
// tenant work and another for app tenant work. The system tenant WorkQueue
// is backed by a granter that allows greater resource usage than the app
// tenant WorkQueue. This is a prioritization scheme. For details regarding
// the granters, see cpu_time_token_granter.go.
func (coord *CPUGrantCoordinators) GetKVWorkQueue(isSystemTenant bool) *WorkQueue {
	if !cpuTimeTokenACEnabled.Get(&coord.st.SV) {
		return coord.slotsCoord.GetWorkQueue(KVWork)
	}
	if isSystemTenant {
		return coord.cpuTimeCoord.getWorkQueue(systemTenant)
	}
	return coord.cpuTimeCoord.getWorkQueue(appTenant)
}

// GetSQLWorkQueue returns a WorkQueue for SQLKVResponseWork or
// SQLSQLResponseWork. If any other queue is requested from this function,
// it panics.
func (coord *CPUGrantCoordinators) GetSQLWorkQueue(workKind WorkKind) *WorkQueue {
	if workKind != SQLKVResponseWork && workKind != SQLSQLResponseWork {
		panic(fmt.Sprintf("workKind %q not supported by GetSQLWorkQueue", workKind))
	}
	return coord.slotsCoord.queues[workKind].(*WorkQueue)
}

// SetTenantWeights sets the weight of tenants, using the provided tenant ID
// => weight map. A nil map will result in all tenants having the same weight.
// SetTenantWeights adjusts the weights on all WorkQueues that
// CPUGrantCoordinators manages.
func (coord *CPUGrantCoordinators) SetTenantWeights(weights map[uint64]uint32) {
	coord.slotsCoord.GetWorkQueue(KVWork).SetTenantWeights(weights)
	coord.cpuTimeCoord.setTenantWeights(weights)
}

// GetRunnableCountCallback returns a callback of type
// goschedstats.RunnableCountCallback.
func (coord *CPUGrantCoordinators) GetRunnableCountCallback() goschedstats.RunnableCountCallback {
	return coord.slotsCoord.CPULoad
}

// Close implements the stop.Closer interface.
func (cg *CPUGrantCoordinators) Close() {
	cg.slotsCoord.Close()
	cg.cpuTimeCoord.close()
}

type cpuTimeTokenGrantCoordinator struct {
	filler *cpuTimeTokenFiller
	queues [numResourceTiers]requesterClose
}

func makeCPUTimeTokenGrantCoordinator(
	ambientCtx log.AmbientContext,
	opts Options,
	settings *cluster.Settings,
	registry *metric.Registry,
	knobs *TestingKnobs,
) *cpuTimeTokenGrantCoordinator {
	granter := &cpuTimeTokenGranter{}
	var childGranters [numResourceTiers]cpuTimeTokenChildGranter
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		childGranters[tier] = cpuTimeTokenChildGranter{
			tier:   tier,
			parent: granter,
		}
	}
	timeSource := timeutil.DefaultTimeSource{}
	filler := &cpuTimeTokenFiller{
		timeSource: timeSource,
		closeCh:    make(chan struct{}),
	}
	allocator := &cpuTimeTokenAllocator{
		granter:  granter,
		settings: settings,
	}
	model := &cpuTimeTokenLinearModel{
		granter:            granter,
		cpuMetricsProvider: opts.CPUMetricsProvider,
		timeSource:         timeSource,
	}
	allocator.model = model
	filler.allocator = allocator

	var requesters [numResourceTiers]requester
	wqMetrics := makeWorkQueueMetrics("cpu", registry)
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		opts := makeWorkQueueOptions(KVWork)
		opts.mode = usesCPUTimeTokens
		requesters[tier] = makeWorkQueue(
			ambientCtx, KVWork, &childGranters[tier], settings, wqMetrics, opts)
		granter.requester[tier] = requesters[tier]
		// This type assertion is always valid, since makeWorkQueue always
		// returns a *WorkQueue.
		allocator.queues[tier] = requesters[tier].(*WorkQueue)
	}

	coordinator := &cpuTimeTokenGrantCoordinator{
		filler: filler,
	}
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		coordinator.queues[tier] = requesters[tier]
	}

	// The filler ticking appears to have a slight negative impact on perf.
	// For now, we accept this, since CPU time token AC will be off by
	// default, and only enabled in Serverless. To track fixing the perf
	// issue, we have the following ticket:
	// https://github.com/cockroachdb/cockroach/issues/161945
	if !knobs.DisableCPUTimeTokenFillerGoroutine {
		var once sync.Once
		if cpuTimeTokenACEnabled.Get(&settings.SV) {
			once.Do(func() {
				filler.start(ambientCtx.AnnotateCtx(context.Background()))
			})
		}
		cpuTimeTokenACEnabled.SetOnChange(&settings.SV, func(ctx context.Context) {
			if cpuTimeTokenACEnabled.Get(&settings.SV) {
				once.Do(func() {
					filler.start(ambientCtx.AnnotateCtx(context.Background()))
				})
			}
		})
	}

	return coordinator
}

func (coord *cpuTimeTokenGrantCoordinator) getWorkQueue(tier resourceTier) *WorkQueue {
	return coord.queues[tier].(*WorkQueue)
}

func (coord *cpuTimeTokenGrantCoordinator) setTenantWeights(weights map[uint64]uint32) {
	for tier := range coord.queues {
		coord.queues[tier].(*WorkQueue).SetTenantWeights(weights)
	}
}

func (coord *cpuTimeTokenGrantCoordinator) close() {
	for tier := range coord.queues {
		coord.queues[tier].close()
	}
	coord.filler.close()
}
