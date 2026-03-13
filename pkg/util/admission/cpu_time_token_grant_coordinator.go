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
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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

// cpuTimeTokenACKillSwitch is an env var kill switch that disables CPU time
// token AC regardless of the cluster setting. This is useful when SQL is
// unavailable, preventing the cluster setting from being changed.
var cpuTimeTokenACKillSwitch = envutil.EnvOrDefaultBool(
	"COCKROACH_DISABLE_CPU_TIME_TOKEN_AC", false)

// cpuTimeTokenACIsEnabled returns true if CPU time token AC is enabled. It
// checks both the cluster setting and the env var kill switch. The kill switch
// takes precedence over the cluster setting.
func cpuTimeTokenACIsEnabled(sv *settings.Values) bool {
	return !cpuTimeTokenACKillSwitch && cpuTimeTokenACEnabled.Get(sv)
}

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
// admission.cpu_time_tokens.enabled is true, it returns the single WorkQueue
// that implements CPU time token AC. Else it returns a WorkQueue that does
// slots-based AC. The isSystemTenant parameter is kept for API compatibility
// but is ignored when CPU time token AC is enabled — all work goes through
// the same queue.
func (coord *CPUGrantCoordinators) GetKVWorkQueue(isSystemTenant bool) *WorkQueue {
	if !cpuTimeTokenACIsEnabled(&coord.st.SV) {
		return coord.slotsCoord.GetWorkQueue(KVWork)
	}
	return coord.cpuTimeCoord.getWorkQueue()
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

// ResourceGroupConfig holds per-resource-group configuration.
type ResourceGroupConfig struct {
	Weight        uint32
	BurstLimitFrac float64
}

// SetResourceGroupConfig sets per-resource-group weights and burst limits.
// Each resource group appears as a tenant with weight = CPU_MIN. The
// BurstLimitFrac controls burst qualification:
//   - >= 1.0: FULLY_UTILIZE (always canBurst)
//   - < 1.0: canBurst only while usage < burst limit fraction of CPU
func (coord *CPUGrantCoordinators) SetResourceGroupConfig(
	config map[uint64]ResourceGroupConfig,
) {
	weights := make(map[uint64]uint32, len(config))
	burstLimits := make(map[uint64]float64, len(config))
	for id, cfg := range config {
		weights[id] = cfg.Weight
		burstLimits[id] = cfg.BurstLimitFrac
	}
	coord.SetTenantWeights(weights)
	coord.cpuTimeCoord.getWorkQueue().SetBurstLimits(burstLimits)
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
	queue  requesterClose
}

func makeCPUTimeTokenGrantCoordinator(
	ambientCtx log.AmbientContext,
	opts Options,
	settings *cluster.Settings,
	registry *metric.Registry,
	knobs *TestingKnobs,
) *cpuTimeTokenGrantCoordinator {
	metrics := makeCPUTimeTokenMetrics()
	registry.AddMetricStruct(metrics)
	timeSource := timeutil.DefaultTimeSource{}
	granter := newCPUTimeTokenGranter(metrics, timeSource)
	filler := &cpuTimeTokenFiller{
		timeSource: timeSource,
		closeCh:    make(chan struct{}),
	}
	allocator := &cpuTimeTokenAllocator{
		granter:  granter,
		settings: settings,
		metrics:  metrics,
	}
	model := &cpuTimeTokenLinearModel{
		granter:            granter,
		cpuMetricsProvider: opts.CPUMetricsProvider,
		timeSource:         timeSource,
		metrics:            metrics,
	}
	allocator.model = model
	filler.allocator = allocator

	wqMetrics := makeWorkQueueMetrics("cpu", registry)
	wqOpts := makeWorkQueueOptions(KVWork)
	wqOpts.mode = usesCPUTimeTokens
	wqOpts.admittedCountPerTenant = metrics.AdmittedCountPerTenant
	wqOpts.waitTimeNanosPerTenant = metrics.WaitTimeNanosPerTenant
	req := makeWorkQueue(
		ambientCtx, KVWork, granter, settings, wqMetrics, wqOpts)
	granter.requester = req
	// This type assertion is always valid, since makeWorkQueue always
	// returns a *WorkQueue.
	allocator.queue = req.(*WorkQueue)

	coordinator := &cpuTimeTokenGrantCoordinator{
		filler: filler,
		queue:  req,
	}

	// The filler ticking appears to have a slight negative impact on perf.
	// For now, we accept this, since CPU time token AC will be off by
	// default, and only enabled in Serverless. To track fixing the perf
	// issue, we have the following ticket:
	// https://github.com/cockroachdb/cockroach/issues/161945
	if !knobs.DisableCPUTimeTokenFillerGoroutine {
		var once sync.Once
		if cpuTimeTokenACIsEnabled(&settings.SV) {
			once.Do(func() {
				filler.start(ambientCtx.AnnotateCtx(context.Background()))
			})
		}
		cpuTimeTokenACEnabled.SetOnChange(&settings.SV, func(ctx context.Context) {
			if cpuTimeTokenACIsEnabled(&settings.SV) {
				once.Do(func() {
					filler.start(ambientCtx.AnnotateCtx(context.Background()))
				})
			}
		})
	}

	return coordinator
}

func (coord *cpuTimeTokenGrantCoordinator) getWorkQueue() *WorkQueue {
	return coord.queue.(*WorkQueue)
}

func (coord *cpuTimeTokenGrantCoordinator) setTenantWeights(weights map[uint64]uint32) {
	coord.queue.(*WorkQueue).SetTenantWeights(weights)
}

func (coord *cpuTimeTokenGrantCoordinator) close() {
	coord.queue.close()
	coord.filler.close()
}
