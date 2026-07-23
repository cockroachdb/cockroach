// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/crtime"
)

// cpuTimeTokenACEnabled is the legacy bool setting for enabling CPU time
// token AC. Deprecated in favor of cpuTimeTokenACMode. Kept registered
// so that clusters upgrading from older versions (where this was set to
// true) continue to function. The cpuTimeTokenACIsEnabled helper checks
// cpuTimeTokenACMode first and falls back to this setting. This setting
// will be retired in 26.4 once all clusters have migrated to the new
// mode setting.
var cpuTimeTokenACEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.cpu_time_tokens.enabled",
	"if true, CPU time token AC will be used for foreground KVWork, instead of slots-based AC -- "+
		"deprecated in favor of admission.cpu_time_tokens.mode",
	false)

// cpuTimeTokenMode selects between off (slot-based AC), Serverless
// (per-tenant groups), and Resource Manager (priority-based groups)
// modes. All modes use a single WorkQueue with 2 buckets.
type cpuTimeTokenMode int64

const (
	// offMode disables CPU time token AC; slot-based AC is used instead.
	// When the mode is off, the legacy bool setting is checked as a
	// fallback.
	offMode cpuTimeTokenMode = iota
	// serverlessMode uses CPU time token AC with a single WorkQueue and
	// 2 buckets (canBurst, noBurst).
	serverlessMode
	// resourceManagerMode uses 1 WorkQueue with N resource groups,
	// a single utilization target, and 2 buckets (canBurst, noBurst).
	resourceManagerMode
)

// cpuTimeTokenACMode selects the CPU time token admission control
// mode. Can be changed at runtime without a restart. When set to off,
// the legacy admission.cpu_time_tokens.enabled bool is checked as a
// fallback for backward compatibility.
//
// This is ApplicationLevel to match the legacy cpuTimeTokenACEnabled
// bool that it replaces. The class should be revisited when the
// legacy bool is retired.
var cpuTimeTokenACMode = settings.RegisterEnumSetting[cpuTimeTokenMode](
	settings.ApplicationLevel,
	"admission.cpu_time_tokens.mode",
	"selects the CPU time token admission control mode: off uses "+
		"slot-based AC (or falls back to the legacy enabled bool), "+
		"serverless uses CPU time token AC with per-tenant groups, "+
		"resource_manager uses CPU time token AC with priority-based groups",
	"off",
	map[cpuTimeTokenMode]string{
		offMode:             "off",
		serverlessMode:      "serverless",
		resourceManagerMode: "resource_manager",
	},
)

// cpuTimeTokenACKillSwitch is an env var kill switch that disables CPU time
// token AC regardless of the cluster setting. This is useful when SQL is
// unavailable, preventing the cluster setting from being changed.
var cpuTimeTokenACKillSwitch = envutil.EnvOrDefaultBool(
	"COCKROACH_DISABLE_CPU_TIME_TOKEN_AC", false)

// cpuTimeTokenACIsEnabled returns true if CPU time token AC is enabled.
// It checks cpuTimeTokenACMode first; if that is off, it falls back
// to the legacy cpuTimeTokenACEnabled bool for backward compatibility.
// The env var kill switch takes precedence over both settings.
func cpuTimeTokenACIsEnabled(sv *settings.Values) bool {
	if cpuTimeTokenACKillSwitch {
		return false
	}
	if cpuTimeTokenACMode.Get(sv) != offMode {
		return true
	}
	return cpuTimeTokenACEnabled.Get(sv)
}

var sqlCPUTimeTokenACEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.sql_cpu_time_tokens.enabled",
	"when true, SQL CPU usage is admitted through the same CPU time token "+
		"budget as KV work; has no effect unless CPU time token AC is enabled "+
		"via admission.cpu_time_tokens.mode or the legacy enabled setting",
	false,
)

// sqlCPUTimeTokenACIsEnabled returns true if SQL CPU usage is admitted
// through the same CPU time token AC as KV work. It has no effect unless
// CPU time token AC is enabled.
func sqlCPUTimeTokenACIsEnabled(sv *settings.Values) bool {
	return cpuTimeTokenACIsEnabled(sv) && sqlCPUTimeTokenACEnabled.Get(sv)
}

// CPUGrantCoordinators acts as a shim. Depending on
// admission.cpu_time_tokens.mode (off, serverless, resource_manager),
// a WorkQueue that does slot-based or CPU time token AC is returned
// from GetKVWorkQueue. This way, we support both, without requiring a
// process restart.
type CPUGrantCoordinators struct {
	st           *cluster.Settings
	slotsCoord   *GrantCoordinator
	cpuTimeCoord *cpuTimeTokenGrantCoordinator
}

// GetKVWorkQueue returns a WorkQueue to use for KVWork. If CPU time
// token AC is enabled (via admission.cpu_time_tokens.mode or the legacy
// enabled bool), it returns the single CPU time token WorkQueue.
// Otherwise it returns a WorkQueue that does slots-based AC.
func (coord *CPUGrantCoordinators) GetKVWorkQueue() *WorkQueue {
	if !cpuTimeTokenACIsEnabled(&coord.st.SV) {
		return coord.slotsCoord.GetWorkQueue(KVWork)
	}
	return coord.cpuTimeCoord.queue
}

// GetCTTWorkQueue returns the CPU time token WorkQueue directly,
// bypassing the CTT-enabled check that GetKVWorkQueue performs. Callers
// that already know they want the CTT queue (e.g. the SQLCPUProvider
// fast path) use this to avoid the TOCTOU window where the setting can
// flip between the caller's check and GetKVWorkQueue's re-check —
// otherwise the caller can be handed the slot-based queue and silently
// run with the wrong admission policy.
func (coord *CPUGrantCoordinators) GetCTTWorkQueue() *WorkQueue {
	return coord.cpuTimeCoord.queue
}

// GetSQLWorkQueue returns the slot-based WorkQueue for SQLKVResponseWork
// or SQLSQLResponseWork; panics for any other WorkKind. Note this is
// only the slots path: when CPU time token AC is enabled SQL work
// instead flows through the CTT queue via SQLCPUProvider (see
// sql_cpu_handle.go).
func (coord *CPUGrantCoordinators) GetSQLWorkQueue(workKind WorkKind) *WorkQueue {
	if workKind != SQLKVResponseWork && workKind != SQLSQLResponseWork {
		panic(fmt.Sprintf("workKind %q not supported by GetSQLWorkQueue", workKind))
	}
	return coord.slotsCoord.queues[workKind].(*WorkQueue)
}

// SetResourceGroupConfig installs a new per-resource-group config (weight +
// maxCPU) into the holder, then signals the RM-mode WorkQueue to refresh its
// cached per-group state. When RM mode is off, the second step is a no-op:
// the change stays staged in the holder and is applied when the mode
// setting changes to resourceManagerMode.
func (coord *CPUGrantCoordinators) SetResourceGroupConfig(config ResourceGroupConfigSet) {
	coord.cpuTimeCoord.configHolder.Set(config)
	coord.cpuTimeCoord.queue.refreshResourceGroupConfig()
}

// GetRunnableCountCallback returns a callback of type
// goschedstats.RunnableCountCallback. The callback fans out to both
// the slot-based coordinator (which adjusts slots and records period
// duration metrics) and the CPU time token allocator (which adjusts
// its dampening factor). The slot coordinator is called first so its
// KVCPULoadShortPeriodDuration / KVCPULoadLongPeriodDuration metrics
// are incremented before the token allocator reads them.
func (coord *CPUGrantCoordinators) GetRunnableCountCallback() goschedstats.RunnableCountCallback {
	return func(runnable int, procs int, samplePeriod time.Duration) {
		coord.slotsCoord.CPULoad(runnable, procs, samplePeriod)
		coord.cpuTimeCoord.allocator.CPULoad(runnable, procs, samplePeriod)
	}
}

// Close implements the stop.Closer interface.
func (cg *CPUGrantCoordinators) Close() {
	cg.slotsCoord.Close()
	cg.cpuTimeCoord.close()
}

type cpuTimeTokenGrantCoordinator struct {
	filler       *cpuTimeTokenFiller
	allocator    *cpuTimeTokenAllocator
	queue        *WorkQueue
	configHolder *ResourceGroupConfigHolder
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
	configHolder := newResourceGroupConfigHolder(&settings.SV)
	allocator := &cpuTimeTokenAllocator{
		granter:         granter,
		settings:        settings,
		configHolder:    configHolder,
		metrics:         metrics,
		nowMono:         crtime.NowMono,
		dampeningFactor: 1.0,
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
	wqOpts.perGroupAggMetrics = &groupAggMetrics{
		primary: &groupAggMetricSet{
			admittedCount:  metrics.AdmittedCount,
			waitTimeNanos:  metrics.WaitTimeNanos,
			tokensUsed:     metrics.TokensUsed,
			tokensReturned: metrics.TokensReturned,
		},
		legacy: &groupAggMetricSet{
			admittedCount:  metrics.LegacyAdmittedCountPerTenant,
			waitTimeNanos:  metrics.LegacyWaitTimeNanosPerTenant,
			tokensUsed:     metrics.LegacyTokensUsedPerTenant,
			tokensReturned: metrics.LegacyTokensReturnedPerTenant,
		},
	}
	wqOpts.configHolder = configHolder
	wqOpts.groupKeyForWorkInfo = cpuTimeTokenGroupKeyForWorkInfo
	queue := makeWorkQueue(
		ambientCtx, KVWork, granter, settings, wqMetrics, wqOpts).(*WorkQueue)
	granter.requester = queue
	allocator.queue = queue

	coordinator := &cpuTimeTokenGrantCoordinator{
		filler:       filler,
		allocator:    allocator,
		queue:        queue,
		configHolder: configHolder,
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
		startIfEnabled := func(ctx context.Context) {
			if cpuTimeTokenACIsEnabled(&settings.SV) {
				once.Do(func() {
					filler.start(ambientCtx.AnnotateCtx(context.Background()))
				})
			}
		}
		cpuTimeTokenACMode.SetOnChange(&settings.SV, startIfEnabled)
		cpuTimeTokenACEnabled.SetOnChange(&settings.SV, startIfEnabled)
	}

	return coordinator
}

func (coord *cpuTimeTokenGrantCoordinator) close() {
	coord.queue.close()
	coord.filler.close()
}
