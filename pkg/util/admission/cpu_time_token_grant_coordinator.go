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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
// (2 WorkQueues, per-tier settings), and Resource Manager (1 WorkQueue,
// resource groups) modes.
type cpuTimeTokenMode int64

const (
	// offMode disables CPU time token AC; slot-based AC is used instead.
	// When the mode is off, the legacy bool setting is checked as a
	// fallback.
	offMode cpuTimeTokenMode = iota
	// serverlessMode uses 2 WorkQueues (systemTenant, appTenant), per-tier
	// utilization targets, and 4 buckets (2 tiers x 2 burst quals).
	serverlessMode
	// resourceManagerMode will use 1 WorkQueue with N resource groups,
	// a single utilization target, and 2 buckets (1 tier x 2 burst quals).
	// TODO(wenyihu): In RM mode, only queue[0] should receive work;
	// queue[1] will sit idle. This routing is not yet implemented.
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
		"serverless uses 2 queues with per-tier targets, "+
		"resource_manager uses 1 queue with resource groups",
	"off",
	map[cpuTimeTokenMode]string{
		offMode:             "off",
		serverlessMode:      "serverless",
		resourceManagerMode: "resource_manager",
	},
	settings.WithValidateEnum(func(val string) error {
		if val == "resource_manager" {
			return errors.New("resource_manager mode is not yet implemented")
		}
		return nil
	}),
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
// enabled bool), it returns a WorkQueue that implements CPU time token
// AC. Else it returns a WorkQueue that does slots-based AC.
//
// In serverless mode, there is one WorkQueue for system tenant work
// and another for app tenant work. The system tenant WorkQueue is
// backed by a granter that allows greater resource usage than the app
// tenant WorkQueue. This is a prioritization scheme. In resource
// manager mode, only queue[0] will be used (not yet implemented). For
// details regarding the granters, see cpu_time_token_granter.go.
func (coord *CPUGrantCoordinators) GetKVWorkQueue(isSystemTenant bool) *WorkQueue {
	if !cpuTimeTokenACIsEnabled(&coord.st.SV) {
		return coord.slotsCoord.GetWorkQueue(KVWork)
	}
	mode := cpuTimeTokenMode(coord.cpuTimeCoord.filler.activeMode.Load())
	switch mode {
	case offMode:
		// activeMode should never be offMode in normal operation: the
		// constructor maps off to serverless, and resetInterval never
		// returns offMode. This case is a defensive fallback.
		return coord.slotsCoord.GetWorkQueue(KVWork)
	case serverlessMode:
		if isSystemTenant {
			return coord.cpuTimeCoord.getWorkQueue(systemTenant)
		}
		return coord.cpuTimeCoord.getWorkQueue(appTenant)
	case resourceManagerMode:
		// RM mode uses a single WorkQueue for all work.
		return coord.cpuTimeCoord.getWorkQueue(0)
	default:
		panic(fmt.Sprintf("unexpected mode %d", mode))
	}
}

// GetCTTWorkQueue returns the CPU time token WorkQueue unconditionally,
// without checking whether CPU time token AC is enabled. The caller is
// responsible for gating on the setting. This avoids a race in GetKVWorkQueue
// where the setting can flip between the caller's check and the internal
// re-check, returning a slot-based queue to a caller that expects a CTT queue.
func (coord *CPUGrantCoordinators) GetCTTWorkQueue(isSystemTenant bool) *WorkQueue {
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

// SetResourceGroupConfig installs a new per-resource-group config (weight +
// maxCPU) into the holder, then signals the RM-mode WorkQueue to refresh its
// cached per-group state. When RM mode is off, the second step is a no-op:
// the change stays staged in the holder and is applied when the mode
// setting changes to resourceManagerMode.
func (coord *CPUGrantCoordinators) SetResourceGroupConfig(config ResourceGroupConfigSet) {
	coord.cpuTimeCoord.configHolder.Set(config)
	q, ok := coord.cpuTimeCoord.queues[rmQueueTier].(*WorkQueue)
	if !ok {
		panic(errors.AssertionFailedf("queues[%d] is not a *WorkQueue", rmQueueTier))
	}
	q.refreshResourceGroupConfig()
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

// rmQueueTier is the index in cpuTimeTokenGrantCoordinator.queues
// that owns RM-mode work. Tier 1 is unused in RM mode; see the
// comment on cpuTimeTokenGranter for why.
const rmQueueTier resourceTier = 0

type cpuTimeTokenGrantCoordinator struct {
	filler       *cpuTimeTokenFiller
	queues       [numResourceTiers]requesterClose
	configHolder *ResourceGroupConfigHolder
}

func makeCPUTimeTokenGrantCoordinator(
	ambientCtx log.AmbientContext,
	opts Options,
	settings *cluster.Settings,
	registry *metric.Registry,
	knobs *TestingKnobs,
) *cpuTimeTokenGrantCoordinator {
	// Always create 2 tiers. In RM mode, tier-1 sits idle (no work
	// routed, zero refill rates). This enables dynamic mode switching
	// at runtime without rebuilding queues.
	// Default to serverless when mode is off (legacy bool path or CTT
	// not yet enabled). The strategy only matters when the filler runs,
	// and the filler only starts when CTT is enabled. Using serverless
	// as the default preserves the legacy 2-queue behavior.
	initialMode := cpuTimeTokenACMode.Get(&settings.SV)
	if initialMode == offMode {
		initialMode = serverlessMode
	}
	metrics := makeCPUTimeTokenMetrics()
	registry.AddMetricStruct(metrics)
	timeSource := timeutil.DefaultTimeSource{}
	granter := newCPUTimeTokenGranter(metrics, timeSource)
	var childGranters [numResourceTiers]cpuTimeTokenChildGranter
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		childGranters[tier] = cpuTimeTokenChildGranter{
			tier:   tier,
			parent: granter,
		}
	}
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

	var requesters [numResourceTiers]requester
	wqMetrics := makeWorkQueueMetrics("cpu", registry)
	// One holder shared across both per-tier WorkQueues. RM mode only
	// uses tier 0; tier 1 sees every Set (the holder is shared) but
	// never applies, since SetResourceGroupConfig forwards refresh
	// signals only to tier 0 (rmQueueTier) below.
	configHolder := newResourceGroupConfigHolder()
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		opts := makeWorkQueueOptions(KVWork)
		opts.mode = usesCPUTimeTokens
		opts.perGroupAggMetrics = &groupAggMetrics{
			admittedCount:  metrics.AdmittedCountPerTenant[tier],
			waitTimeNanos:  metrics.WaitTimeNanosPerTenant[tier],
			tokensUsed:     metrics.TokensUsedPerTenant[tier],
			tokensReturned: metrics.TokensReturnedPerTenant[tier],
		}
		opts.configHolder = configHolder
		requesters[tier] = makeWorkQueue(
			ambientCtx, KVWork, &childGranters[tier], settings, wqMetrics, opts)
		granter.requester[tier] = requesters[tier]
		// This type assertion is always valid, since makeWorkQueue always
		// returns a *WorkQueue.
		allocator.queues[tier] = requesters[tier].(*WorkQueue)
	}
	allocator.strategy = allocator.newStrategy(initialMode)

	coordinator := &cpuTimeTokenGrantCoordinator{
		filler:       filler,
		configHolder: configHolder,
	}
	for tier := resourceTier(0); tier < numResourceTiers; tier++ {
		coordinator.queues[tier] = requesters[tier]
	}

	// Initialize the filler's activeMode so GetKVWorkQueue returns the
	// correct queue before the filler goroutine starts.
	filler.activeMode.Store(int64(initialMode))

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

func (coord *cpuTimeTokenGrantCoordinator) getWorkQueue(tier resourceTier) *WorkQueue {
	// TODO(wenyihu6): activeMode is read here and again in the caller
	// (GetKVWorkQueue / GetCTTWorkQueue). If a mode transition lands
	// between the two reads, the assertion can fire.
	if buildutil.CrdbTestBuild {
		mode := cpuTimeTokenMode(coord.filler.activeMode.Load())
		if mode == resourceManagerMode && tier != 0 {
			panic(fmt.Sprintf(
				"queue[%d] accessed in resource manager mode", tier))
		}
	}
	q, ok := coord.queues[tier].(*WorkQueue)
	if !ok {
		panic(errors.AssertionFailedf("queues[%d] is not a *WorkQueue", tier))
	}
	return q
}

func (coord *cpuTimeTokenGrantCoordinator) close() {
	for tier := range coord.queues {
		coord.queues[tier].close()
	}
	coord.filler.close()
}
