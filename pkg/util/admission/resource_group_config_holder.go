// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"math"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ResourceGroupConfig is the per-group state WorkQueue applies when admitting
// work in Resource Manager mode. Callers must pre-normalize the values; the
// holder stores them as-is.
type ResourceGroupConfig struct {
	// Weight is the group's share used for fair-share ordering in the heap.
	// Groups with higher weight get proportionally more CPU time before
	// yielding to others. Must be > 0.
	Weight uint32
	// BurstFrac is the fraction of the 100%-CPU rate allocated to this
	// group's per-group burst bucket. For example, 0.2 means the group's
	// burst bucket refills at 20% of the full CPU rate.
	BurstFrac float64
	// MaxCPU=true forces canBurst regardless of bucket utilization. The bucket
	// is still refilled at BurstFrac of the 100%-CPU rate; MaxCPU only exempts
	// the group from the bucket-fullness gate. Within the same canBurst
	// qualification, groups remain ordered by used/weight.
	MaxCPU bool
}

// ResourceGroupConfigSet is the set of per-group configs keyed by groupKey.
// Once installed in the holder, callers must treat the map as read-only.
type ResourceGroupConfigSet map[groupKey]ResourceGroupConfig

// SafeFormat renders one entry per line, sorted by tenantID then
// groupID, e.g.:
//
//	t0g1 weight=80 burstFrac=0.80 maxCPU=true
//	t0g2 weight=20 burstFrac=0.20 maxCPU=false
func (s ResourceGroupConfigSet) SafeFormat(w redact.SafePrinter, _ rune) {
	keys := make([]groupKey, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, groupKey.compare)
	for _, k := range keys {
		cfg := s[k]
		w.Printf("%s weight=%d burstFrac=%.2f maxCPU=%t\n",
			k, cfg.Weight, cfg.BurstFrac, cfg.MaxCPU)
	}
}

// String implements fmt.Stringer via SafeFormat.
func (s ResourceGroupConfigSet) String() string {
	return redact.StringWithoutMarkers(s)
}

// GetOrDefault returns the config for k if installed, otherwise a
// fallback: resource groups (tenantID==0) get defaultRGGroupConfig;
// tenant groups (groupID==0) get defaultTenantGroupConfig. Used by
// WorkQueue's lazy group creation: an Admit for a key without a
// corresponding groupInfo consults the set to populate weight and
// maxCPU on the new groupInfo.
//
// TODO(wenyihu6): collapse to a single fallback once we can align the
// rg and tenant defaults.
func (s ResourceGroupConfigSet) GetOrDefault(k groupKey) ResourceGroupConfig {
	if cfg, ok := s[k]; ok {
		return cfg
	}
	if k.groupID == 0 {
		// Tenant group (tenantID is set, groupID is zero).
		return defaultTenantGroupConfig
	}
	// Resource group (groupID is set).
	return defaultRGGroupConfig
}

// defaultRGGroupConfig is the safety fallback returned by GetOrDefault
// for resource group keys (groupID != 0) not in the installed
// configuration. In steady state this is unreachable: the built-in
// configs cover high/low. It exists to keep Admit's lazy-create path
// total — if a caller installs a config that omits a known group ID,
// Admit gets a usable weight rather than a zero-weight group.
// Weight=20 mirrors the low default; MaxCPU=false keeps an
// unconfigured group from bypassing the burst-fullness gate.
//
// TODO(wenyihu6): once SQL DDL (CREATE/ALTER RESOURCE GROUP) is wired
// through, decide whether unknown group IDs should be a hard error.
var defaultRGGroupConfig = ResourceGroupConfig{Weight: 20, BurstFrac: 0.2, MaxCPU: false}

// defaultTenantGroupConfig is the fallback for tenant group keys
// (groupID == 0): every tenant gets defaultGroupWeight, since
// per-tenant weights are no longer configurable. MaxCPU=false because
// tenants don't carry burst flags.
var defaultTenantGroupConfig = ResourceGroupConfig{
	Weight: defaultGroupWeight, BurstFrac: 0.20, MaxCPU: false,
}

// systemTenantGroupConfig is the built-in config for the system tenant
// (ID 1). It has maximum weight to ensure system work is never starved,
// BurstFrac=1.0 so the full burst budget is available, and MaxCPU=true
// to bypass the burst-fullness gate.
var systemTenantGroupConfig = ResourceGroupConfig{
	Weight: math.MaxUint32, BurstFrac: 1.0, MaxCPU: true,
}

// builtinGroupConfigs are configs that are always present in the
// holder. Set seeds from this list first; callers cannot overwrite
// built-in keys.
var builtinGroupConfigs = ResourceGroupConfigSet{
	tenantGroupKey(1):    systemTenantGroupConfig,
	highResourceGroupKey: {Weight: 80, BurstFrac: 0.8, MaxCPU: true},
	lowResourceGroupKey:  {Weight: 20, BurstFrac: 0.2, MaxCPU: false},
}

// ConfigSnapshot is the immutable snapshot returned by
// ResourceGroupConfigHolder.Snapshot. It bundles the per-group config
// set with the utilization targets derived from cluster settings.
// Fields are unexported; callers go through the accessor methods so
// the snapshot can present a coherent view of "non-burstable target"
// and "burstable ceiling" independently of how they are stored.
type ConfigSnapshot struct {
	groups      ResourceGroupConfigSet
	noBurstFrac float64
	burstDelta  float64
}

// Groups returns the per-group config set (built-ins + caller-provided).
// The map is shared across all consumers of the snapshot and must be
// treated as read-only.
func (s ConfigSnapshot) Groups() ResourceGroupConfigSet {
	return s.groups
}

// MaxNonBurstableFraction returns the non-burstable CPU utilization
// target (e.g. 0.8 for 80% of CPU capacity).
func (s ConfigSnapshot) MaxNonBurstableFraction() float64 {
	return s.noBurstFrac
}

// MaxFraction returns the burstable CPU utilization ceiling: the
// non-burstable target plus the configured burst delta.
func (s ConfigSnapshot) MaxFraction() float64 {
	return s.noBurstFrac + s.burstDelta
}

// ResourceGroupConfigHolder owns the source-of-truth config set for RM mode.
// It is pure storage behind an RWMutex; reads (every Admit) vastly outnumber
// writes (config changes only).
type ResourceGroupConfigHolder struct {
	// sv provides access to cluster settings for the snapshot's
	// mode and utilization targets. Required.
	sv *settings.Values

	mu struct {
		syncutil.RWMutex
		config ResourceGroupConfigSet
	}
}

// newResourceGroupConfigHolder constructs a holder seeded with
// builtinGroupConfigs. sv must be non-nil; the holder reads cluster
// settings on every Snapshot.
func newResourceGroupConfigHolder(sv *settings.Values) *ResourceGroupConfigHolder {
	if sv == nil {
		panic(errors.AssertionFailedf("newResourceGroupConfigHolder: sv must be non-nil"))
	}
	h := &ResourceGroupConfigHolder{sv: sv}
	h.Set(nil)
	return h
}

// Set replaces the stored config wholesale. Keys absent from config are
// dropped. Built-in configs (builtinGroupConfigs) are always present;
// callers cannot overwrite them.
//
// NB: caller may mutate config after Set returns; the input is copied.
func (h *ResourceGroupConfigHolder) Set(config ResourceGroupConfigSet) {
	cp := make(ResourceGroupConfigSet, len(builtinGroupConfigs)+len(config))
	for k, v := range builtinGroupConfigs {
		cp[k] = v
	}
	for k, v := range config {
		if _, ok := builtinGroupConfigs[k]; ok {
			panic(errors.AssertionFailedf(
				"ResourceGroupConfigHolder.Set: key %s is a built-in and cannot be overwritten", k))
		}
		cp[k] = v
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.config = cp
}

// Snapshot returns the installed config bundled with utilization
// targets from cluster settings. The Groups map is returned directly
// (no copy); it is immutable post-install because Set installs a
// fresh map rather than mutating in place, so prior snapshots remain
// stable.
func (h *ResourceGroupConfigHolder) Snapshot() ConfigSnapshot {
	h.mu.RLock()
	groups := h.mu.config
	h.mu.RUnlock()
	return ConfigSnapshot{
		groups:      groups,
		noBurstFrac: KVCPUTimeUtilGoal.Get(h.sv),
		burstDelta:  KVCPUTimeUtilBurstDelta.Get(h.sv),
	}
}
