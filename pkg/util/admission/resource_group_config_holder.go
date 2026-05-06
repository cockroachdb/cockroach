// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ResourceGroupConfig is the per-resource-group state WorkQueue applies in
// Resource Manager mode. The caller is responsible for pre-normalizing the
// values: the holder stores the map as-is and does no derivation.
type ResourceGroupConfig struct {
	// Weight is the group's percentage share of node CPU, in [0, 100]. Used
	// directly as the group's heap weight and as its burst-bucket refill share
	// (Weight/100). Callers are expected to set weights that sum to 100 across
	// the configured set. But the holder does not require this strictly as an
	// invariant.
	Weight uint32
	// MaxCPU=true forces burstQualification to canBurst regardless of the
	// bucket utilization. The bucket itself is still tracked and refilled at
	// Weight/100; MaxCPU only exempts the group from the bucket-fullness gate.
	// Within the same canBurst qualification, groups remain ordered by
	// used/weight.
	MaxCPU bool
}

// defaultRMResourceGroupConfig is the configuration used until an explicit
// SetResourceGroupConfig call replaces it. The two hardcoded groups (high/low)
// match the two outputs of priorityToResourceGroupKey.
//
// TODO(wenyihu6): what's the reasonable weight here?
var defaultRMResourceGroupConfig = map[groupKey]ResourceGroupConfig{
	rgGroupKey(highResourceGroupID): {Weight: 80, MaxCPU: true},
	rgGroupKey(lowResourceGroupID):  {Weight: 20, MaxCPU: false},
}

// defaultGroupConfig is the safety fallback returned by
// ResourceGroupConfigHolder.GetOrDefault for keys that aren't in the
// installed configuration. In current code the only producers of
// rgKind keys are priorityToResourceGroupKey (high/low), and both
// are seeded by defaultRMResourceGroupConfig at construction, so
// this fallback is unreachable in steady state. It exists to keep
// the lazy-create path on Admit total — if a future caller installs
// a config that omits high/low (or introduces new rg IDs without
// wiring them through Set first), Admit gets a usable weight rather
// than a panic or a zero-weight group.
//
// Weight=20 mirrors the low default: an unconfigured group should
// participate in fair sharing but is treated as the lowest tier.
// MaxCPU=false because we don't want an unconfigured group to bypass
// the burst-fullness gate.
//
// TODO(wenyihu6): once SQL DDL (CREATE/ALTER RESOURCE GROUP) is
// wired through, decide whether unknown rgKind IDs should be a hard
// error instead of falling back here.
var defaultGroupConfig = ResourceGroupConfig{
	Weight: 20,
	MaxCPU: false,
}

// ResourceGroupConfigHolder owns the source-of-truth resource group
// configuration for Resource Manager mode. The configuration is
// pre-normalized by the caller (see ResourceGroupConfig); the holder
// is pure storage behind an RWMutex (reads vastly outnumber writes:
// every Admit calls GetOrDefault, while Set fires only on a config
// change).
//
// All keys must be rgKind groupKeys: the holder is RM-only and Set
// asserts this invariant on entry.
type ResourceGroupConfigHolder struct {
	mu struct {
		syncutil.RWMutex
		// config is treated as immutable once stored. Set installs a
		// fresh map (defensive copy of the caller's input); Snapshot
		// returns this pointer directly without copying. Readers must
		// not mutate the returned map.
		config map[groupKey]ResourceGroupConfig
	}
}

// newResourceGroupConfigHolder constructs a holder seeded with
// defaultRMResourceGroupConfig. The seed ensures that an immediate
// Snapshot on a freshly-constructed holder returns the high/low
// hardcoded resource groups, which is what WorkQueue applies on
// first activation of RM mode (no separate "wait for first Set"
// path).
func newResourceGroupConfigHolder() *ResourceGroupConfigHolder {
	h := &ResourceGroupConfigHolder{}
	h.Set(defaultRMResourceGroupConfig)
	return h
}

// Set replaces the stored config wholesale. Keys present in the prior
// config but absent from config are dropped. The holder copies the map
// (values are copied by value since ResourceGroupConfig is a value type),
// so callers may safely mutate the input after Set returns.
//
// All keys must be rgKind; Set panics on any other kind. The holder
// only stores RM-mode config and tenantKind/invalidKind keys would
// silently route to the wrong code path in WorkQueue.
func (h *ResourceGroupConfigHolder) Set(config map[groupKey]ResourceGroupConfig) {
	cp := make(map[groupKey]ResourceGroupConfig, len(config))
	for k, v := range config {
		if k.kind != rgKind {
			panic(errors.AssertionFailedf("ResourceGroupConfigHolder.Set: non-rg key %s", k))
		}
		cp[k] = v
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.config = cp
}

// GetOrDefault returns the config for k if it is configured,
// otherwise defaultGroupConfig. Used by WorkQueue's lazy group
// creation in RM mode: an admit for a key without a corresponding
// groupInfo consults the holder to populate weight and maxCPU for
// the new groupInfo (burstFrac is computed inline as Weight/100).
func (h *ResourceGroupConfigHolder) GetOrDefault(k groupKey) ResourceGroupConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if cfg, ok := h.mu.config[k]; ok {
		return cfg
	}
	return defaultGroupConfig
}

// Snapshot returns the currently-installed config map. The returned
// map is shared with the holder and with any concurrent Snapshot
// callers; callers must treat it as read-only. A subsequent Set
// installs a new map and does not modify the previously-returned
// one, so existing snapshots remain valid (and stable) for the
// lifetime of the caller's reference.
func (h *ResourceGroupConfigHolder) Snapshot() map[groupKey]ResourceGroupConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.mu.config
}
