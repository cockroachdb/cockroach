// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"maps"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	// MaxCPU=true forces burstQualification to canBurst regardless of the bucket
	// level. The bucket itself is still tracked and refilled at Weight/100;
	// MaxCPU only exempts the group from the bucket-fullness gate. Within the
	// same canBurst qualification, groups are still ordered by used/weight, so
	// MaxCPU short-circuits the burst qualification gate.
	MaxCPU bool
}

// defaultRMResourceGroupConfig is the configuration used until an explicit
// SetResourceGroupConfig call replaces it. The two hardcoded groups (high/low)
// match the two outputs of priorityToResourceGroupKey.
//
// TODO(wenyihu6): what's the reasonable weight here?
var defaultRMResourceGroupConfig = map[uint64]ResourceGroupConfig{
	highResourceGroupID: {Weight: 80, MaxCPU: true},
	lowResourceGroupID:  {Weight: 20, MaxCPU: false},
}

// defaultGroupConfig is the safety fallback returned by
// ResourceGroupConfigHolder.GetOrDefault for IDs that aren't explicitly
// configured. Weight=5 (Weight/100 = 5% burst share) lets the request
// compete in fair sharing while granting only a small burst budget. The
// value is RM-specific and intentionally not tied to defaultGroupWeight
// (which is the serverless minimum tenant weight, in a different scale).
// In practise, this should only fire when there is a race where the
// request is received before the holder has been populated with the
// resource group config (e.g. Admit lands during the startup window
// before SetResourceGroupConfig has been called for that ID).
var defaultGroupConfig = ResourceGroupConfig{
	Weight: 5,
	MaxCPU: false,
}

// ResourceGroupConfigHolder owns the source-of-truth resource group
// configuration for Resource Manager mode. The configuration is
// pre-normalized by the caller (see ResourceGroupConfig); the holder
// is pure storage behind a mutex.
type ResourceGroupConfigHolder struct {
	mu struct {
		syncutil.Mutex
		config map[uint64]ResourceGroupConfig
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
func (h *ResourceGroupConfigHolder) Set(config map[uint64]ResourceGroupConfig) {
	cp := make(map[uint64]ResourceGroupConfig, len(config))
	maps.Copy(cp, config)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.config = cp
}

// GetOrDefault returns the config for id if it is configured,
// otherwise defaultGroupConfig. Used by WorkQueue's lazy group
// creation in RM mode: an admit for an ID without a corresponding
// groupInfo consults the holder to populate weight and maxCPU for
// the new groupInfo (burstFrac is computed inline as Weight/100).
func (h *ResourceGroupConfigHolder) GetOrDefault(id uint64) ResourceGroupConfig {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cfg, ok := h.mu.config[id]; ok {
		return cfg
	}
	return defaultGroupConfig
}

// Snapshot returns a copy of the current config. The holder retains no
// reference to the returned map; subsequent Set calls do not affect
// previously-returned snapshots.
func (h *ResourceGroupConfigHolder) Snapshot() map[uint64]ResourceGroupConfig {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := make(map[uint64]ResourceGroupConfig, len(h.mu.config))
	maps.Copy(snap, h.mu.config)
	return snap
}
