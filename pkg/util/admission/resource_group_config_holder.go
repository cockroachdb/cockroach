// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// This file owns the runtime-side, normalized representation of
// per-resource-group configuration consumed by WorkQueue in Resource
// Manager (RM) mode. Four representations are useful to keep distinct
// when reasoning about RM config flow:
//
//  1. Per-group, un-normalized: the operator-supplied weight/maxCPU
//     for a single resource group, before normalization. This will
//     eventually be defined in protobuf so it can be serialized to a
//     system table. Not yet present in this package.
//  2. Set, un-normalized: the un-normalized configurations for an
//     entire set of resource groups, requiring normalization across
//     the set (e.g., weights summed to 100). Not yet present here.
//  3. Per-group, normalized: ResourceGroupConfig below. Ready to be
//     applied directly by WorkQueue without further derivation.
//  4. Set, normalized: ResourceGroupConfigSet below. The full set of
//     normalized per-group configs that ResourceGroupConfigHolder
//     stores and that WorkQueue.applyConfigLocked walks.
//
// ResourceGroupConfigHolder is the source of truth for (4). It is
// pure storage behind a mutex; no derivation happens here. Callers
// are expected to perform any normalization (1)→(3), (2)→(4) before
// calling Set.

// ResourceGroupConfig is the normalized, per-resource-group state
// WorkQueue applies in RM mode (representation 3 in the file
// header). Callers must pre-normalize the values; the holder stores
// them as-is.
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

// ResourceGroupConfigSet is the normalized set of per-group configs
// (representation 4 in the file header) keyed by groupKey. The
// holder treats values of this type as immutable: once installed via
// Set, the map is not mutated; once returned via Snapshot, callers
// must not mutate it either.
//
// All keys must be rgKind groupKeys: the holder is RM-only.
type ResourceGroupConfigSet map[groupKey]ResourceGroupConfig

// SafeFormat implements redact.SafeFormatter. Renders one entry per
// line, sorted by id, e.g.:
//
//	rg1 weight=80 maxCPU=true
//	rg2 weight=20 maxCPU=false
//
// Both groupKey and the numeric fields are redact-safe, so the whole
// type is safe.
func (s ResourceGroupConfigSet) SafeFormat(w redact.SafePrinter, _ rune) {
	keys := make([]groupKey, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].id < keys[j].id })
	for _, k := range keys {
		cfg := s[k]
		w.Printf("%s weight=%d maxCPU=%t\n", k, cfg.Weight, cfg.MaxCPU)
	}
}

// String implements fmt.Stringer via SafeFormat.
func (s ResourceGroupConfigSet) String() string {
	return redact.StringWithoutMarkers(s)
}

// defaultRMResourceGroupConfig is the configuration used until an explicit
// SetResourceGroupConfig call replaces it. The two hardcoded groups (high/low)
// match the two outputs of priorityToResourceGroupKey.
//
// TODO(wenyihu6): what's the reasonable weight here?
var defaultRMResourceGroupConfig = ResourceGroupConfigSet{
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

// ResourceGroupConfigHolder owns the source-of-truth, normalized
// per-resource-group configuration (representation 4 in the file
// header) for RM mode. It is pure storage behind an RWMutex (reads
// vastly outnumber writes: every Admit calls GetOrDefault, while Set
// fires only on a config change).
//
// All keys must be rgKind groupKeys; Set asserts this on entry.
type ResourceGroupConfigHolder struct {
	mu struct {
		syncutil.RWMutex
		config ResourceGroupConfigSet
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

// Set replaces the stored config wholesale. Keys present in the
// prior config but absent from config are dropped.
//
// NB: caller is free to mutate config after Set returns, because
// the input is copied before install.
//
// All keys must be rgKind; Set panics on any other kind. The holder
// only stores RM-mode config and tenantKind/invalidKind keys would
// silently route to the wrong code path in WorkQueue.
func (h *ResourceGroupConfigHolder) Set(config ResourceGroupConfigSet) {
	cp := make(ResourceGroupConfigSet, len(config))
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

// Snapshot returns the installed config map directly (no copy).
//
// NB: caller cannot alter the returned map because it shares the same map
// reference as h.mu.config. May be stale after a subsequent Set.
func (h *ResourceGroupConfigHolder) Snapshot() ResourceGroupConfigSet {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.mu.config
}
