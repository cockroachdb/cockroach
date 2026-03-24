// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcutil

import (
	"math/rand"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// MetamorphicSetting describes a single metamorphic setting for CDC roachtests.
type MetamorphicSetting struct {
	Name    string
	Resolve func(*rand.Rand) (string, bool)
}

// Convenience helper to metamorphically resolve bool settings as a string
// with 50/50 distribution. As of writing, all use cases for metamorphic
// settings are cluster settings, i.e. we need to cast to a string to apply them.
func metamorphicBool(rng *rand.Rand) (string, bool) {
	return strconv.FormatBool(rng.Intn(2) == 0), true
}

var (
	RangeFeedSchedulerEnabled = MetamorphicSetting{
		Name:    "kv.rangefeed.scheduler.enabled",
		Resolve: metamorphicBool,
	}
	SchemaLockTables = MetamorphicSetting{
		Name:    "schema_lock_tables",
		Resolve: metamorphicBool,
	}
	PerTableTrackingEnabled = MetamorphicSetting{
		Name:    "changefeed.progress.per_table_tracking.enabled",
		Resolve: metamorphicBool,
	}
	ResolvedTSGranularity = MetamorphicSetting{
		Name: "changefeed.resolved_timestamp.granularity",
		Resolve: func(rng *rand.Rand) (string, bool) {
			return strconv.Itoa(rng.Intn(30)) + "s", true
		},
	}
	// DistributionStrategy resolves to a random strategy 50% of the time,
	// or disabled the other 50%.
	DistributionStrategy = MetamorphicSetting{
		Name: "changefeed.default_range_distribution_strategy",
		Resolve: func(rng *rand.Rand) (string, bool) {
			if rng.Intn(2) == 0 {
				return "", false
			}
			vals := changefeedccl.RangeDistributionStrategy.GetAvailableValues()
			return vals[rng.Intn(len(vals))], true
		},
	}
)

// resolvedSetting represents a metamorphic setting that has been
// accessed via Get() before and resolved to a value. It may be disabled
// as determined by the Resolve method, or explicitly disabled via Disable().
type resolvedSetting struct {
	value string
	// enabled is false if the setting should not be applied.
	enabled bool
}

// MetamorphicSettings holds lazily-resolved metamorphic decisions for
// CDC roachtests.
type MetamorphicSettings struct {
	rng    *rand.Rand
	Seed   int64
	values map[string]resolvedSetting
}

// NewMetamorphicSettings creates a MetamorphicSettings with a random seed.
// The seed can be overridden via the COCKROACH_RANDOM_SEED environment
// variable to reproduce a specific run.
func NewMetamorphicSettings(l *logger.Logger) *MetamorphicSettings {
	rng, seed := randutil.NewLockedPseudoRand()
	l.Printf("metamorphic settings seed: %d (use COCKROACH_RANDOM_SEED to reproduce)", seed)
	return &MetamorphicSettings{
		rng:    rng,
		Seed:   seed,
		values: make(map[string]resolvedSetting),
	}
}

// Get returns the resolved value and whether the setting is enabled.
// On first access the value is randomly chosen and cached.
func (s *MetamorphicSettings) Get(setting MetamorphicSetting) (string, bool) {
	if v, ok := s.values[setting.Name]; ok {
		return v.value, v.enabled
	}
	value, enabled := setting.Resolve(s.rng)
	s.values[setting.Name] = resolvedSetting{value: value, enabled: enabled}
	return value, enabled
}

// Disable prevents a specific setting from being applied.
func (s *MetamorphicSettings) Disable(setting MetamorphicSetting) {
	s.values[setting.Name] = resolvedSetting{value: "", enabled: false}
}

// Resolved returns all resolved and enabled settings as a map.
func (s *MetamorphicSettings) Resolved() map[string]string {
	result := make(map[string]string)
	for name, v := range s.values {
		if v.enabled {
			result[name] = v.value
		}
	}
	return result
}
