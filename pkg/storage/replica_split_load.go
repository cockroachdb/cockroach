// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// SplitByLoadEnabled wraps "kv.range_split.by_load_enabled".
var SplitByLoadEnabled = settings.RegisterBoolSetting(
	"kv.range_split.by_load_enabled",
	"allow automatic splits of ranges based on where load is concentrated",
	true,
)

// SplitByLoadQPSThreshold wraps "kv.range_split.load_qps_threshold".
var SplitByLoadQPSThreshold = settings.RegisterIntSetting(
	"kv.range_split.load_qps_threshold",
	"the QPS over which, the range becomes a candidate for load based splitting",
	250, // 250 req/s
)

// SplitByLoadQPSThreshold returns the QPS request rate for a given replica.
func (r *Replica) SplitByLoadQPSThreshold() float64 {
	return float64(SplitByLoadQPSThreshold.Get(&r.store.cfg.Settings.SV))
}

// SplitByLoadEnabled returns whether load based splitting is enabled.
// Although this is a method of *Replica, the configuration is really global,
// shared across all stores.
func (r *Replica) SplitByLoadEnabled() bool {
	return SplitByLoadEnabled.Get(&r.store.cfg.Settings.SV) &&
		r.store.ClusterSettings().Version.IsActive(cluster.VersionLoadSplits) &&
		!r.store.TestingKnobs().DisableLoadBasedSplitting
}
