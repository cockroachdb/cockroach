// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import "time"

// NodeCapacityProviderTestingKnobs contains testing knobs for the node capacity
// provider.
type NodeCapacityProviderTestingKnobs struct {
	CpuUsageRefreshInterval    time.Duration
	CpuCapacityRefreshInterval time.Duration
}

// NodeCapacityProviderTestingKnobs implements the ModuleTestingKnobs interface.
func (*NodeCapacityProviderTestingKnobs) ModuleTestingKnobs() {}
