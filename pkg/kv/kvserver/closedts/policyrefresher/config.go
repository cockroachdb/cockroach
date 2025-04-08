// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package policyrefresher

import "sync/atomic"

// TestingKnobs contains testing knobs for the policy refresher.
type TestingKnobs struct {
	// ClusterUpgradeOverride allows tests to override whether the cluster is
	// considered upgraded for the purposes of enabling auto-tuning of closed
	// timestamp policies. When set to true, auto-tuning will be enabled regardless
	// of the actual cluster version.
	ClusterUpgradeOverride atomic.Bool
}

// TestingKnobs implements the ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
