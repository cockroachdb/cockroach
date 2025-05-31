// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package policyrefresher

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TestingKnobs contains testing knobs for the policy refresher.
type TestingKnobs struct {
	// InjectedLatencies returns a callback that returns a map of node IDs to
	// latencies that will be used by the policy refresher instead of observed
	// latencies from rpc context without checking for cluster settings or
	// version compatibility.
	InjectedLatencies func() map[roachpb.NodeID]time.Duration
}

// TestingKnobs implements the ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
