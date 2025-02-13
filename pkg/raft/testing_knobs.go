// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

// TestingKnobs is a part of the context used to control parts of
// the system inside raft used only for testing.
type TestingKnobs struct {
	// DisablePreCampaignStoreLivenessCheck may be used by tests to disable
	// the check performed by a peer before campaigning to ensure it has
	// StoreLiveness support from a majority quorum.
	DisablePreCampaignStoreLivenessCheck bool
}

// IsPreCampaignStoreLivenessCheckDisabled returns true if the test knob
// DisablePreCampaignStoreLivenessCheck is set to true.
func (tk *TestingKnobs) IsPreCampaignStoreLivenessCheckDisabled() bool {
	return tk != nil && tk.DisablePreCampaignStoreLivenessCheck
}
