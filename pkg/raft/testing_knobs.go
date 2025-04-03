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
	// EnableApplyUnstableEntries allows applying entries once they are known to
	// be committed but before they have been written locally to stable storage.
	//
	// TODO(pav-kv): this is not known to be safe using with async storage writes,
	// hence it's only a testing knob. We can probably make it safe at some point.
	EnableApplyUnstableEntries bool
}

// IsPreCampaignStoreLivenessCheckDisabled returns true if the test knob
// DisablePreCampaignStoreLivenessCheck is set to true.
func (tk *TestingKnobs) IsPreCampaignStoreLivenessCheckDisabled() bool {
	return tk != nil && tk.DisablePreCampaignStoreLivenessCheck
}

// ApplyUnstableEntries returns EnableApplyUnstableEntries.
func (tk *TestingKnobs) ApplyUnstableEntries() bool {
	return tk != nil && tk.EnableApplyUnstableEntries
}
