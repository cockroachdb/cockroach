// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgradebase

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type PausePoint int

const (
	NoPause PausePoint = iota
	AfterFirstCheckForInstances
	AfterFenceRPC
	AfterSecondCheckForInstances
	AfterFenceWriteToSettingsTable
	AfterMigration
	AfterVersionBumpRPC
	AfterVersionWriteToSettingsTable
)

// TestingKnobs are knobs to inject behavior into the upgrade manager which
// are useful for testing.
type TestingKnobs struct {

	// ListBetweenOverride injects an override for clusterversion.ListBetween() in
	// order to run upgrades corresponding to versions which do not actually
	// exist. This function has to return versions in the range (from, to].
	ListBetweenOverride func(from, to roachpb.Version) []roachpb.Version

	// RegistryOverride is used to inject upgrades for specific cluster versions.
	RegistryOverride func(v roachpb.Version) (Upgrade, bool)

	// InterlockPausePoint specifies the point in the upgrade interlock where
	// the upgrade should pause.
	InterlockPausePoint PausePoint

	// InterlockResumeChannel specifies the channel to wait on when the paused
	// during the upgrade interlock.
	InterlockResumeChannel *chan struct{}

	// InterlockReachedPausePointChannel specifies the channel to post to once
	// the interlock pause point has been reached.
	InterlockReachedPausePointChannel *chan struct{}

	// DontUseJobs, if set, makes upgrades run without employing jobs. This helps
	// tests that care about not having random rows in the system.jobs table, and
	// such. Jobs are not essential for running upgrades, but they help in
	// production.
	DontUseJobs bool

	// SkipJobMetricsPollingJobBootstrap, if set, disables the
	// clusterversion.V23_1_CreateJobsMetricsPollingJob upgrade, which prevents a
	// job from being created.
	SkipJobMetricsPollingJobBootstrap bool

	// AfterRunPermanentUpgrades is called after each call to
	// RunPermanentUpgrades.
	AfterRunPermanentUpgrades func()

	// SkipUpdateSQLActivityJobBootstrap, if set, disables the
	// clusterversion.V23_1AddSystemActivityTables upgrade, which prevents a
	// job from being created.
	SkipUpdateSQLActivityJobBootstrap bool

	SkipMVCCStatisticsJobBootstrap bool

	SkipUpdateTableMetadataCacheBootstrap bool

	SkipSqlActivityFlushJobBootstrap bool

	// ForceCheckLicenseViolation is true if we want the v24_3_check_license_violation.go
	// task to continue even though we are in a test environment.
	ForceCheckLicenseViolation bool
}

// ModuleTestingKnobs makes TestingKnobs a base.ModuleTestingKnobs.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
