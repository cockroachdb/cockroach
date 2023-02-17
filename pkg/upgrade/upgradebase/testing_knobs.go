// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgradebase

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TestingKnobs are knobs to inject behavior into the upgrade manager which
// are useful for testing.
type TestingKnobs struct {

	// ListBetweenOverride injects an override for `clusterversion.ListBetween()
	// in order to run upgrades corresponding to versions which do not
	// actually exist.
	ListBetweenOverride func(from, to roachpb.Version) []roachpb.Version

	// RegistryOverride is used to inject upgrades for specific cluster versions.
	RegistryOverride func(v roachpb.Version) (Upgrade, bool)

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
}

// ModuleTestingKnobs makes TestingKnobs a base.ModuleTestingKnobs.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
