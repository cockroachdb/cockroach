// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cluster

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// Settings is the collection of cluster settings. For a running CockroachDB
// node, there is a single instance of Settings which is shared across various
// components.
type Settings struct {
	SV settings.Values

	// Manual defaults to false. If set, lets this ClusterSetting's MakeUpdater
	// method return a dummy updater that simply throws away all values. This is
	// for use in tests for which manual control is desired.
	//
	// Also see the Override() method that different types of settings provide for
	// overwriting the default of a single setting.
	Manual atomic.Value // bool

	Tracer        *tracing.Tracer
	ExternalIODir string

	// Set to 1 if a profile is active (if the profile is being grabbed through
	// the `pprofui` server as opposed to the raw endpoint).
	cpuProfiling int32 // atomic

	// Version provides a read-only view to the active cluster version and this
	// binary's version details.
	Version clusterversion.Handle
}

// TelemetryOptOut is a place for controlling whether to opt out of telemetry or not.
func TelemetryOptOut() bool {
	return envutil.EnvOrDefaultBool("COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", false)
}

// NoSettings is used when a func requires a Settings but none is available
// (for example, a CLI subcommand that does not connect to a cluster).
var NoSettings *Settings // = nil

// IsCPUProfiling returns true if a pprofui CPU profile is being recorded. This can
// be used by moving parts across the system to add profiler labels which are
// too expensive to be enabled at all times.
func (s *Settings) IsCPUProfiling() bool {
	return atomic.LoadInt32(&s.cpuProfiling) == 1
}

// SetCPUProfiling is called from the pprofui to inform the system that a CPU
// profile is being recorded.
func (s *Settings) SetCPUProfiling(to bool) {
	i := int32(0)
	if to {
		i = 1
	}
	atomic.StoreInt32(&s.cpuProfiling, i)
}

// MakeUpdater returns a new Updater, pre-alloced to the registry size. Note
// that if the Setting has the Manual flag set, this Updater simply ignores all
// updates.
func (s *Settings) MakeUpdater() settings.Updater {
	if isManual, ok := s.Manual.Load().(bool); ok && isManual {
		return &settings.NoopUpdater{}
	}
	return settings.NewUpdater(&s.SV)
}

// MakeClusterSettings returns a Settings object that has its binary and
// minimum supported versions set to this binary's build and it's minimum
// supported versions respectively. The cluster version setting is not
// initialized.
func MakeClusterSettings() *Settings {
	s := &Settings{}

	sv := &s.SV
	s.Version = clusterversion.MakeVersionHandle(&s.SV)
	sv.Init(s.Version)

	s.Tracer = tracing.NewTracer()
	s.Tracer.Configure(sv)

	return s
}

// MakeTestingClusterSettings returns a Settings object that has its binary and
// minimum supported versions set to the baked in binary version. It also
// initializes the cluster version setting to the binary version.
//
// It is typically used for testing or one-off situations in which a Settings
// object is needed, but cluster settings don't play a crucial role.
func MakeTestingClusterSettings() *Settings {
	return MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion, clusterversion.TestingBinaryVersion, true /* initializeVersion */)
}

// MakeTestingClusterSettingsWithVersions returns a Settings object that has its
// binary and minimum supported versions set to the provided versions.
// It also can also initialize the cluster version setting to the specified
// binaryVersion.
//
// It is typically used in tests that want to override the default binary and
// minimum supported versions.
func MakeTestingClusterSettingsWithVersions(
	binaryVersion, binaryMinSupportedVersion roachpb.Version, initializeVersion bool,
) *Settings {
	s := &Settings{}

	sv := &s.SV
	s.Version = clusterversion.MakeVersionHandleWithOverride(
		&s.SV, binaryVersion, binaryMinSupportedVersion)
	sv.Init(s.Version)

	s.Tracer = tracing.NewTracer()
	s.Tracer.Configure(sv)

	if initializeVersion {
		// Initialize cluster version to specified binaryVersion.
		if err := clusterversion.Initialize(context.TODO(), binaryVersion, &s.SV); err != nil {
			log.Fatalf(context.TODO(), "unable to initialize version: %s", err)
		}
	}
	return s
}
