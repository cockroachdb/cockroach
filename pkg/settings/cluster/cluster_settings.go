// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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

	// Tracks whether a CPU profile is going on and if so, which kind. See
	// CPUProfileType().
	// This is used so that we can enable "non-cheap" instrumentation only when it
	// is useful.
	cpuProfiling int32 // atomic

	// Version provides the interface through which callers read/write to the
	// active cluster version, and access this binary's version details. Setting
	// the active cluster version has a very specific, intended usage pattern.
	// Look towards the interface itself for more commentary.
	Version clusterversion.Handle

	// Cache can be used for arbitrary caching, e.g. to cache decoded
	// enterprises licenses for utilccl.CheckEnterpriseEnabled().
	Cache syncutil.Map[any, any]

	// OverridesInformer can be nil.
	OverridesInformer OverridesInformer
}

// OverridesInformer is an interface that can be used to figure out if a setting
// is currently being overridden by the host cluster (only possible for
// secondary tenants).
//
// TODO(radu): move this functionality into settings.Values, provide a way to
// obtain it along with the current value consistently.
type OverridesInformer interface {
	IsOverridden(settingKey settings.InternalKey) bool
}

// TelemetryOptOut controls whether to opt out of telemetry (including Sentry) or not.
var TelemetryOptOut = envutil.EnvOrDefaultBool("COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", false)

// NoSettings is used when a func requires a Settings but none is available
// (for example, a CLI subcommand that does not connect to a cluster).
var NoSettings *Settings // = nil

// CPUProfileType tracks whether a CPU profile is in progress.
type CPUProfileType int32

const (
	// CPUProfileNone means that no CPU profile is currently taken.
	CPUProfileNone CPUProfileType = iota
	// CPUProfileDefault means that a CPU profile is currently taken, but
	// pprof labels are not enabled.
	CPUProfileDefault
	// CPUProfileWithLabels means that a CPU profile is currently taken and
	// pprof labels are enabled.
	CPUProfileWithLabels
)

// CPUProfileType returns the type of CPU profile being recorded, if any.
// This can be used by moving parts across the system to add profiler labels
// which are too expensive to be enabled at all times. If no profile is
// currently being recorded, returns CPUProfileNone.
func (s *Settings) CPUProfileType() CPUProfileType {
	return CPUProfileType(atomic.LoadInt32(&s.cpuProfiling))
}

// SetCPUProfiling is called from the pprofui to inform the system that a CPU
// profile is being recorded. If an error is returned, a profile was already in
// progress and the caller must try again later.
func (s *Settings) SetCPUProfiling(to CPUProfileType) error {
	if to == CPUProfileNone {
		atomic.StoreInt32(&s.cpuProfiling, int32(CPUProfileNone))
	} else if !atomic.CompareAndSwapInt32(&s.cpuProfiling, int32(CPUProfileNone), int32(to)) {
		return errors.New("a CPU profile is already in process, try again later")
	}
	if log.V(1) {
		log.Infof(context.Background(), "active CPU profile type set to: %d", to)
	}
	return nil
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

// MakeClusterSettings returns a Settings object. The cluster version setting is
// not initialized.
func MakeClusterSettings() *Settings {
	return MakeClusterSettingsWithVersions(clusterversion.Latest.Version(), clusterversion.MinSupported.Version())
}

// MakeClusterSettingsWithVersions returns a Settings object that has the given
// latest and minimum supported versions. The cluster version setting is not
// initialized.
func MakeClusterSettingsWithVersions(latest, minSupported roachpb.Version) *Settings {
	s := &Settings{}

	sv := &s.SV
	s.Version = clusterversion.MakeVersionHandle(&s.SV, latest, minSupported)
	sv.Init(context.TODO(), s.Version)
	return s
}

// MakeTestingClusterSettings returns a Settings object that is initialized with
// the latest version.
//
// It is typically used for testing or one-off situations in which a Settings
// object is needed, but cluster settings don't play a crucial role.
func MakeTestingClusterSettings() *Settings {
	return MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.Latest.Version(),
		true /* initializeVersion */)
}

// MakeTestingClusterSettingsWithVersions returns a Settings object that has its
// latest and minimum supported versions set to the provided versions.
//
// It can optionally initialize the cluster version setting to the specified
// latestVersion.
//
// It is typically used in tests that want to override the binary's latest and
// minimum supported versions.
func MakeTestingClusterSettingsWithVersions(
	latestVersion, minSupportedVersion roachpb.Version, initializeVersion bool,
) *Settings {
	s := MakeClusterSettingsWithVersions(latestVersion, minSupportedVersion)

	if initializeVersion {
		// Initialize cluster version to specified latestVersion.
		if err := clusterversion.Initialize(context.TODO(), latestVersion, &s.SV); err != nil {
			log.Fatalf(context.TODO(), "unable to initialize version: %s", err)
		}
	}
	return s
}

// TestingCloneClusterSettings makes a clone of the Settings object. This is to
// be used for settings objects that are passed as initial parameters for test
// clusters; the given Settings object should not be in use by any server.
func TestingCloneClusterSettings(st *Settings) *Settings {
	result := &Settings{}
	result.Version = clusterversion.MakeVersionHandle(
		&result.SV, st.Version.LatestVersion(), st.Version.MinSupportedVersion(),
	)
	result.SV.TestingCopyForServer(&st.SV, result.Version)
	return result
}
