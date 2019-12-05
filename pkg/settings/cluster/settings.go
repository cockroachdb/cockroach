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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// Settings is the collection of cluster settings. For a running CockroachDB
// node, there is a single instance of ClusterSetting which is shared across all
// of its components.
//
// For testing or one-off situations in which a ClusterSetting is needed, but
// cluster settings don't play a crucial role, MakeTestingClusterSetting() is
// provided; the version is pre-initialized to the binary's server version.
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

	Initialized bool

	// Set to 1 if a profile is active (if the profile is being grabbed through
	// the `pprofui` server as opposed to the raw endpoint).
	cpuProfiling int32 // atomic

	// Versions describing the range supported by this binary.
	binaryMinSupportedVersion    roachpb.Version
	binaryServerVersion          roachpb.Version
	beforeClusterVersionChangeMu struct {
		syncutil.Mutex
		// Callback to be called when the cluster version is about to be updated.
		cb func(ctx context.Context, newVersion ClusterVersion)
	}
}

// TelemetryOptOut is a place for controlling whether to opt out of telemetry or not.
func TelemetryOptOut() bool {
	return envutil.EnvOrDefaultBool("COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", false)
}

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

// NoSettings is used when a func requires a Settings but none is available
// (for example, a CLI subcommand that does not connect to a cluster).
var NoSettings *Settings // = nil

// KeyVersionSetting is the "version" settings key.
const KeyVersionSetting = "version"

// Version represents the cluster's "active version". The active version is a
// cluster setting, but a special one. It can only advance to higher and higher
// versions. The setting can be used to see if migrations are to be considered
// enabled or disabled through the IsActive() method.
//
// During the node startup sequence, an initial version (persisted to the
// engines) is read and passed to Initialize(). It is only after that that the
// Version field of this struct is ready for use (i.e. Version() and IsActive()
// can be called). In turn, the node usually registers itself as a callback to
// be notified of any further updates to the setting, which are also persisted.
//
// This dance is necessary because we cannot determine a safe default value for
// the version setting without looking at what's been persisted: The setting
// specifies the minimum binary version we have to expect to be in a mixed
// cluster with. We can't assume this binary's MinimumSupportedVersion as the
// cluster could've started up earlier and enabled features that are no longer
// compatible with this binary's MinimumSupportedVersion; we can't assume it's
// our binary's ServerVersion as that would enable features that may trip up
// older versions running in the same cluster. Hence, only once we get word of
// the "safe" version to use can we allow moving parts that actually need to
// know what's going on.
//
// Additionally, whenever the version changes, we want to persist that update to
// wherever the caller to Initialize() got the initial version from
// (typically a collection of `engine.Engine`s), which the caller will do by
// registering itself via SetBeforeChange()`, which is invoked *before* exposing
// the new version to callers of `IsActive()` and `Version()`.
var Version = registerClusterVersionSetting()

// registerClusterVersionSetting creates a clusterVersionSetting and registers
// it with the cluster settings registry.
func registerClusterVersionSetting() clusterVersionSetting {
	s := makeClusterVersionSetting()
	s.StateMachineSetting.SetReportable(true)
	settings.RegisterStateMachineSetting(
		KeyVersionSetting,
		"set the active cluster version in the format '<major>.<minor>'", // hide optional `-<unstable>,
		&s.StateMachineSetting)
	s.SetVisibility(settings.Public)
	return s
}

var preserveDowngradeVersion = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		"cluster.preserve_downgrade_option",
		"disable (automatic or manual) cluster version upgrade from the specified version until reset",
		"",
		func(sv *settings.Values, s string) error {
			if sv == nil || s == "" {
				return nil
			}
			opaque := sv.Opaque()
			st := opaque.(*Settings)
			clusterVersion := Version.ActiveVersion(context.TODO(), st).Version
			downgradeVersion, err := roachpb.ParseVersion(s)
			if err != nil {
				return err
			}

			// cluster.preserve_downgrade_option can only be set to the current cluster version.
			if downgradeVersion != clusterVersion {
				return errors.Errorf(
					"cannot set cluster.preserve_downgrade_option to %s (cluster version is %s)",
					s, clusterVersion)
			}
			return nil
		},
	)
	s.SetReportable(true)
	s.SetVisibility(settings.Public)
	return s
}()

// MakeTestingClusterSettings returns a Settings object that has had its version
// initialized to BinaryServerVersion.
func MakeTestingClusterSettings() *Settings {
	return MakeTestingClusterSettingsWithVersion(BinaryServerVersion, BinaryServerVersion)
}

// MakeTestingClusterSettingsWithVersion returns a Settings object that has had
// its version initialized to the provided version configuration.
func MakeTestingClusterSettingsWithVersion(minVersion, serverVersion roachpb.Version) *Settings {
	st := MakeClusterSettings(minVersion, serverVersion)
	// Initialize with all features enabled.
	if err := Version.Initialize(context.TODO(), serverVersion, st); err != nil {
		log.Fatalf(context.TODO(), "unable to initialize version: %s", err)
	}
	return st
}

// MakeClusterSettings makes a new ClusterSettings object. The version limits
// are initialized to the supplied range.
//
// Note that the cluster version is not initialized (i.e. Version.Initialize()
// is not called).
func MakeClusterSettings(minVersion, serverVersion roachpb.Version) *Settings {
	s := &Settings{
		binaryMinSupportedVersion: minVersion,
		binaryServerVersion:       serverVersion,
	}

	sv := &s.SV
	sv.Init(s)

	s.Tracer = tracing.NewTracer()
	s.Tracer.Configure(sv)
	s.Initialized = true

	return s
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

// clusterVersionSetting is the implementation of the Version setting. Like all setting structs,
// it is immutable, as Version is a global; all the state is maintained
// in a Settings instance.
type clusterVersionSetting struct {
	settings.StateMachineSetting
}

var _ settings.StateMachineSettingImpl = clusterVersionSetting{}

func makeClusterVersionSetting() clusterVersionSetting {
	s := clusterVersionSetting{}
	s.StateMachineSetting = settings.MakeStateMachineSetting(s)
	return s
}

func (cv clusterVersionSetting) BinaryVersion(st *Settings) roachpb.Version {
	return st.binaryServerVersion
}

func (cv clusterVersionSetting) BinaryMinSupportedVersion(st *Settings) roachpb.Version {
	return st.binaryMinSupportedVersion
}

// Initialize initializes cluster version. Before this method has been
// called, usage of the version is illegal and leads to a fatal error.
func (cv clusterVersionSetting) Initialize(
	ctx context.Context, version roachpb.Version, st *Settings,
) error {
	if ver := cv.ActiveVersionOrEmpty(ctx, st); ver != (ClusterVersion{}) {
		// Allow initializing a second time as long as it's setting the version to
		// what it was already set. This is useful in tests that use
		// MakeTestingClusterSettings() which initializes the version, and the
		// start a server which again initializes it.
		if version == ver.Version {
			return nil
		}
		return errors.AssertionFailedf("cannot initialize version to %s because already set to: %s",
			version, ver)
	}
	if err := cv.validateSupportedVersionInner(ctx, version, &st.SV); err != nil {
		return err
	}

	// Return the serialized form of the new version.
	newV := ClusterVersion{Version: version}
	encoded, err := protoutil.Marshal(&newV)
	if err != nil {
		return err
	}
	cv.SetInternal(&st.SV, encoded)
	return nil
}

// ActiveVersion returns the cluster's current active version: the minimum
// cluster version the caller may assume is in effect.
//
// ActiveVersion fatals if the version has not been initialized.
func (cv *clusterVersionSetting) ActiveVersion(ctx context.Context, st *Settings) ClusterVersion {
	ver := cv.ActiveVersionOrEmpty(ctx, st)
	if ver == (ClusterVersion{}) {
		log.Fatalf(ctx, "version not initialized")
	}
	return ver
}

// ActiveVersionOrEmpty is like ActiveVersion, but returns an empty version if
// the active version was not initialized.
func (cv *clusterVersionSetting) ActiveVersionOrEmpty(
	ctx context.Context, st *Settings,
) ClusterVersion {
	encoded := cv.GetInternal(&st.SV)
	if encoded == nil {
		return ClusterVersion{}
	}
	var curVer ClusterVersion
	if err := protoutil.Unmarshal(encoded.([]byte), &curVer); err != nil {
		log.Fatal(ctx, err)
	}
	return curVer
}

// IsActive returns true if the features of the supplied version key are active
// at the running version. In other words, if a particular version returns true
// from this method, it means that you're guaranteed that all of the nodes in
// the cluster have running binaries that are at least as new as that version,
// and that you're guaranteed that those nodes will never be downgraded to an
// older version.
//
// If this returns true then all nodes in the cluster will eventually see this
// version. However, this is not atomic because versions are gossiped. Because
// of this, nodes should not be gating proper handling of remotely initiated
// requests that their binary knows how to handle on this state. The following
// example shows why this is important:
//
//  The cluster restarts into the new version and the operator issues a SET
//  VERSION, but node1 learns of the bump 10 seconds before node2, so during
//  that window node1 might be receiving "old" requests that it itself wouldn't
//  issue any more. Similarly, node2 might be receiving "new" requests that its
//  binary must necessarily be able to handle (because the SET VERSION was
//  successful) but that it itself wouldn't issue yet.
//
// This is still a useful method to have as node1, in the example above, can use
// this information to know when it's safe to start issuing "new" outbound
// requests. When receiving these "new" inbound requests, despite not seeing the
// latest active version, node2 is aware that the sending node has, and it will
// too, eventually.
func (cv *clusterVersionSetting) IsActive(
	ctx context.Context, st *Settings, versionKey VersionKey,
) bool {
	return cv.ActiveVersion(ctx, st).IsActive(versionKey)
}

// BeforeChange is part of the StateMachineSettingImpl interface
func (cv clusterVersionSetting) BeforeChange(
	ctx context.Context, encodedVal []byte, sv *settings.Values,
) {
	var clusterVersion ClusterVersion
	if err := protoutil.Unmarshal(encodedVal, &clusterVersion); err != nil {
		log.Fatalf(ctx, "failed to unmarshall version: %s", err)
	}

	opaque := sv.Opaque()
	st := opaque.(*Settings)
	st.beforeClusterVersionChangeMu.Lock()
	if cb := st.beforeClusterVersionChangeMu.cb; cb != nil {
		cb(ctx, clusterVersion)
	}
	st.beforeClusterVersionChangeMu.Unlock()
}

// SetBeforeChange registers a callback to be called before the cluster version
// is updated. The new cluster version will only become "visible" after the
// callback has returned.
//
// The callback can be set at most once.
func (cv clusterVersionSetting) SetBeforeChange(
	ctx context.Context, st *Settings, cb func(ctx context.Context, newVersion ClusterVersion),
) {
	st.beforeClusterVersionChangeMu.Lock()
	defer st.beforeClusterVersionChangeMu.Unlock()
	if st.beforeClusterVersionChangeMu.cb != nil {
		log.Fatalf(ctx, "beforeClusterVersionChange already set")
	}
	st.beforeClusterVersionChangeMu.cb = cb
}

// Decode is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSetting) Decode(val []byte) (interface{}, error) {
	var clusterVersion ClusterVersion
	if err := protoutil.Unmarshal(val, &clusterVersion); err != nil {
		return "", err
	}
	return clusterVersion, nil
}

// DecodeToString is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSetting) DecodeToString(val []byte) (string, error) {
	clusterVersion, err := cv.Decode(val)
	if err != nil {
		return "", err
	}
	return clusterVersion.(ClusterVersion).Version.String(), nil
}

// ValidateLogical is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSetting) ValidateLogical(
	ctx context.Context, sv *settings.Values, curRawProto []byte, newVal string,
) ([]byte, error) {
	newVersion, err := roachpb.ParseVersion(newVal)
	if err != nil {
		return nil, err
	}
	if err := cv.validateSupportedVersionInner(ctx, newVersion, sv); err != nil {
		return nil, err
	}

	var oldV ClusterVersion
	if err := protoutil.Unmarshal(curRawProto, &oldV); err != nil {
		return nil, err
	}

	// Versions cannot be downgraded.
	if newVersion.Less(oldV.Version) {
		return nil, errors.Errorf(
			"versions cannot be downgraded (attempting to downgrade from %s to %s)",
			oldV.Version, newVersion)
	}

	// Prevent cluster version upgrade until cluster.preserve_downgrade_option is reset.
	if downgrade := preserveDowngradeVersion.Get(sv); downgrade != "" {
		return nil, errors.Errorf(
			"cannot upgrade to %s: cluster.preserve_downgrade_option is set to %s",
			newVersion, downgrade)
	}

	// Return the serialized form of the new version.
	newV := ClusterVersion{Version: newVersion}
	return protoutil.Marshal(&newV)
}

// ValidateGossipVersion is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSetting) ValidateGossipUpdate(
	ctx context.Context, sv *settings.Values, rawProto []byte,
) (retErr error) {

	defer func() {
		// This implementation of ValidateGossipUpdate never returns errors. Instead,
		// we crash. Not being able to update our version to what the rest of the cluster is running
		// is a serious issue.
		if retErr != nil {
			log.Fatalf(ctx, "failed to validate version upgrade: %s", retErr)
		}
	}()

	var ver ClusterVersion
	if err := protoutil.Unmarshal(rawProto, &ver); err != nil {
		return err
	}
	return cv.validateSupportedVersionInner(ctx, ver.Version, sv)
}

func (cv clusterVersionSetting) validateSupportedVersionInner(
	ctx context.Context, ver roachpb.Version, sv *settings.Values,
) error {
	opaque := sv.Opaque()
	st := opaque.(*Settings)
	if st.binaryMinSupportedVersion == (roachpb.Version{}) {
		panic("binaryMinSupportedVersion not set")
	}
	if st.binaryServerVersion.Less(ver) {
		// TODO(tschottdorf): also ask gossip about other nodes.
		return errors.Errorf("cannot upgrade to %s: node running %s",
			ver, st.binaryServerVersion)
	}
	if ver.Less(st.binaryMinSupportedVersion) {
		return errors.Errorf("node at %s cannot run %s (minimum version is %s)",
			st.binaryServerVersion, ver, st.binaryMinSupportedVersion)
	}
	return nil
}

// SettingsListDefault is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSetting) SettingsListDefault() string {
	return BinaryServerVersion.String()
}
