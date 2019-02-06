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
	"runtime/debug"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// Settings is the collection of cluster settings. For a running CockroachDB
// node, there is a single instance of ClusterSetting which is shared across all
// of its components.
//
// The Version setting deserves an individual explanantion. During the node
// startup sequence, an initial version (persisted to the engines) is read and
// passed to InitializeVersion(). It is only after that that the Version field
// of this struct is ready for use (i.e. Version() and IsActive() can be
// called). In turn, the node usually registers itself as a callback to be
// notified of any further updates to the setting, which are then persisted.
//
// This dance is necessary because we cannot determine a safe default value for
// the version setting without looking at what's been persisted: The setting
// specifies the minimum binary version we have to expect to be in a mixed
// cluster with. We can't assume this binary's MinimumSupportedVersion as we
// could've started up earlier and enabled features that are not actually
// compatible with that version; we can't assume it's our binary's ServerVersion
// as that would enable features that may trip up older versions running in the
// same cluster. Hence, only once we get word of the "safe" version to use can
// we allow moving parts that actually need to know what's going on.
//
// Additionally, whenever the version changes, we want to persist that update to
// wherever the caller to InitializeVersion() got the initial version from
// (typically a collection of `engine.Engine`s), which the caller will do by
// registering itself via `(*Setting).Version.OnChange()`, which is invoked
// *before* exposing the new version to callers of `IsActive()` and `Version()`.
//
// For testing or one-off situations in which a ClusterSetting is needed, but
// cluster settings don't play a crucial role, MakeTestingClusterSetting() is
// provided; it is pre-initialized to the binary's ServerVersion.
type Settings struct {
	SV settings.Values

	// Manual defaults to false. If set, lets this ClusterSetting's MakeUpdater
	// method return a dummy updater that simply throws away all values. This is
	// for use in tests for which manual control is desired.
	Manual atomic.Value // bool

	Version ExposedClusterVersion
	// !!! comment
	binaryMinSupportedVersion roachpb.Version
	binaryMaxSupportedVersion roachpb.Version

	Tracer        *tracing.Tracer
	ExternalIODir string

	Initialized bool

	// Set to 1 if a profile is active (if the profile is being grabbed through
	// the `pprofui` server as opposed to the raw endpoint).
	cpuProfiling int32 // atomic
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

var version = settings.RegisterStateMachineSetting(KeyVersionSetting,
	"set the active cluster version in the format '<major>.<minor>'", // hide optional `-<unstable>`
	// !!! settings.TransformerFn(versionTransformer),
	clusterVersionSettingImpl{},
)

var preserveDowngradeVersion = settings.RegisterValidatedStringSetting(
	"cluster.preserve_downgrade_option",
	"disable (automatic or manual) cluster version upgrade from the specified version until reset",
	"",
	func(sv *settings.Values, s string) error {
		if sv == nil || s == "" {
			return nil
		}
		opaque := sv.Opaque()
		st := opaque.(*Settings)
		clusterVersion := st.Version.Version().Version
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

// InitializeVersion initializes the Version field of this setting. Before this
// method has been called, usage of the Version field is illegal and leads to a
// fatal error.
func (s *Settings) InitializeVersion(
	cv ClusterVersion,
	binaryMinSupportedVersion roachpb.Version,
	binaryMaxSupportedVersion roachpb.Version,
) error {
	s.binaryMinSupportedVersion = binaryMinSupportedVersion
	s.binaryMaxSupportedVersion = binaryMaxSupportedVersion

	b, err := protoutil.Marshal(&cv)
	if err != nil {
		return err
	}
	// Note that we don't call `updater.ResetRemaining()`.
	updater := settings.NewUpdater(&s.SV)
	if err := updater.Set(KeyVersionSetting, string(b), version.Typ()); err != nil {
		return err
	}
	log.Infof(context.TODO(), "!!! InitializeVersion: %s (%s -> %s) [%p]", cv, binaryMinSupportedVersion, binaryMaxSupportedVersion, &s.Version)
	s.Version.baseVersion.Store(&cv)
	return nil
}

// An ExposedClusterVersion exposes a cluster-wide minimum version which is
// assumed to be supported by all nodes. This in turn allows features which are
// incompatible with older versions to be used safely.
type ExposedClusterVersion struct {
	// // !!! remove these two
	// MinSupportedVersion roachpb.Version
	// ServerVersion       roachpb.Version

	// baseVersion track the value from Values, but after it has been written to
	// all the local engines.
	baseVersion atomic.Value // stores *ClusterVersion
	cb          func(ClusterVersion)
}

// IsInitialized returns true if the cluster version has been initialized and is
// ready for use.
func (ecv *ExposedClusterVersion) IsInitialized() bool {
	bv := ecv.baseVersion.Load()
	if bv == nil {
		return false
	}
	return *bv.(*ClusterVersion) != ClusterVersion{}
}

// OnChange registers (a single) callback that will be invoked whenever the
// cluster version changes. The new cluster version will only become "visible"
// after the callback has returned.
//
// The callback can be set at most once.
func (ecv *ExposedClusterVersion) OnChange(f func(cv ClusterVersion)) {
	if ecv.cb != nil {
		log.Fatal(context.TODO(), "cannot set callback twice")
	}
	ecv.cb = f
}

// Version returns the minimum cluster version the caller may assume is in
// effect. It must not be called until the setting has been initialized.
func (ecv *ExposedClusterVersion) Version() ClusterVersion {
	v := ecv.baseVersion.Load()
	if v == nil {
		log.Infof(context.TODO(), "!!! crashing on: %p", ecv)
		log.Fatal(context.Background(), "Version() was called before having been initialized")
	}
	return *v.(*ClusterVersion)
	// !!!
	// if (v == ClusterVersion{}) {
	//   log.Fatal(context.Background(), "Version() was called before having been initialized")
	// }
	// return v
}

// !!!
// // BootstrapVersion returns the version a newly initialized cluster should have.
// func (ecv *ExposedClusterVersion) BootstrapVersion() ClusterVersion {
//   return ClusterVersion{Version: ecv.ServerVersion}
// }

// IsActive returns true if the features of the supplied version key are active
// at the running version.
//
// If this returns true then all nodes in the cluster will eventually see this
// version. However, this is not atomic because versions are gossiped. Because
// of this, nodes should not gate proper handling of remotely initiated requests
// that their binary knows how to handle on this state. The following example
// shows why this is important:
//  The cluster restarts into the new version and the operator issues a SET
//  VERSION, but node1 learns of the bump 10 seconds before node2, so during
//  that window node1 might be receiving "old" requests that it itself wouldn't
//  issue any more. Similarly, node2 might be receiving "new" requests that its
//  binary must necessarily be able to handle (because the SET VERSION was
//  successful) but that it itself wouldn't issue yet.
func (ecv *ExposedClusterVersion) IsActive(versionKey VersionKey) bool {
	return ecv.Version().IsActive(versionKey)
}

// CheckVersion is like IsActive but returns an appropriate error in the
// case of a cluster version which is too low.
func (ecv *ExposedClusterVersion) CheckVersion(versionKey VersionKey, feature string) error {
	if !ecv.Version().IsActive(versionKey) {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cluster version does not support %s (>= %s required)",
			feature,
			VersionByKey(versionKey).String(),
		)
	}
	return nil
}

// Silence unused warning.
var _ = (*ExposedClusterVersion)(nil).CheckVersion

// !!! Can I get rid of this function?
// MakeTestingClusterSettings returns a Settings object that has had its version
// initialized to BinaryServerVersion.
func MakeTestingClusterSettings() *Settings {
	// !!! return MakeTestingClusterSettingsWithVersion(BinaryServerVersion)
	return MakeClusterSettings()
}

// !!! can I get rid of this function? testContexts can now be built using
// StartWithStoreConfigAndVersion(), and that calls st.InitializeVersion.
//
// MakeTestingClusterSettingsWithVersion returns a Settings object that has had
// its version initialized to the provided version configuration.
func MakeTestingClusterSettingsWithVersion(serverVersion roachpb.Version) *Settings {
	st := MakeClusterSettings()
	// !!! cv := st.Version.BootstrapVersion()
	// Initialize with all features enabled.
	cv := ClusterVersion{Version: serverVersion}
	if err := st.InitializeVersion(cv, serverVersion, serverVersion); err != nil {
		log.Fatalf(context.TODO(), "unable to initialize version: %s", err)
	}
	return st
}

// MakeClusterSettings makes a new ClusterSettings object for the given minimum
// supported and server version, respectively.
// !!! func MakeClusterSettings(minVersion, serverVersion roachpb.Version) *Settings {
func MakeClusterSettings() *Settings {
	s := &Settings{}

	// // !!! can I get rid of this?
	// // Initialize the setting. Note that baseVersion starts out with the zero
	// // cluster version, for which the transformer accepts any new version. After
	// // that, it'll only accept "valid bumps". We use this to initialize the
	// // variable lazily, after we have read the current version from the engines.
	// // After that, updates come from Gossip and need to be compatible with the
	// // engine version.
	// s.Version.MinSupportedVersion = minVersion
	// s.Version.ServerVersion = serverVersion
	// s.Version.baseVersion.Store(&ClusterVersion{})

	sv := &s.SV
	sv.Init(s)

	s.Tracer = tracing.NewTracer()
	s.Tracer.Configure(sv)

	version.SetOnChange(sv, func() {
		proto, err := version.Decode([]byte(version.Get(sv)))
		if err != nil {
			log.Fatal(context.Background(), err)
		}
		clusterVersion := proto.(ClusterVersion)
		if (clusterVersion == ClusterVersion{}) {
			panic("!!!")
		}

		// Call callback before exposing the new version to callers of
		// IsActive() and Version(). Don't do this if the new version is
		// trivial, which is the case as the setting is initialized.
		// !!! if (newV != ClusterVersion{}) && s.Version.cb != nil {
		if s.Version.cb != nil {
			s.Version.cb(clusterVersion)
		}
		log.Infof(context.TODO(), "!!! version.OnChange: %s", clusterVersion)
		debug.PrintStack()
		s.Version.baseVersion.Store(&clusterVersion)
	})

	s.Initialized = true

	return s
}

func (s *Settings) BinaryVersion() roachpb.Version {
	return s.binaryMaxSupportedVersion
}

//
// !!!
// func (s *Settings) BinaryMinSupportedVersion() roachpb.Version {
//   return s.binaryMinSupportedVersion
// }

// MakeUpdater returns a new Updater, pre-alloced to the registry size. Note
// that if the Setting has the Manual flag set, this Updater simply ignores all
// updates.
func (s *Settings) MakeUpdater() settings.Updater {
	if isManual, ok := s.Manual.Load().(bool); ok && isManual {
		return &settings.NoopUpdater{}
	}
	return settings.NewUpdater(&s.SV)
}

// !!! can this go away
type stringedVersion ClusterVersion

func (sv *stringedVersion) String() string {
	if sv == nil {
		sv = &stringedVersion{}
	}
	return sv.Version.String()
}

type clusterVersionSettingImpl struct {
	// !!!
	// getGeneric                          func(*settings.Values) []byte
	// validateClusterVersionAgainstBinary func(roachpb.Version) error
}

var _ settings.StateMachineSettingImpl = clusterVersionSettingImpl{}

// !!!
// // setGetGeneric is part of the StateMachineSettingImpl interface.
// func (cv *clusterVersionSettingImpl) setGetGeneric(cb func(*settings.Values) interface{}) {
//   cv.getGeneric = func(sv *settings.Values) []byte {
//     val := cb(sv)
//     if val == nil {
//       return nil
//     }
//     return val.([]byte)
//   }
// }

// Decode is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSettingImpl) Decode(val []byte) (interface{}, error) {
	var clusterVersion ClusterVersion
	if err := protoutil.Unmarshal(val, &clusterVersion); err != nil {
		return "", err
	}
	return clusterVersion, nil
}

// DecodeToString is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSettingImpl) DecodeToString(val []byte) (string, error) {
	clusterVersion, err := cv.Decode(val)
	if err != nil {
		return "", err
	}
	return clusterVersion.(ClusterVersion).Version.String(), nil
}

// ValidateLogical is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSettingImpl) ValidateLogical(
	sv *settings.Values, curRawProto []byte, newVal string,
) ([]byte, error) {
	opaque := sv.Opaque()
	if opaque == settings.TestOpaque {
		// !!! if update != nil {
		panic("modifying version when TestOpaque is set")
		// !!! }
		// !!! return nil, nil
	}

	newVersion, err := roachpb.ParseVersion(newVal)
	if err != nil {
		return nil, err
	}
	if err := cv.validateSupportedVersionInner(opaque.(*Settings), newVersion); err != nil {
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

	// !!!
	// // Check that we're not skipping versions.
	// if !oldV.CanBump(newVersion) {
	//   return nil, errors.Errorf(
	//     "cannot upgrade directly from %s to %s", oldV.Version, newVersion)
	// }

	// Return the serialized form of the new version.
	newV := ClusterVersion{Version: newVersion}
	return protoutil.Marshal(&newV)
}

// ValidateGossipVersion is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSettingImpl) ValidateGossipUpdate(
	sv *settings.Values, rawProto []byte,
) error {
	var ver ClusterVersion
	if err := protoutil.Unmarshal(rawProto, &ver); err != nil {
		return err
	}
	st := sv.Opaque().(*Settings)
	return cv.validateSupportedVersionInner(st, ver.Version)
}

func (cv clusterVersionSettingImpl) validateSupportedVersionInner(
	st *Settings, ver roachpb.Version,
) error {
	if st.binaryMinSupportedVersion == (roachpb.Version{}) {
		panic("binaryMinSupportedVersion not set")
	}
	if st.binaryMaxSupportedVersion.Less(ver) {
		// TODO(tschottdorf): also ask gossip about other nodes.
		return errors.Errorf("cannot upgrade to %s: node running %s",
			ver, st.binaryMaxSupportedVersion)
	}
	if ver.Less(st.binaryMinSupportedVersion) {
		return errors.Errorf("node at %s cannot run %s (minimum version is %s)",
			st.binaryMaxSupportedVersion, ver, st.binaryMinSupportedVersion)
	}
	return nil
}

// SettingsListDefault is part of the StateMachineSettingImpl interface.
func (cv clusterVersionSettingImpl) SettingsListDefault() string {
	return BinaryServerVersion.String()
}

// !!!
// // versionTransformer is the transformer function for the version StateMachine.
// // It has access to the Settings struct via the opaque member of settings.Values.
// // The returned versionStringer must, when printed, only return strings that are
// // safe to include in diagnostics reporting.
// //
// // Args:
// // curRawProto: The current value of the setting - an encoded ClusterVersion
// //   proto. Can be empty if the setting is not yet set, in which case the
// //   "default" value will be used.
// // versionBump: A string indicating the proposed new value of the setting. Can
// //   be nil, in which case validation is performed and the current value (or the
// //   "default") is returned. Gossip updates pass the incoming gossipped value to
// //   curRawProto and nil for versionBump. SHOW CLUSTE SETTING passes the KV
// //   value as curRawProto and nil as versionBump.
// func versionTransformer(
//   sv *settings.Values, curRawProto []byte, versionBump *string,
// ) (newRawProto []byte, versionStringer interface{}, _ error) {
//   if curRawProto == nil && versionBump == nil {
//     panic("!!!")
//   }
//   opaque := sv.Opaque()
//   if opaque == settings.TestOpaque {
//     // This is a test where a cluster.Settings is not set up yet. In that case
//     // this function is ran only once, on initialization.
//     if curRawProto != nil || versionBump != nil {
//       panic("modifying version when TestOpaque is set")
//     }
//     return nil, nil, nil
//   }
//   s := opaque.(*Settings)
//   minSupportedVersion := s.Version.MinSupportedVersion
//   serverVersion := s.Version.ServerVersion
//
//   defer func() {
//     if versionStringer != nil {
//       versionStringer = (*stringedVersion)(versionStringer.(*ClusterVersion))
//     }
//   }()
//   var oldV ClusterVersion
//
//   // If no old value supplied, fill in the default.
//   if curRawProto == nil {
//     oldV = *s.Version.baseVersion.Load().(*ClusterVersion)
//     var err error
//     curRawProto, err = protoutil.Marshal(&oldV)
//     if err != nil {
//       return nil, nil, err
//     }
//   }
//
//   if err := protoutil.Unmarshal(curRawProto, &oldV); err != nil {
//     return nil, nil, err
//   }
//   if versionBump == nil {
//     // Round-trip the existing value, but only if it passes sanity
//     // checks. This is also the path taken when the setting gets updated
//     // via the gossip callback.
//     if serverVersion.Less(oldV.Version) {
//       log.Fatalf(context.TODO(), "node at %s cannot run at %s", serverVersion, oldV.Version)
//     }
//     if (oldV.Version != roachpb.Version{}) && oldV.Less(minSupportedVersion) {
//       log.Fatalf(context.TODO(), "node at %s cannot run at %s (minimum version is %s)", serverVersion, oldV.Version, minSupportedVersion)
//     }
//     return curRawProto, &oldV, nil
//   }
//
//   // We have a new proposed update to the value, validate it.
//   newVersion, err := roachpb.ParseVersion(*versionBump)
//   if err != nil {
//     return nil, nil, err
//   }
//   newV := ClusterVersion{Version: newVersion}
//
//   // Prevent cluster version upgrade until cluster.preserve_downgrade_option is reset.
//   if downgrade := preserveDowngradeVersion.Get(sv); downgrade != "" {
//     return nil, nil, errors.Errorf(
//       "cannot upgrade to %s: cluster.preserve_downgrade_option is set to %s",
//       newVersion, downgrade)
//   }
//
//   if newVersion.Less(oldV.Version) {
//     return nil, nil, errors.Errorf(
//       "versions cannot be downgraded (attempting to downgrade from %s to %s)",
//       oldV.Version, newVersion)
//   }
//
//   if oldV != (ClusterVersion{}) && !oldV.CanBump(newVersion) {
//     return nil, nil, errors.Errorf(
//       "cannot upgrade directly from %s to %s", oldV.Version, newVersion)
//   }
//
//   if serverVersion.Less(newVersion) {
//     // TODO(tschottdorf): also ask gossip about other nodes.
//     return nil, nil, errors.Errorf("cannot upgrade to %s: node running %s",
//       newVersion, serverVersion)
//   }
//
//   b, err := protoutil.Marshal(&newV)
//   return b, &newV, err
// }
