// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cluster

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
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

	Tracer        *tracing.Tracer
	ExternalIODir string

	Initialized bool
}

// NoSettings is used when a func requires a Settings but none is available
// (for example, a CLI subcommand that does not connect to a cluster).
var NoSettings *Settings // = nil

// KeyVersionSetting is the "version" settings key.
const KeyVersionSetting = "version"

var version = settings.RegisterStateMachineSetting(KeyVersionSetting,
	"set the active cluster version in the format '<major>.<minor>'.", // hide optional `-<unstable>`
	settings.TransformerFn(versionTransformer),
)

// InitializeVersion initializes the Version field of this setting. Before this
// method has been called, usage of the Version field is illegal and leads to a
// fatal error.
func (s *Settings) InitializeVersion(cv ClusterVersion) error {
	b, err := protoutil.Marshal(&cv)
	if err != nil {
		return err
	}
	// Note that we don't call `updater.ResetRemaining()`.
	updater := settings.NewUpdater(&s.SV)
	if err := updater.Set(KeyVersionSetting, string(b), version.Typ()); err != nil {
		return err
	}
	s.Version.baseVersion.Store(&cv)
	return nil
}

// An ExposedClusterVersion exposes a cluster-wide minimum version which is
// assumed to be supported by all nodes. This in turn allows features which are
// incompatible with older versions to be used safely.
type ExposedClusterVersion struct {
	MinSupportedVersion roachpb.Version
	ServerVersion       roachpb.Version
	baseVersion         atomic.Value // stores *ClusterVersion
	cb                  func(ClusterVersion)
}

// IsInitialized returns true if the cluster version has been initialized and is
// ready for use.
func (ecv *ExposedClusterVersion) IsInitialized() bool {
	return *ecv.baseVersion.Load().(*ClusterVersion) != ClusterVersion{}
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
	v := *ecv.baseVersion.Load().(*ClusterVersion)
	if (v == ClusterVersion{}) {
		log.Fatal(context.Background(), "Version() was called before having been initialized")
	}
	return v
}

// HasBeenInitialized returns whether the cluster version has been initialized
// yet and if Version can be safely called.
func (ecv *ExposedClusterVersion) HasBeenInitialized() bool {
	v := *ecv.baseVersion.Load().(*ClusterVersion)
	return v != ClusterVersion{}
}

// BootstrapVersion returns the version a newly initialized cluster should have.
func (ecv *ExposedClusterVersion) BootstrapVersion() ClusterVersion {
	return ClusterVersion{
		MinimumVersion: ecv.ServerVersion,
		UseVersion:     ecv.ServerVersion,
	}
}

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

// IsMinSupported returns true if the features of the supplied version will be
// permanently available (i.e. cannot be downgraded away).
func (ecv *ExposedClusterVersion) IsMinSupported(versionKey VersionKey) bool {
	return ecv.Version().IsMinSupported(versionKey)
}

// CheckVersion is like IsMinSupported but returns an appropriate error in the
// case of a cluster version which is too low.
func (ecv *ExposedClusterVersion) CheckVersion(versionKey VersionKey, feature string) error {
	if !ecv.Version().IsMinSupported(versionKey) {
		return pgerror.NewErrorf(
			pgerror.CodeFeatureNotSupportedError,
			"cluster version does not support %s (>= %s required)",
			feature,
			VersionByKey(versionKey).String(),
		)
	}
	return nil
}

// MakeTestingClusterSettings returns a Settings object that has had its version
// initialized to BinaryServerVersion.
func MakeTestingClusterSettings() *Settings {
	return MakeTestingClusterSettingsWithVersion(BinaryServerVersion, BinaryServerVersion)
}

// MakeTestingClusterSettingsWithVersion returns a Settings object that has had
// its version initialized to the provided version configuration.
func MakeTestingClusterSettingsWithVersion(minVersion, serverVersion roachpb.Version) *Settings {
	st := MakeClusterSettings(minVersion, serverVersion)
	cv := st.Version.BootstrapVersion()
	// Initialize with all features enabled.
	if err := st.InitializeVersion(cv); err != nil {
		log.Fatalf(context.TODO(), "unable to initialize version: %s", err)
	}
	return st
}

// MakeClusterSettings makes a new ClusterSettings object for the given minimum
// supported and server version, respectively.
func MakeClusterSettings(minVersion, serverVersion roachpb.Version) *Settings {
	s := &Settings{}

	// Initialize the setting. Note that baseVersion starts out with the zero
	// cluster version, for which the transformer accepts any new version. After
	// that, it'll only accept "valid bumps". We use this to initialize the
	// variable lazily, after we have read the current version from the engines.
	// After that, updates come from Gossip and need to be compatible with the
	// engine version.
	s.Version.MinSupportedVersion = minVersion
	s.Version.ServerVersion = serverVersion
	s.Version.baseVersion.Store(&ClusterVersion{})
	sv := &s.SV
	sv.Init(s)

	s.Tracer = tracing.NewTracer()
	s.Tracer.Configure(sv)

	version.SetOnChange(sv, func() {
		_, obj, err := version.Validate(sv, []byte(version.Get(sv)), nil)
		if err != nil {
			log.Fatal(context.Background(), err)
		}
		newV := *((*ClusterVersion)(obj.(*stringedVersion)))

		// Call callback before exposing the new version to callers of
		// IsActive() and Version(). Don't do this if the new version is
		// trivial, which is the case as the setting is initialized.
		if (newV != ClusterVersion{}) && s.Version.cb != nil {
			s.Version.cb(newV)
		}
		s.Version.baseVersion.Store(&newV)
	})

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

type stringedVersion ClusterVersion

func (sv *stringedVersion) String() string {
	if sv == nil {
		sv = &stringedVersion{}
	}
	return sv.MinimumVersion.String()
}

// versionTransformer is the transformer function for the version StateMachine.
// It has access to the Settings struct via the opaque member of settings.Values.
// The returned versionStringer must, when printed, only return strings that are
// safe to include in diagnostics reporting.
func versionTransformer(
	sv *settings.Values, curRawProto []byte, versionBump *string,
) (newRawProto []byte, versionStringer interface{}, _ error) {
	opaque := sv.Opaque()
	if opaque == settings.TestOpaque {
		// This is a test where a cluster.Settings is not set up yet. In that case
		// this function is ran only once, on initialization.
		if curRawProto != nil || versionBump != nil {
			panic("modifying version when TestOpaque is set")
		}
		return nil, nil, nil
	}
	s := opaque.(*Settings)
	minSupportedVersion := s.Version.MinSupportedVersion
	serverVersion := s.Version.ServerVersion

	defer func() {
		if versionStringer != nil {
			versionStringer = (*stringedVersion)(versionStringer.(*ClusterVersion))
		}
	}()
	var oldV ClusterVersion

	// If no old value supplied, fill in the default.
	if curRawProto == nil {
		oldV = *s.Version.baseVersion.Load().(*ClusterVersion)
		var err error
		curRawProto, err = protoutil.Marshal(&oldV)
		if err != nil {
			return nil, nil, err
		}
	}

	if err := protoutil.Unmarshal(curRawProto, &oldV); err != nil {
		return nil, nil, err
	}
	if versionBump == nil {
		// Round-trip the existing value, but only if it passes sanity
		// checks. This is also the path taken when the setting gets updated
		// via the gossip callback.
		if serverVersion.Less(oldV.MinimumVersion) {
			log.Fatalf(context.TODO(), "node at %s cannot run at %s", serverVersion, oldV.MinimumVersion)
		}
		if (oldV.MinimumVersion != roachpb.Version{}) && oldV.MinimumVersion.Less(minSupportedVersion) {
			log.Fatalf(context.TODO(), "node at %s cannot run at %s (minimum version is %s)", serverVersion, oldV.MinimumVersion, minSupportedVersion)
		}
		return curRawProto, &oldV, nil
	}

	// We have a new proposed update to the value, validate it.
	minVersion, err := roachpb.ParseVersion(*versionBump)
	if err != nil {
		return nil, nil, err
	}
	newV := oldV
	newV.UseVersion = minVersion
	newV.MinimumVersion = minVersion

	if minVersion.Less(oldV.MinimumVersion) {
		return nil, nil, errors.Errorf("cannot downgrade from %s to %s", oldV.MinimumVersion, minVersion)
	}

	if oldV != (ClusterVersion{}) && !oldV.MinimumVersion.CanBump(minVersion) {
		return nil, nil, errors.Errorf("cannot upgrade directly from %s to %s", oldV.MinimumVersion, minVersion)
	}

	if serverVersion.Less(minVersion) {
		// TODO(tschottdorf): also ask gossip about other nodes.
		return nil, nil, errors.Errorf("cannot upgrade to %s: node running %s",
			minVersion, serverVersion)
	}

	b, err := protoutil.Marshal(&newV)
	return b, &newV, err
}
