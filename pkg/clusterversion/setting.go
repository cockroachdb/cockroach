// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterversion

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// KeyVersionSetting is the "version" settings key.
const KeyVersionSetting = "version"

// version represents the cluster's "active version". This is a cluster setting,
// but a special one. It can only advance to higher and higher versions. The
// setting can be used to see if migrations are to be considered enabled or
// disabled through the isActive() method. All external usage of the cluster
// settings takes place through a Handle and `Initialize()`/`SetBeforeChange()`.
//
// During the node startup sequence, an initial version (persisted to the
// engines) is read and passed to `version.initialize`. It is only after that
// that `version.{activeVersion,isActive} can be called. In turn, the node
// usually registers itself as a callback to be notified of any further updates
// to the setting, which are also persisted.
//
// This dance is necessary because we cannot determine a safe default value for
// the version setting without looking at what's been persisted: The setting
// specifies the minimum binary version we have to expect to be in a mixed
// cluster with. We can't assume it is this binary's
// binaryMinSupportedVersion as the cluster could've started up earlier and
// enabled features that are no longer compatible it; we can't assume it's our
// binaryVersion as that would enable features that may trip up older versions
// running in the same cluster. Hence, only once we get word of the "safe"
// version to use can we allow moving parts that actually need to know what's
// going on.
//
// Additionally, whenever the version changes, we want to persist that update to
// wherever the caller to initialize() got the initial version from
// (typically a collection of `engine.Engine`s), which the caller will do by
// registering itself via setBeforeChange()`, which is invoked *before* exposing
// the new version to callers of `activeVersion()` and `isActive()`.
var version = registerClusterVersionSetting()

// clusterVersionSetting is the implementation of the 'version' setting. Like all
// setting structs, it is immutable, as Version is a global; all the state is
// maintained in a Handle instance.
type clusterVersionSetting struct {
	settings.StateMachineSetting
}

var _ settings.StateMachineSettingImpl = &clusterVersionSetting{}

// registerClusterVersionSetting creates a clusterVersionSetting and registers
// it with the cluster settings registry.
func registerClusterVersionSetting() *clusterVersionSetting {
	s := makeClusterVersionSetting()
	s.StateMachineSetting.SetReportable(true)
	settings.RegisterStateMachineSetting(
		KeyVersionSetting,
		"set the active cluster version in the format '<major>.<minor>'", // hide optional `-<unstable>,
		&s.StateMachineSetting)
	s.SetVisibility(settings.Public)
	return s
}

func makeClusterVersionSetting() *clusterVersionSetting {
	s := &clusterVersionSetting{}
	s.StateMachineSetting = settings.MakeStateMachineSetting(s)
	return s
}

// initialize initializes cluster version. Before this method has been called,
// usage of the version is illegal and leads to a fatal error.
func (cv *clusterVersionSetting) initialize(
	ctx context.Context, version roachpb.Version, sv *settings.Values,
) error {
	if ver := cv.activeVersionOrEmpty(ctx, sv); ver != (ClusterVersion{}) {
		// Allow initializing a second time as long as it's not regressing.
		//
		// This is useful in tests that use MakeTestingClusterSettings() which
		// initializes the version, and the start a server which again
		// initializes it once more.
		//
		// It's also used in production code during bootstrap, where the version
		// is first initialized to BinaryMinSupportedVersion and then
		// re-initialized to BootstrapVersion (=BinaryVersion).
		if version.Less(ver.Version) {
			return errors.AssertionFailedf("cannot initialize version to %s because already set to: %s",
				version, ver)
		}
		if version == ver.Version {
			// Don't trigger callbacks, etc, a second time.
			return nil
		}
		// Now version > ver.Version.
	}
	if err := cv.validateSupportedVersionInner(ctx, version, sv); err != nil {
		return err
	}

	// Return the serialized form of the new version.
	newV := ClusterVersion{Version: version}
	encoded, err := protoutil.Marshal(&newV)
	if err != nil {
		return err
	}
	cv.SetInternal(sv, encoded)
	return nil
}

// activeVersion returns the cluster's current active version: the minimum
// cluster version the caller may assume is in effect.
//
// activeVersion fatals if the version has not been initialized.
func (cv *clusterVersionSetting) activeVersion(
	ctx context.Context, sv *settings.Values,
) ClusterVersion {
	ver := cv.activeVersionOrEmpty(ctx, sv)
	if ver == (ClusterVersion{}) {
		log.Fatalf(ctx, "version not initialized")
	}
	return ver
}

// activeVersionOrEmpty is like activeVersion, but returns an empty version if
// the active version was not initialized.
func (cv *clusterVersionSetting) activeVersionOrEmpty(
	ctx context.Context, sv *settings.Values,
) ClusterVersion {
	encoded := cv.GetInternal(sv)
	if encoded == nil {
		return ClusterVersion{}
	}
	var curVer ClusterVersion
	if err := protoutil.Unmarshal(encoded.([]byte), &curVer); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
	return curVer
}

// isActive returns true if the features of the supplied version key are active
// at the running version. See comment on Handle.IsActive for intended usage.
func (cv *clusterVersionSetting) isActive(
	ctx context.Context, sv *settings.Values, versionKey VersionKey,
) bool {
	return cv.activeVersion(ctx, sv).IsActive(versionKey)
}

// setBeforeChange registers a callback to be called before the cluster version
// is updated. The new cluster version will only become "visible" after the
// callback has returned.
//
// The callback can be set at most once.
func (cv *clusterVersionSetting) setBeforeChange(
	ctx context.Context, cb func(ctx context.Context, newVersion ClusterVersion), sv *settings.Values,
) {
	vh := sv.Opaque().(Handle)
	h := vh.(*handleImpl)
	h.beforeClusterVersionChangeMu.Lock()
	defer h.beforeClusterVersionChangeMu.Unlock()
	if h.beforeClusterVersionChangeMu.cb != nil {
		log.Fatalf(ctx, "beforeClusterVersionChange already set")
	}
	h.beforeClusterVersionChangeMu.cb = cb
}

// Decode is part of the StateMachineSettingImpl interface.
func (cv *clusterVersionSetting) Decode(val []byte) (interface{}, error) {
	var clusterVersion ClusterVersion
	if err := protoutil.Unmarshal(val, &clusterVersion); err != nil {
		return "", err
	}
	return clusterVersion, nil
}

// DecodeToString is part of the StateMachineSettingImpl interface.
func (cv *clusterVersionSetting) DecodeToString(val []byte) (string, error) {
	clusterVersion, err := cv.Decode(val)
	if err != nil {
		return "", err
	}
	return clusterVersion.(ClusterVersion).Version.String(), nil
}

// ValidateLogical is part of the StateMachineSettingImpl interface.
func (cv *clusterVersionSetting) ValidateLogical(
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
func (cv *clusterVersionSetting) ValidateGossipUpdate(
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

// SettingsListDefault is part of the StateMachineSettingImpl interface.
func (cv *clusterVersionSetting) SettingsListDefault() string {
	return binaryVersion.String()
}

// BeforeChange is part of the StateMachineSettingImpl interface
func (cv *clusterVersionSetting) BeforeChange(
	ctx context.Context, encodedVal []byte, sv *settings.Values,
) {
	var clusterVersion ClusterVersion
	if err := protoutil.Unmarshal(encodedVal, &clusterVersion); err != nil {
		log.Fatalf(ctx, "failed to unmarshall version: %s", err)
	}

	vh := sv.Opaque().(Handle)
	h := vh.(*handleImpl)
	h.beforeClusterVersionChangeMu.Lock()
	if cb := h.beforeClusterVersionChangeMu.cb; cb != nil {
		cb(ctx, clusterVersion)
	}
	h.beforeClusterVersionChangeMu.Unlock()
}

func (cv *clusterVersionSetting) validateSupportedVersionInner(
	ctx context.Context, ver roachpb.Version, sv *settings.Values,
) error {
	vh := sv.Opaque().(Handle)
	if vh.BinaryMinSupportedVersion() == (roachpb.Version{}) {
		panic("BinaryMinSupportedVersion not set")
	}
	if vh.BinaryVersion().Less(ver) {
		// TODO(tschottdorf): also ask gossip about other nodes.
		return errors.Errorf("cannot upgrade to %s: node running %s",
			ver, vh.BinaryVersion())
	}
	if ver.Less(vh.BinaryMinSupportedVersion()) {
		return errors.Errorf("node at %s cannot run %s (minimum version is %s)",
			vh.BinaryVersion(), ver, vh.BinaryMinSupportedVersion())
	}
	return nil
}

var preserveDowngradeVersion = registerPreserveDowngradeVersionSetting()

func registerPreserveDowngradeVersionSetting() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		"cluster.preserve_downgrade_option",
		"disable (automatic or manual) cluster version upgrade from the specified version until reset",
		"",
		func(sv *settings.Values, s string) error {
			if sv == nil || s == "" {
				return nil
			}
			clusterVersion := version.activeVersion(context.TODO(), sv).Version
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
}
