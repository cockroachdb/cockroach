// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// KeyVersionSetting is the "version" settings key.
const KeyVersionSetting = "version"

// version represents the cluster's "active version". This is a cluster setting,
// but a special one. It can only advance to higher and higher versions. The
// setting can be used to see if upgrades are to be considered enabled or
// disabled through the `isActive()` method. All external usage of the cluster
// settings takes place through a Handle and `Initialize()`.
//
// During the node startup sequence, an initial version (persisted to the
// engines) is read and passed to `version.initialize`. It is only after that
// that `version.{activeVersion,isActive} can be called. Further updates to the
// setting also need to be persisted before informing the setting itself about
// it.
//
// This dance is necessary because we cannot determine a safe default value for
// the version setting without looking at what's been persisted: The setting
// specifies the minimum binary version we have to expect to be in a mixed
// cluster with. We can't assume it is this binary's minSupportedVersion as the
// cluster could've started up earlier and enabled features that are no longer
// compatible it; we can't assume it's our latestVersion as that would enable
// features that may trip up older versions running in the same cluster. Hence,
// only once we get word of the "safe" version to use can we allow moving parts
// that actually need to know what's going on.
var version = registerClusterVersionSetting()

// clusterVersionSetting is the implementation of the 'version' setting. Like all
// setting structs, it is immutable, as Version is a global; all the state is
// maintained in a Handle instance.
type clusterVersionSetting struct {
	settings.VersionSetting
}

var _ settings.VersionSettingImpl = &clusterVersionSetting{}

// registerClusterVersionSetting creates a clusterVersionSetting and registers
// it with the cluster settings registry.
func registerClusterVersionSetting() *clusterVersionSetting {
	s := &clusterVersionSetting{}
	s.VersionSetting = settings.MakeVersionSetting(s)
	settings.RegisterVersionSetting(
		settings.ApplicationLevel,
		KeyVersionSetting,
		"set the active cluster version in the format '<major>.<minor>'", // hide optional `-<internal>,
		&s.VersionSetting,
		settings.WithPublic,
		settings.WithReportable(true),
	)
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
		// is first initialized to MinSupportedVersion and then re-initialized to
		// BootstrapVersion (=LatestVersion).
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
	if err := cv.validateBinaryVersions(version, sv); err != nil {
		return err
	}

	// Return the serialized form of the new version.
	newV := ClusterVersion{Version: version}
	cv.SetInternal(ctx, sv, newV)
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
	curVer := cv.GetInternal(sv)
	if curVer == nil {
		return ClusterVersion{}
	}
	return curVer.(ClusterVersion)
}

// isActive returns true if the features of the supplied version key are active
// at the running version. See comment on Handle.IsActive for intended usage.
func (cv *clusterVersionSetting) isActive(
	ctx context.Context, sv *settings.Values, versionKey Key,
) bool {
	return cv.activeVersion(ctx, sv).IsActive(versionKey)
}

// Decode is part of the VersionSettingImpl interface.
func (cv *clusterVersionSetting) Decode(val []byte) (settings.ClusterVersionImpl, error) {
	var clusterVersion ClusterVersion
	if err := clusterVersion.Unmarshal(val); err != nil {
		return nil, err
	}
	return clusterVersion, nil
}

// Validate is part of the VersionSettingImpl interface.
func (cv *clusterVersionSetting) ValidateVersionUpgrade(
	_ context.Context, sv *settings.Values, curRawProto, newRawProto []byte,
) error {
	var newCV ClusterVersion
	if err := newCV.Unmarshal(newRawProto); err != nil {
		return err
	}

	if err := cv.validateBinaryVersions(newCV.Version, sv); err != nil {
		return err
	}

	var oldCV ClusterVersion
	if err := oldCV.Unmarshal(curRawProto); err != nil {
		return err
	}

	// Versions cannot be downgraded.
	if newCV.Version.Less(oldCV.Version) {
		return errors.Errorf(
			"versions cannot be downgraded (attempting to downgrade from %s to %s)",
			oldCV.Version, newCV.Version)
	}

	// Prevent cluster version upgrade until cluster.preserve_downgrade_option
	// is reset.
	if downgrade := PreserveDowngradeVersion.Get(sv); downgrade != "" {
		return errors.Errorf(
			"cannot upgrade to %s: cluster.preserve_downgrade_option is set to %s",
			newCV.Version, downgrade)
	}

	return nil
}

// ValidateBinaryVersions is part of the VersionSettingImpl interface.
func (cv *clusterVersionSetting) ValidateBinaryVersions(
	ctx context.Context, sv *settings.Values, rawProto []byte,
) (retErr error) {
	defer func() {
		// This implementation of ValidateBinaryVersions never returns errors.
		// Instead, we crash. Not being able to update our version to what the
		// rest of the cluster is running is a serious issue.
		if retErr != nil {
			log.Fatalf(ctx, "failed to validate version upgrade: %s", retErr)
		}
	}()

	var ver ClusterVersion
	if err := ver.Unmarshal(rawProto); err != nil {
		return err
	}
	return cv.validateBinaryVersions(ver.Version, sv)
}

// SettingsListDefault is part of the VersionSettingImpl interface.
func (cv *clusterVersionSetting) SettingsListDefault() string {
	return Latest.String()
}

func (cv *clusterVersionSetting) validateBinaryVersions(
	ver roachpb.Version, sv *settings.Values,
) error {
	vh := sv.Opaque().(Handle)
	if vh.MinSupportedVersion() == (roachpb.Version{}) {
		panic("MinSupportedVersion not set")
	}
	if vh.LatestVersion().Less(ver) {
		// TODO(tschottdorf): also ask gossip about other nodes.
		return errors.Errorf("cannot upgrade to %s: node running %s",
			ver, vh.LatestVersion())
	}
	if ver.Less(vh.MinSupportedVersion()) {
		return errors.Errorf("node at %s cannot run %s (minimum version is %s)",
			vh.LatestVersion(), ver, vh.MinSupportedVersion())
	}
	return nil
}

var PreserveDowngradeVersion = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"cluster.preserve_downgrade_option",
	"disable (automatic or manual) cluster version upgrade from the specified version until reset",
	"",
	settings.WithValidateString(func(sv *settings.Values, s string) error {
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
	}),
	settings.WithReportable(true),
	settings.WithPublic,
)

var metaPreserveDowngradeLastUpdated = metric.Metadata{
	Name:        "cluster.preserve-downgrade-option.last-updated",
	Help:        "Unix timestamp of last updated time for cluster.preserve_downgrade_option",
	Measurement: "Timestamp",
	Unit:        metric.Unit_TIMESTAMP_SEC,
}

// RegisterOnVersionChangeCallback is a callback function that updates the
// cluster.preserve-downgrade-option.last-updated when the
// cluster.preserve_downgrade_option settings is changed.
func RegisterOnVersionChangeCallback(
	sv *settings.Values, preserveDowngradeLastUpdatedMetric *metric.Gauge,
) {
	PreserveDowngradeVersion.SetOnChange(sv, func(ctx context.Context) {
		var value int64
		downgrade := PreserveDowngradeVersion.Get(sv)
		if downgrade != "" {
			value = timeutil.Now().Unix()
		}
		preserveDowngradeLastUpdatedMetric.Update(value)
	})
}

// Metrics defines the settings tracked in prometheus.
type Metrics struct {
	// PreserveDowngradeLastUpdated is a metric gauge that measures the
	// time the cluster.preserve_downgrade_option was last updated.
	PreserveDowngradeLastUpdated *metric.Gauge
}

// MakeMetrics is a function that creates the metrics defined in the Metrics
// struct.
func MakeMetricsAndRegisterOnVersionChangeCallback(sv *settings.Values) Metrics {
	gauge := metric.NewGauge(metaPreserveDowngradeLastUpdated)
	RegisterOnVersionChangeCallback(sv, gauge)
	return Metrics{
		PreserveDowngradeLastUpdated: gauge,
	}
}

// AutoUpgradeEnabled is used to enable and disable automatic upgrade.
var AutoUpgradeEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"cluster.auto_upgrade.enabled",
	"disable automatic cluster version upgrade until reset",
	true,
	settings.WithReportable(true),
	settings.WithPublic,
)
