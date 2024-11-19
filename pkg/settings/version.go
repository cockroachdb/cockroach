// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"fmt"
)

// VersionSetting is the setting type that allows users to control the cluster
// version. It starts off at an initial version and takes into account the
// current version to validate proposed updates. This is (necessarily) tightly
// coupled with the setting implementation in pkg/clusterversion, and it's done
// through the VersionSettingImpl interface. We rely on the implementation to
// decode to and from raw bytes, and to perform the validation itself. The
// VersionSetting itself is then just the tiny shim that lets us hook into the
// rest of the settings machinery (by interfacing with Values, to load and store
// cluster versions).
//
// TODO(irfansharif): If the cluster version is no longer backed by gossip,
// maybe we should stop pretending it's a regular gossip-backed cluster setting.
// We could introduce new syntax here to motivate this shift.
type VersionSetting struct {
	impl VersionSettingImpl
	common
}

var _ internalSetting = &VersionSetting{}

// VersionSettingImpl is the interface bridging pkg/settings and
// pkg/clusterversion. See VersionSetting for additional commentary.
type VersionSettingImpl interface {
	// Decode takes in an encoded cluster version and returns it as the native
	// type (the clusterVersion proto). Except it does it through the
	// ClusterVersionImpl to avoid circular dependencies.
	Decode(val []byte) (ClusterVersionImpl, error)

	// Validate checks whether an version update is permitted. It takes in the
	// old and the proposed new value (both in encoded form). This is called by
	// SET CLUSTER SETTING.
	ValidateVersionUpgrade(ctx context.Context, sv *Values, oldV, newV []byte) error

	// ValidateBinaryVersions is a subset of Validate. It only checks that the
	// current binary supports the proposed version. This is called when the
	// version is being communicated to us by a different node (currently
	// through gossip).
	//
	// TODO(irfansharif): Update this comment when we stop relying on gossip to
	// propagate version bumps.
	ValidateBinaryVersions(ctx context.Context, sv *Values, newV []byte) error

	// SettingsListDefault returns the value that should be presented by
	// `./cockroach gen settings-list`
	SettingsListDefault() string
}

// ClusterVersionImpl is used to stub out the dependency on the ClusterVersion
// type (in pkg/clusterversion). The VersionSetting below is used to set
// ClusterVersion values, but we can't import the type directly due to the
// cyclical dependency structure.
type ClusterVersionImpl interface {
	fmt.Stringer

	// Encode encodes the ClusterVersion (using the protobuf encoding).
	Encode() []byte
}

// MakeVersionSetting instantiates a version setting instance. See
// VersionSetting for additional commentary.
func MakeVersionSetting(impl VersionSettingImpl) VersionSetting {
	return VersionSetting{impl: impl}
}

// Validate checks whether an version update is permitted. It takes in the
// old and the proposed new value (both in encoded form). This is called by
// SET CLUSTER SETTING.
func (v *VersionSetting) Validate(ctx context.Context, sv *Values, oldV, newV []byte) error {
	return v.impl.ValidateVersionUpgrade(ctx, sv, oldV, newV)
}

// SettingsListDefault returns the value that should be presented by
// `./cockroach gen settings-list`.
func (v *VersionSetting) SettingsListDefault() string {
	return v.impl.SettingsListDefault()
}

// Typ is part of the Setting interface.
func (*VersionSetting) Typ() string {
	// This is named "m" (instead of "v") for backwards compatibility reasons.
	return VersionSettingValueType
}

// VersionSettingValueType is the value type string (m originally for
// "migration") used in the system.settings table.
const VersionSettingValueType = "m"

// String is part of the Setting interface.
func (v *VersionSetting) String(sv *Values) string {
	cv := v.GetInternal(sv)
	if cv == nil {
		panic("unexpected nil value")
	}
	return cv.String()
}

// DefaultString returns the default value for the setting as a string.
func (v *VersionSetting) DefaultString() string {
	return encodedDefaultVersion
}

// Encoded is part of the NonMaskedSetting interface.
func (v *VersionSetting) Encoded(sv *Values) string {
	cv := v.GetInternal(sv)
	if cv == nil {
		panic("unexpected nil value")
	}
	return string(cv.Encode())
}

// EncodedDefault is part of the NonMaskedSetting interface.
func (v *VersionSetting) EncodedDefault() string {
	return encodedDefaultVersion
}

const encodedDefaultVersion = "unsupported"

// DecodeToString decodes and renders an encoded value.
func (v *VersionSetting) DecodeToString(encoded string) (string, error) {
	if encoded == encodedDefaultVersion {
		return encodedDefaultVersion, nil
	}
	cv, err := v.impl.Decode([]byte(encoded))
	if err != nil {
		return "", err
	}
	return cv.String(), nil
}

// GetInternal returns the setting's current value.
func (v *VersionSetting) GetInternal(sv *Values) ClusterVersionImpl {
	val := sv.getGeneric(v.slot)
	if val == nil {
		return nil
	}
	return val.(ClusterVersionImpl)
}

// SetInternal updates the setting's value in the provided Values container.
func (v *VersionSetting) SetInternal(ctx context.Context, sv *Values, newVal ClusterVersionImpl) {
	sv.setGeneric(ctx, v.slot, newVal)
}

// setToDefault is part of the internalSetting interface. This is a no-op for
// VersionSetting. They don't have defaults that they can go back to at any
// time.
func (v *VersionSetting) setToDefault(ctx context.Context, sv *Values) {}

// decodeAndSet is part of the internalSetting interface. We intentionally avoid
//
// We intentionally avoid updating the setting through this code path. The
// specific setting backed by VersionSetting is the cluster version setting,
// changes to which are propagated through direct RPCs to each node in the
// cluster instead of gossip. This is done using the BumpClusterVersion RPC.
func (v *VersionSetting) decodeAndSet(ctx context.Context, sv *Values, encoded string) error {
	return nil
}

// decodeAndSetDefaultOverride is part of the internalSetting interface.
//
// We intentionally avoid updating the setting through this code path. The
// specific setting backed by VersionSetting is the cluster version setting,
// changes to which are propagated through direct RPCs to each node in the
// cluster instead of gossip. This is done using the BumpClusterVersion RPC.
func (v *VersionSetting) decodeAndSetDefaultOverride(
	ctx context.Context, sv *Values, encoded string,
) error {
	return nil
}

// RegisterVersionSetting adds the provided version setting to the global
// registry.
func RegisterVersionSetting(
	class Class, key InternalKey, desc string, setting *VersionSetting, opts ...SettingOption,
) {
	register(class, key, desc, setting)
	setting.apply(opts)
}
