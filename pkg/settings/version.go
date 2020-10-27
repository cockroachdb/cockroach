// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings

import (
	"bytes"
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
type VersionSetting struct {
	impl VersionSettingImpl
	common
}

var _ Setting = &VersionSetting{}

// VersionSettingImpl is the interface bridging pkg/settings and
// pkg/clusterversion. See VersionSetting for additional commentary.
type VersionSettingImpl interface {
	// Decode takes in an encoded cluster version and returns it as the native
	// type (the ClusterVersion proto). Except it does it through the
	// ClusterVersionImpl to avoid circular dependencies.
	Decode(val []byte) (ClusterVersionImpl, error)

	// Validate checks whether an version update is permitted. It takes in the
	// old and the proposed new value (both in encoded form). This is called by
	// SET CLUSTER SETTING.
	Validate(ctx context.Context, sv *Values, oldV, newV []byte) ([]byte, error)

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

	// BeforeChange is called before an updated value for this setting is about
	// to be set in the Values container.
	BeforeChange(ctx context.Context, encodedVal []byte, sv *Values)
}

// ClusterVersionImpl is used to stub out the dependency on the ClusterVersion
// type (in pkg/clusterversion). The VersionSetting below is used to set
// ClusterVersion values, but we can't import the type directly due to the
// cyclical dependency structure.
type ClusterVersionImpl interface {
	ClusterVersionImpl()
	// We embed fmt.Stringer so to be able to later satisfy the `Setting`
	// interface (which requires us to return a string representation of the
	// current value of the setting)
	fmt.Stringer
}

// MakeVersionSetting instantiates a version setting instance. See
// VersionSetting for additional commentary.
func MakeVersionSetting(impl VersionSettingImpl) VersionSetting {
	return VersionSetting{impl: impl}
}

// Decode takes in an encoded cluster version and returns it as the native
// type (the ClusterVersion proto). Except it does it through the
// ClusterVersionImpl to avoid circular dependencies.
func (v *VersionSetting) Decode(val []byte) (ClusterVersionImpl, error) {
	return v.impl.Decode(val)
}

// Validate checks whether an version update is permitted. It takes in the
// old and the proposed new value (both in encoded form). This is called by
// SET CLUSTER SETTING.
func (v *VersionSetting) Validate(
	ctx context.Context, sv *Values, oldV, newV []byte,
) ([]byte, error) {
	return v.impl.Validate(ctx, sv, oldV, newV)
}

// SettingsListDefault returns the value that should be presented by
// `./cockroach gen settings-list`.
func (v *VersionSetting) SettingsListDefault() string {
	return v.impl.SettingsListDefault()
}

// Typ is part of the Setting interface.
func (*VersionSetting) Typ() string {
	return "v"
}

// String is part of the Setting interface.
func (v *VersionSetting) String(sv *Values) string {
	encV := []byte(v.Get(sv))
	if encV == nil {
		panic("unexpected nil value")
	}
	cv, err := v.impl.Decode(encV)
	if err != nil {
		panic(err)
	}
	return cv.String()
}

// Encoded is part of the WritableSetting interface.
func (v *VersionSetting) Encoded(sv *Values) string {
	return v.Get(sv)
}

// EncodedDefault is part of the WritableSetting interface.
func (v *VersionSetting) EncodedDefault() string {
	return "unsupported"
}

// Get retrieves the encoded value (in string form) in the setting. It panics if
// set() has not been previously called.
//
// TODO(irfansharif): This (along with `set`) below should be folded into one of
// the Setting interfaces, or be removed entirely. All readable settings
// implement it.
func (v *VersionSetting) Get(sv *Values) string {
	encV := v.GetInternal(sv)
	if encV == nil {
		panic(fmt.Sprintf("missing value for version setting in slot %d", v.getSlotIdx()))
	}
	return string(encV.([]byte))
}

// GetInternal returns the setting's current value.
func (v *VersionSetting) GetInternal(sv *Values) interface{} {
	return sv.getGeneric(v.getSlotIdx())
}

// SetInternal updates the setting's value in the provided Values container.
func (v *VersionSetting) SetInternal(sv *Values, newVal interface{}) {
	sv.setGeneric(v.getSlotIdx(), newVal)
}

// set lets the updater process set the provided value as the current version.
//
// TODO(irfansharif): This should really be folded into one of the Setting
// interfaces. All writable settings satisfy it.
func (v *VersionSetting) set(sv *Values, encodedVal []byte) error {
	// We use ValidateBinaryVersions here since we're receiving this value from
	// another node (that has already done the full validation elsewhere).
	if err := v.impl.ValidateBinaryVersions(context.TODO(), sv, encodedVal); err != nil {
		return err
	}

	curVal := v.GetInternal(sv)
	if curVal != nil && bytes.Equal(curVal.([]byte), encodedVal) {
		// We're already at the right version, there's nothing to do.
		return nil
	}

	// Invoke the registered callback, if any.
	v.impl.BeforeChange(context.TODO(), encodedVal, sv)

	v.SetInternal(sv, encodedVal)
	return nil
}

// setToDefault is part of the extendingSetting interface. This is a no-op for
// VersionSetting. They don't have defaults that they can go back to at any
// time.
//
// TODO(irfansharif): Is this true? Shouldn't the default here just the the
// version we initialize with?
func (v *VersionSetting) setToDefault(_ *Values) {}

// RegisterVersionSetting adds the provided version setting to the global
// registry.
func RegisterVersionSetting(key, desc string, setting *VersionSetting) {
	register(key, desc, setting)
}

// TestingRegisterVersionSetting is like RegisterVersionSetting,
// but it takes a VersionSettingImpl.
func TestingRegisterVersionSetting(key, desc string, impl VersionSettingImpl) *VersionSetting {
	setting := MakeVersionSetting(impl)
	register(key, desc, &setting)
	return &setting
}
