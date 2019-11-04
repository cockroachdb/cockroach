// Copyright 2017 The Cockroach Authors.
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

// StateMachineSettingImpl provides the setting-specific parts of a
// StateMachineSetting. The StateMachineSetting is in charge of interacting with
// the Values (loading and saving state) and StateMachineSettingImpl is in
// charge to converting to/from strings and performing validations.
type StateMachineSettingImpl interface {
	// Decode takes in an encoded value and returns it as the native type of the
	// setting in question. For the Version setting, this is a ClusterVersion
	// proto.
	Decode(val []byte) (interface{}, error)

	// DecodeToString takes in an encoded value and returns its string
	// representation.
	DecodeToString(val []byte) (string, error)

	// ValidateLogical checks whether an update is permitted. It takes in the old
	// (encoded) value and the proposed new value (as a string to be parsed).
	// This is called by SET CLUSTER SETTING.
	ValidateLogical(ctx context.Context, sv *Values, old []byte, newV string) ([]byte, error)

	// ValidateGossipUpdate performs fewer validations than ValidateLogical.
	// For the cluster version setting, it only checks that the current binary
	// supports the proposed version. This is called when the version is being
	// communicated to us by a different node.
	ValidateGossipUpdate(ctx context.Context, sv *Values, val []byte) error

	// SettingsListDefault returns the value that should be presented by
	// `./cockroach gen settings-list`
	SettingsListDefault() string

	// BeforeChange is called before an updated value for this setting is about to
	// be set on the values container.
	BeforeChange(ctx context.Context, encodedVal []byte, sv *Values)
}

// A StateMachineSetting is a setting that keeps a state machine driven by user
// input.
//
// For (a nonsensical) example, a StateMachineSetting can be used to maintain an
// encoded protobuf containing an integer that the user can only increment by 3
// if the int is odd and by two if it is even. More generally, the setting
// starts from an initial state, and can take the current state into account
// when determining user input. Initially this is motivated for use in cluster
// version upgrades.
//
// The state machine as well as its encoding are represented by the
// StateMachineSettingImpl backing this StateMachineSetting; it is in charge to
// converting to/from strings and performing validations.
type StateMachineSetting struct {
	impl StateMachineSettingImpl
	common
}

var _ Setting = &StateMachineSetting{}

// MakeStateMachineSetting creates a StateMachineSetting.
func MakeStateMachineSetting(impl StateMachineSettingImpl) StateMachineSetting {
	return StateMachineSetting{impl: impl}
}

// Decode takes in an encoded value and returns it as the native type of the
// setting in question. For the Version setting, this is a ClusterVersion proto.
func (s *StateMachineSetting) Decode(val []byte) (interface{}, error) {
	return s.impl.Decode(val)
}

func (s *StateMachineSetting) String(sv *Values) string {
	encV := []byte(s.Get(sv))
	if encV == nil {
		panic("unexpected nil value")
	}
	str, err := s.impl.DecodeToString(encV)
	if err != nil {
		panic(err)
	}
	return str
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*StateMachineSetting) Typ() string {
	return "m"
}

// Get retrieves the (encoded) value in the setting. Get panics if set( ) has
// not been previously called.
func (s *StateMachineSetting) Get(sv *Values) string {
	encV := sv.getGeneric(s.slotIdx)
	if encV == nil {
		panic(fmt.Sprintf("missing value for state machine in slot %d", s.slotIdx))
	}
	return string(encV.([]byte))
}

// GetInternal returns the setting's current value.
func (s *StateMachineSetting) GetInternal(sv *Values) interface{} {
	return sv.getGeneric(s.slotIdx)
}

// SetInternal updates the setting's value in the sv container.
func (s *StateMachineSetting) SetInternal(sv *Values, newVal interface{}) {
	sv.setGeneric(s.getSlotIdx(), newVal)
}

// SettingsListDefault returns the value that should be presented by
// `./cockroach gen settings-list`
func (s *StateMachineSetting) SettingsListDefault() string {
	return s.impl.SettingsListDefault()
}

// Encoded is part of the Setting interface.
func (s *StateMachineSetting) Encoded(sv *Values) string {
	return s.Get(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (s *StateMachineSetting) EncodedDefault() string {
	return "unsupported"
}

// Validate that the state machine accepts the user input. Returns new encoded
// state.
func (s *StateMachineSetting) Validate(
	ctx context.Context, sv *Values, old []byte, update string,
) ([]byte, error) {
	return s.impl.ValidateLogical(ctx, sv, old, update)
}

// set is part of the Setting interface.
func (s *StateMachineSetting) set(sv *Values, encodedVal []byte) error {
	if err := s.impl.ValidateGossipUpdate(context.TODO(), sv, encodedVal); err != nil {
		return err
	}
	curVal := sv.getGeneric(s.slotIdx)
	if curVal != nil {
		if bytes.Equal(curVal.([]byte), encodedVal) {
			// Nothing to do.
			return nil
		}
	}
	s.impl.BeforeChange(context.TODO(), encodedVal, sv)
	sv.setGeneric(s.slotIdx, encodedVal)
	return nil
}

// setToDefault is part of the Setting interface.
//
// This is a no-op for StateMachineSettings. They don't have defaults that they
// can go back to at any time.
func (s *StateMachineSetting) setToDefault(_ *Values) {}

// RegisterStateMachineSetting registers a StateMachineSetting. See the comment
// for StateMachineSetting for details.
func RegisterStateMachineSetting(key, desc string, setting *StateMachineSetting) {
	register(key, desc, setting)
}

// RegisterStateMachineSettingImpl is like RegisterStateMachineSetting,
// but it takes a StateMachineSettingImpl.
func RegisterStateMachineSettingImpl(
	key, desc string, impl StateMachineSettingImpl,
) *StateMachineSetting {
	setting := MakeStateMachineSetting(impl)
	register(key, desc, &setting)
	return &setting
}
