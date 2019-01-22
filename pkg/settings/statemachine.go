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
)

// // !!!
// // A TransformerFn encapsulates the logic of a StateMachineSetting.
// type TransformerFn func(sv *Values, old []byte, update *string) (finalV []byte, finalObj interface{}, _ error)

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
	ValidateLogical(sv *Values, old []byte, newV string) ([]byte, error)

	// ValidateGossipUpdate performs fewer validations than ValidateLogical.
	// For the cluster version setting, it only checks that the current binary
	// supports the proposed version. This is called when the version is being
	// communicated to us by a different node.
	ValidateGossipUpdate(sv *Values, val []byte) error

	// SettingsListDefault returns the value that should be presented by
	// `./cockroach gen settings-list`
	SettingsListDefault() string
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
// TransformerFn backing this StateMachineSetting. It is a method that takes
//
// - the Values instance
// - the known previous encoded value, if any (i.e. the current state)
// - the update the user wants to make to the encoded struct (i.e. the desired
//   transition)
//
// and returns
//
// - the new encoded value (i.e. the new state)
// - an interface backed by a "user-friendly" representation of the new state
//   (i.e. something that can be printed)
// - an error if the input was illegal.
//
// Furthermore,
//
// - with a `nil` input byte slice, the default value (hidden in the
//   transformer) is used internally
// - with a `nil` update, the previous (or default) value is returned
// - with both non-nil, a transition from the old state using the given update
//   is carried out, and the new state and representation (or an error)
//   returned.
//
// The opaque member of Values can be used to associate a higher-level object to
// the Values instance (making it accessible to the function).
//
// !!! comment
// Updates to the setting via an Updater validate the new state syntactically,
// but not semantically. Users must call Validate with the authoritative
// previous state, the suggested state transition, and then overwrite the state
// machine with the result.
type StateMachineSetting struct {
	// !!! transformer TransformerFn
	impl StateMachineSettingImpl
	common
}

var _ Setting = &StateMachineSetting{}

// Decode takes in an encoded value and returns it as the native type of the
// setting in question. For the Version setting, this is a ClusterVersion proto.
func (s *StateMachineSetting) Decode(val []byte) (interface{}, error) {
	return s.impl.Decode(val)
}

func (s *StateMachineSetting) String(sv *Values) string {
	encV := []byte(s.Get(sv))
	if encV == nil {
		// !!! panic here? I think Get doesn't return nil any more.
		return "not set"
	}
	str, err := s.impl.DecodeToString(encV)
	if err != nil {
		panic(err)
	}
	return str
	// !!!
	// encV := []byte(s.Get(sv))
	// _, iface, err := s.transformer(sv, encV, nil /* update */)
	// if err != nil {
	//   panic(err)
	// }
	// return fmt.Sprintf("%v", iface)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*StateMachineSetting) Typ() string {
	return "m"
}

// !!! comment
// Get retrieves the (encoded) value in the setting (or the encoded default).
func (s *StateMachineSetting) Get(sv *Values) string {
	encV := sv.getGeneric(s.slotIdx)
	if encV == nil {
		panic("!!! nil Get")
		// !!!
		// defV, _, err := s.transformer(sv, nil /* old */, nil /* update */)
		// if err != nil {
		//   panic(err)
		// }
		// return string(defV)
	}
	return string(encV.([]byte))
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

// !!! comment. Maybe method can go away.
// Validate that the state machine accepts the user input. Returns new encoded
// state, unencoded state, or an error.
func (s *StateMachineSetting) Validate(
	sv *Values, old []byte, update string,
) ([]byte, error) {
	return s.impl.ValidateLogical(sv, old, update)
	// !!! return s.transformer(sv, old, update)
}

// set is part of the Setting interface.
func (s *StateMachineSetting) set(sv *Values, encodedVal []byte) error {
	if err := s.impl.ValidateGossipUpdate(sv, encodedVal); err != nil {
		return err
	}
	curVal := sv.getGeneric(s.slotIdx)
	if curVal != nil {
		if bytes.Equal(curVal.([]byte), encodedVal) {
			// Nothing to do.
			return nil
		}
	}
	// !!!
	// if _, _, err := s.transformer(sv, encodedVal, nil /* update */); err != nil {
	//   return err
	// }
	// if bytes.Equal([]byte(s.Get(sv)), encodedVal) {
	//   return nil
	// }
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
func RegisterStateMachineSetting(key, desc string, impl StateMachineSettingImpl) *StateMachineSetting {
	setting := &StateMachineSetting{
		impl: impl,
	}
	register(key, desc, setting)
	return setting
}
