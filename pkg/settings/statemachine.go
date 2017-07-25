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

package settings

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

// A TransformerFn encapsulates the logic of a StatemachineSetting.
type TransformerFn func(old []byte, update *string) (finalV []byte, finalObj interface{}, _ error)

// A StatemachineSetting is a setting that keeps a state machine driven by user
// input.
//
// For (a nonsensical) example, a StatemachineSetting can be used to maintain an
// encoded protobuf containing an integer that the user can only increment by 3
// if the int is odd and by 2 if it is equal. More generally, the setting starts
// from an initial state, and can take the current state into account when
// determining user input. Initially this is motivated for use in cluster
// version upgrades.
//
// The state machine as well as its encoding are represented by the
// TransformerFn backing this StatemachineSetting. It is a method that takes
//
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
// Updates to the setting via an Updater validate the new state syntactically,
// but not semantically. Users must call Validate with the authoritative
// previous state, the suggested state transition, and then overwrite the state
// machine with the result.
type StatemachineSetting struct {
	v           atomic.Value // []byte (marshalled state)
	transformer TransformerFn
	common
}

var _ Setting = &StatemachineSetting{}

func (s *StatemachineSetting) String() string {
	encV := []byte(s.Get())
	_, iface, err := s.transformer(encV, nil)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%v", iface)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*StatemachineSetting) Typ() string {
	return "m"
}

// Get retrieves the (encoded) value in the setting (or the encoded default).
func (s *StatemachineSetting) Get() string {
	encV := s.v.Load()
	if encV == nil {
		defV, _, err := s.transformer(nil, nil)
		if err != nil {
			panic(err)
		}
		return string(defV)
	}
	return string(encV.([]byte))
}

// Validate that the state machine accepts the user input. Returns new encoded
// state, unencoded state, or an error. If no update is given, round trips
// current state.
func (s *StatemachineSetting) Validate(old []byte, update *string) ([]byte, interface{}, error) {
	return s.transformer(old, update)
}

func (s *StatemachineSetting) set(finalEncodedV []byte) error {
	if _, _, err := s.transformer(finalEncodedV, nil); err != nil {
		return err
	}
	if bytes.Equal([]byte(s.Get()), finalEncodedV) {
		return nil
	}
	s.v.Store(finalEncodedV)
	s.changed()
	return nil
}

func (s *StatemachineSetting) setToDefault() {
	defV, _, err := s.transformer(nil, nil)
	if err != nil {
		panic(err)
	}
	if err := s.set(defV); err != nil {
		panic(err)
	}
}

// RegisterStatemachineSetting registers a StatemachineSetting. See the comment
// for StatemachineSetting for details.
func RegisterStatemachineSetting(key, desc string, transformer TransformerFn) *StatemachineSetting {
	setting := &StatemachineSetting{
		transformer: transformer,
	}
	register(key, desc, setting)
	return setting
}

// TestingSetStatemachine returns a mock, unregistered string setting for testing. See
// TestingSetBool for more details.
func TestingSetStatemachine(s **StatemachineSetting, transformer TransformerFn) func() {
	saved := *s
	tmp := &StatemachineSetting{transformer: transformer}
	*s = tmp
	return func() {
		*s = saved
	}
}

// OnChange registers a callback to be called when the setting changes.
func (s *StatemachineSetting) OnChange(fn func()) *StatemachineSetting {
	s.setOnChange(fn)
	return s
}
