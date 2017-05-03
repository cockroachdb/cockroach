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
	"sync/atomic"

	"github.com/pkg/errors"
)

// StringSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "string" is updated.
type StringSetting struct {
	defaultValue string
	v            atomic.Value
	validateFn   func(string) error
}

var _ Setting = &StringSetting{}

func (s *StringSetting) String() string {
	return s.Get()
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*StringSetting) Typ() string {
	return "s"
}

// Get retrieves the string value in the setting.
func (s *StringSetting) Get() string {
	return s.v.Load().(string)
}

// Validate that a value conforms with the validation function.
func (s *StringSetting) Validate(v string) error {
	if s.validateFn != nil {
		if err := s.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

func (s *StringSetting) set(v string) error {
	if err := s.Validate(v); err != nil {
		return err
	}
	s.v.Store(v)
	return nil
}

func (s *StringSetting) setToDefault() {
	if err := s.set(s.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterStringSetting defines a new setting with type string.
func RegisterStringSetting(key, desc string, defaultValue string) *StringSetting {
	return RegisterValidatedStringSetting(key, desc, defaultValue, nil)
}

// RegisterValidatedStringSetting defines a new setting with type string with a
// validation function.
func RegisterValidatedStringSetting(
	key, desc string, defaultValue string, validateFn func(string) error,
) *StringSetting {
	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &StringSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}

// TestingSetString returns a mock, unregistered string setting for testing. See
// TestingSetBool for more details.
func TestingSetString(s **StringSetting, v string) func() {
	saved := *s
	tmp := &StringSetting{}
	if err := tmp.set(v); err != nil {
		panic(err)
	}
	*s = tmp
	return func() {
		*s = saved
	}
}
