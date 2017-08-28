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
	"github.com/pkg/errors"
)

// StringSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "string" is updated.
type StringSetting struct {
	defaultValue string
	validateFn   func(string) error
	common
}

var _ Setting = &StringSetting{}

func (s *StringSetting) String(sv *Values) string {
	return s.Get(sv)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*StringSetting) Typ() string {
	return "s"
}

// Get retrieves the string value in the setting.
func (s *StringSetting) Get(sv *Values) string {
	loaded := sv.getGeneric(s.slotIdx)
	if loaded == nil {
		return ""
	}
	return loaded.(string)
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

func (s *StringSetting) set(sv *Values, v string) error {
	if err := s.Validate(v); err != nil {
		return err
	}
	if s.Get(sv) != v {
		sv.setGeneric(s.slotIdx, v)
	}
	return nil
}

func (s *StringSetting) setToDefault(sv *Values) {
	if err := s.set(sv, s.defaultValue); err != nil {
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
