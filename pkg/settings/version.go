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

// VersionSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "string" is updated.
type VersionSetting struct {
	defaultValue string
	validateFn   func(*Values, string) error
	common
}

var _ Setting = &VersionSetting{}

func (s *VersionSetting) String(sv *Values) string {
	return s.Get(sv)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*VersionSetting) Typ() string {
	return "s"
}

// Get retrieves the string value in the setting.
func (s *VersionSetting) Get(sv *Values) string {
	loaded := sv.getGeneric(s.slotIdx)
	if loaded == nil {
		return ""
	}
	return loaded.(string)
}

// Validate that a value conforms with the validation function.
func (s *VersionSetting) Validate(sv *Values, v string) error {
	if s.validateFn != nil {
		if err := s.validateFn(sv, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *VersionSetting) set(sv *Values, v string) error {
	if err := s.Validate(sv, v); err != nil {
		return err
	}
	if s.Get(sv) != v {
		sv.setGeneric(s.slotIdx, v)
	}
	return nil
}

func (s *VersionSetting) setToDefault(sv *Values) {
	if err := s.set(sv, s.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterValidatedVersionSetting defines a new setting with type string with a
// validation function.
func RegisterValidatedVersionSetting(
	key, desc string, defaultValue string, validateFn func(*Values, string) error,
) *VersionSetting {
	if validateFn != nil {
		if err := validateFn(nil, defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &VersionSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}
