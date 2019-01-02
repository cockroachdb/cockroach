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

import "github.com/pkg/errors"

// IntSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "int" is updated.
type IntSetting struct {
	common
	defaultValue int64
	validateFn   func(int64) error
}

var _ Setting = &IntSetting{}

// Get retrieves the int value in the setting.
func (i *IntSetting) Get(sv *Values) int64 {
	return sv.getInt64(i.slotIdx)
}

func (i *IntSetting) String(sv *Values) string {
	return EncodeInt(i.Get(sv))
}

// Encoded returns the encoded value of the current value of the setting.
func (i *IntSetting) Encoded(sv *Values) string {
	return i.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (i *IntSetting) EncodedDefault() string {
	return EncodeInt(i.defaultValue)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*IntSetting) Typ() string {
	return "i"
}

// Validate that a value conforms with the validation function.
func (i *IntSetting) Validate(v int64) error {
	if i.validateFn != nil {
		if err := i.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

// Override changes the setting without validation.
// For testing usage only.
func (i *IntSetting) Override(sv *Values, v int64) {
	sv.setInt64(i.slotIdx, v)
}

func (i *IntSetting) set(sv *Values, v int64) error {
	if err := i.Validate(v); err != nil {
		return err
	}
	i.Override(sv, v)
	return nil
}

func (i *IntSetting) setToDefault(sv *Values) {
	if err := i.set(sv, i.defaultValue); err != nil {
		panic(err)
	}
}

// Default returns the default value.
func (i *IntSetting) Default() int64 {
	return i.defaultValue
}

// RegisterIntSetting defines a new setting with type int.
func RegisterIntSetting(key, desc string, defaultValue int64) *IntSetting {
	return RegisterValidatedIntSetting(key, desc, defaultValue, nil)
}

// RegisterNonNegativeIntSetting defines a new setting with type int.
func RegisterNonNegativeIntSetting(key, desc string, defaultValue int64) *IntSetting {
	return RegisterValidatedIntSetting(key, desc, defaultValue, func(v int64) error {
		if v < 0 {
			return errors.Errorf("cannot set %s to a negative value: %d", key, v)
		}
		return nil
	})
}

// RegisterPositiveIntSetting defines a new setting with type int.
func RegisterPositiveIntSetting(key, desc string, defaultValue int64) *IntSetting {
	return RegisterValidatedIntSetting(key, desc, defaultValue, func(v int64) error {
		if v < 1 {
			return errors.Errorf("cannot set %s to a value < 1: %d", key, v)
		}
		return nil
	})
}

// RegisterValidatedIntSetting defines a new setting with type int with a
// validation function.
func RegisterValidatedIntSetting(
	key, desc string, defaultValue int64, validateFn func(int64) error,
) *IntSetting {
	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &IntSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}
