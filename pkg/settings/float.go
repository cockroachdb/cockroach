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
	"math"

	"github.com/pkg/errors"
)

// FloatSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "float" is updated.
type FloatSetting struct {
	common
	defaultValue float64
	validateFn   func(float64) error
}

var _ Setting = &FloatSetting{}

// Get retrieves the float value in the setting.
func (f *FloatSetting) Get(sv *Values) float64 {
	return math.Float64frombits(uint64(sv.getInt64(f.slotIdx)))
}

func (f *FloatSetting) String(sv *Values) string {
	return EncodeFloat(f.Get(sv))
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*FloatSetting) Typ() string {
	return "f"
}

// Validate that a value conforms with the validation function.
func (f *FloatSetting) Validate(v float64) error {
	if f.validateFn != nil {
		if err := f.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

func (f *FloatSetting) set(sv *Values, v float64) error {
	if err := f.Validate(v); err != nil {
		return err
	}
	sv.setInt64(f.slotIdx, int64(math.Float64bits(v)))
	return nil
}

func (f *FloatSetting) setToDefault(sv *Values) {
	if err := f.set(sv, f.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterFloatSetting defines a new setting with type float.
func RegisterFloatSetting(key, desc string, defaultValue float64) *FloatSetting {
	return RegisterValidatedFloatSetting(key, desc, defaultValue, nil)
}

// RegisterValidatedFloatSetting defines a new setting with type float.
func RegisterValidatedFloatSetting(
	key, desc string, defaultValue float64, validateFn func(float64) error,
) *FloatSetting {
	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &FloatSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}

// RegisterNonNegativeFloatSetting defines a new setting with type float.
func RegisterNonNegativeFloatSetting(key, desc string, defaultValue float64) *FloatSetting {
	return RegisterValidatedFloatSetting(key, desc, defaultValue, func(v float64) error {
		if v < 0 {
			return errors.Errorf("cannot set %s to a negative value: %f", key, v)
		}
		return nil
	})
}
