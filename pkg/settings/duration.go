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
	"time"

	"github.com/pkg/errors"
)

// DurationSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "duration" is updated.
type DurationSetting struct {
	common
	defaultValue time.Duration
	validateFn   func(time.Duration) error
}

var _ Setting = &DurationSetting{}

// Get retrieves the duration value in the setting.
func (d *DurationSetting) Get(sv *Values) time.Duration {
	return time.Duration(sv.getInt64(d.slotIdx))
}

func (d *DurationSetting) String(sv *Values) string {
	return EncodeDuration(d.Get(sv))
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*DurationSetting) Typ() string {
	return "d"
}

// Validate that a value conforms with the validation function.
func (d *DurationSetting) Validate(v time.Duration) error {
	if d.validateFn != nil {
		if err := d.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

// Override changes the setting without validation.
// For testing usage only.
func (d *DurationSetting) Override(sv *Values, v time.Duration) {
	sv.setInt64(d.slotIdx, int64(v))
}

func (d *DurationSetting) set(sv *Values, v time.Duration) error {
	if err := d.Validate(v); err != nil {
		return err
	}
	sv.setInt64(d.slotIdx, int64(v))
	return nil
}

func (d *DurationSetting) setToDefault(sv *Values) {
	if err := d.set(sv, d.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterDurationSetting defines a new setting with type duration.
func RegisterDurationSetting(key, desc string, defaultValue time.Duration) *DurationSetting {
	return RegisterValidatedDurationSetting(key, desc, defaultValue, nil)
}

// RegisterNonNegativeDurationSetting defines a new setting with type duration.
func RegisterNonNegativeDurationSetting(
	key, desc string, defaultValue time.Duration,
) *DurationSetting {
	return RegisterValidatedDurationSetting(key, desc, defaultValue, func(v time.Duration) error {
		if v < 0 {
			return errors.Errorf("cannot set %s to a negative duration: %s", key, v)
		}
		return nil
	})
}

// RegisterValidatedDurationSetting defines a new setting with type duration.
func RegisterValidatedDurationSetting(
	key, desc string, defaultValue time.Duration, validateFn func(time.Duration) error,
) *DurationSetting {
	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &DurationSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}
