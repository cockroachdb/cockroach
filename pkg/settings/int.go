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

import "github.com/cockroachdb/errors"

// IntSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "int" is updated.
type IntSetting struct {
	common
	defaultValue int64
	validateFn   func(int64) error
}

var _ extendedSetting = &IntSetting{}

// Get retrieves the int value in the setting.
func (i *IntSetting) Get(sv *Values) int64 {
	return sv.container.getInt64(i.slotIdx)
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

// Override changes the setting without validation and also overrides the
// default value.
//
// For testing usage only.
func (i *IntSetting) Override(sv *Values, v int64) {
	sv.setInt64(i.slotIdx, v)
	sv.setDefaultOverrideInt64(i.slotIdx, v)
}

func (i *IntSetting) set(sv *Values, v int64) error {
	if err := i.Validate(v); err != nil {
		return err
	}
	sv.setInt64(i.slotIdx, v)
	return nil
}

func (i *IntSetting) setToDefault(sv *Values) {
	// See if the default value was overridden.
	ok, val, _ := sv.getDefaultOverride(i.slotIdx)
	if ok {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = i.set(sv, val)
		return
	}
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

// RegisterPublicIntSetting defines a new setting with type int and makes it public.
func RegisterPublicIntSetting(key, desc string, defaultValue int64) *IntSetting {
	s := RegisterValidatedIntSetting(key, desc, defaultValue, nil)
	s.SetVisibility(Public)
	return s
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
