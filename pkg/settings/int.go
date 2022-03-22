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
	"context"

	"github.com/cockroachdb/errors"
)

// IntSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "int" is updated.
type IntSetting struct {
	common
	defaultValue int64
	validateFn   func(int64) error
}

var _ numericSetting = &IntSetting{}

// Get retrieves the int value in the setting.
func (i *IntSetting) Get(sv *Values) int64 {
	return sv.container.getInt64(i.slot)
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

// Default returns default value for setting.
func (i *IntSetting) Default() int64 {
	return i.defaultValue
}

// Defeat the linter.
var _ = (*IntSetting).Default

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
func (i *IntSetting) Override(ctx context.Context, sv *Values, v int64) {
	sv.setInt64(ctx, i.slot, v)
	sv.setDefaultOverride(i.slot, v)
}

func (i *IntSetting) set(ctx context.Context, sv *Values, v int64) error {
	if err := i.Validate(v); err != nil {
		return err
	}
	sv.setInt64(ctx, i.slot, v)
	return nil
}

func (i *IntSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(i.slot); val != nil {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = i.set(ctx, sv, val.(int64))
		return
	}
	if err := i.set(ctx, sv, i.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterIntSetting defines a new setting with type int with a
// validation function.
func RegisterIntSetting(
	class Class, key, desc string, defaultValue int64, validateFns ...func(int64) error,
) *IntSetting {
	var composed func(int64) error
	if len(validateFns) > 0 {
		composed = func(v int64) error {
			for _, validateFn := range validateFns {
				if err := validateFn(v); err != nil {
					return errors.Wrapf(err, "invalid value for %s", key)
				}
			}
			return nil
		}
	}
	if composed != nil {
		if err := composed(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &IntSetting{
		defaultValue: defaultValue,
		validateFn:   composed,
	}
	register(class, key, desc, setting)
	return setting
}

// WithPublic sets public visibility and can be chained.
func (i *IntSetting) WithPublic() *IntSetting {
	i.SetVisibility(Public)
	return i
}

// PositiveInt can be passed to RegisterIntSetting
func PositiveInt(v int64) error {
	if v < 1 {
		return errors.Errorf("cannot be set to a non-positive value: %d", v)
	}
	return nil
}

// NonNegativeInt can be passed to RegisterIntSetting.
func NonNegativeInt(v int64) error {
	if v < 0 {
		return errors.Errorf("cannot be set to a negative value: %d", v)
	}
	return nil
}
