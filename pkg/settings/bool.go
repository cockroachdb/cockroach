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
	"strconv"
)

// BoolSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bool" is updated.
type BoolSetting struct {
	common
	defaultValue bool
}

var _ internalSetting = &BoolSetting{}

// Get retrieves the bool value in the setting.
func (b *BoolSetting) Get(sv *Values) bool {
	return sv.getInt64(b.slot) != 0
}

func (b *BoolSetting) String(sv *Values) string {
	return EncodeBool(b.Get(sv))
}

// Encoded returns the encoded value of the current value of the setting.
func (b *BoolSetting) Encoded(sv *Values) string {
	return b.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (b *BoolSetting) EncodedDefault() string {
	return EncodeBool(b.defaultValue)
}

// DecodeToString decodes and renders an encoded value.
func (b *BoolSetting) DecodeToString(encoded string) (string, error) {
	bv, err := b.DecodeValue(encoded)
	if err != nil {
		return "", err
	}
	return EncodeBool(bv), nil
}

// DecodeValue decodes the value into a float.
func (b *BoolSetting) DecodeValue(encoded string) (bool, error) {
	return strconv.ParseBool(encoded)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*BoolSetting) Typ() string {
	return "b"
}

// Default returns default value for setting.
func (b *BoolSetting) Default() bool {
	return b.defaultValue
}

// Defeat the linter.
var _ = (*BoolSetting).Default

// Override changes the setting without validation and also overrides the
// default value.
//
// For testing usage only.
func (b *BoolSetting) Override(ctx context.Context, sv *Values, v bool) {
	b.set(ctx, sv, v)
	sv.setDefaultOverride(b.slot, v)
}

func (b *BoolSetting) set(ctx context.Context, sv *Values, v bool) {
	vInt := int64(0)
	if v {
		vInt = 1
	}
	sv.setInt64(ctx, b.slot, vInt)
}

func (b *BoolSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(b.slot); val != nil {
		b.set(ctx, sv, val.(bool))
		return
	}
	b.set(ctx, sv, b.defaultValue)
}

// WithPublic sets public visibility and can be chained.
func (b *BoolSetting) WithPublic() *BoolSetting {
	b.SetVisibility(Public)
	return b
}

// RegisterBoolSetting defines a new setting with type bool.
func RegisterBoolSetting(class Class, key, desc string, defaultValue bool) *BoolSetting {
	setting := &BoolSetting{defaultValue: defaultValue}
	register(class, key, desc, setting)
	return setting
}
