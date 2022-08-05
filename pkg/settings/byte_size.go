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
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
)

// ByteSizeSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bytesize" is updated.
type ByteSizeSetting struct {
	IntSetting
}

var _ numericSetting = &ByteSizeSetting{}

// Typ returns the short (1 char) string denoting the type of setting.
func (*ByteSizeSetting) Typ() string {
	return "z"
}

func (b *ByteSizeSetting) String(sv *Values) string {
	return string(humanizeutil.IBytes(b.Get(sv)))
}

// DecodeToString decodes and renders an encoded value.
func (b *ByteSizeSetting) DecodeToString(encoded string) (string, error) {
	iv, err := b.DecodeValue(encoded)
	if err != nil {
		return "", err
	}
	return string(humanizeutil.IBytes(iv)), nil
}

// WithPublic sets public visibility and can be chained.
func (b *ByteSizeSetting) WithPublic() *ByteSizeSetting {
	b.SetVisibility(Public)
	return b
}

// RegisterByteSizeSetting defines a new setting with type bytesize and any
// supplied validation function(s). If no validation functions are given, then
// the non-negative int validation is performed.
func RegisterByteSizeSetting(
	class Class, key, desc string, defaultValue int64, validateFns ...func(int64) error,
) *ByteSizeSetting {

	var validateFn = func(v int64) error {
		if len(validateFns) > 0 {
			for _, fn := range validateFns {
				if err := fn(v); err != nil {
					return errors.Wrapf(err, "invalid value for %s", key)
				}
			}
			return nil
		}
		return NonNegativeInt(v)
	}

	if err := validateFn(defaultValue); err != nil {
		panic(errors.Wrap(err, "invalid default"))
	}
	setting := &ByteSizeSetting{IntSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}}
	register(class, key, desc, setting)
	return setting
}
