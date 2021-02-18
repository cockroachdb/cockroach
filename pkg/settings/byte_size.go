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

var _ extendedSetting = &ByteSizeSetting{}

// Typ returns the short (1 char) string denoting the type of setting.
func (*ByteSizeSetting) Typ() string {
	return "z"
}

func (b *ByteSizeSetting) String(sv *Values) string {
	return humanizeutil.IBytes(b.Get(sv))
}

// WithPublic sets public visibility and can be chained.
func (b *ByteSizeSetting) WithPublic() *ByteSizeSetting {
	b.SetVisibility(Public)
	return b
}

// WithSystemOnly marks this setting as system-only and can be chained.
func (b *ByteSizeSetting) WithSystemOnly() *ByteSizeSetting {
	b.common.systemOnly = true
	return b
}

// RegisterByteSizeSetting defines a new setting with type bytesize and any
// supplied validation function(s).
func RegisterByteSizeSetting(
	key, desc string, defaultValue int64, validateFns ...func(int64) error,
) *ByteSizeSetting {

	var validateFn func(int64) error
	if len(validateFns) > 0 {
		validateFn = func(v int64) error {
			for _, fn := range validateFns {
				if err := fn(v); err != nil {
					return errors.Wrapf(err, "invalid value for %s", key)
				}
			}
			return nil
		}
	}

	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &ByteSizeSetting{IntSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}}
	register(key, desc, setting)
	return setting
}
