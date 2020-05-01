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

// RegisterByteSizeSetting defines a new setting with type bytesize.
func RegisterByteSizeSetting(key, desc string, defaultValue int64) *ByteSizeSetting {
	return RegisterValidatedByteSizeSetting(key, desc, defaultValue, nil)
}

// RegisterPublicByteSizeSetting defines a new setting with type bytesize and makes it public.
func RegisterPublicByteSizeSetting(key, desc string, defaultValue int64) *ByteSizeSetting {
	s := RegisterValidatedByteSizeSetting(key, desc, defaultValue, nil)
	s.SetVisibility(Public)
	return s
}

// RegisterValidatedByteSizeSetting defines a new setting with type bytesize
// with a validation function.
func RegisterValidatedByteSizeSetting(
	key, desc string, defaultValue int64, validateFn func(int64) error,
) *ByteSizeSetting {
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

// RegisterPublicValidatedByteSizeSetting defines a new setting with type
// bytesize with a validation function and makes it public.
func RegisterPublicValidatedByteSizeSetting(
	key, desc string, defaultValue int64, validateFn func(int64) error,
) *ByteSizeSetting {
	s := RegisterValidatedByteSizeSetting(key, desc, defaultValue, validateFn)
	s.SetVisibility(Public)
	return s
}
