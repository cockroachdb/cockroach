// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package settings

import (
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/pkg/errors"
)

// ByteSizeSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bytesize" is updated.
type ByteSizeSetting struct {
	IntSetting
}

var _ Setting = &ByteSizeSetting{}

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
