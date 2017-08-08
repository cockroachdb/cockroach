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
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// EncodeDuration encodes a duration in the format parseRaw expects.
func EncodeDuration(d time.Duration) string {
	return d.String()
}

// EncodeBool encodes a bool in the format parseRaw expects.
func EncodeBool(b bool) string {
	return strconv.FormatBool(b)
}

// EncodeInt encodes an int in the format parseRaw expects.
func EncodeInt(i int64) string {
	return strconv.FormatInt(i, 10)
}

// EncodeFloat encodes a bool in the format parseRaw expects.
func EncodeFloat(f float64) string {
	return strconv.FormatFloat(f, 'E', -1, 64)
}

// ResettingUpdater is a helper for updating the in-memory settings.
//
// RefreshSettings passes the serialized representations of all individual
// settings -- e.g. the rows read from the system.settings table. We update the
// wrapped atomic settings values as we go and note which settings were updated,
// then set the rest to default in Done().
type ResettingUpdater struct {
	r Registry
	m map[string]struct{}
}

// An Updater is a helper interface used for updating the settings in a
// Registry.
type Updater interface {
	Set(k, rawValue, valType string) error
	Done()
}

// A NoopUpdater ignores all updates.
type NoopUpdater struct{}

// Set implements Updater. It is a no-op.
func (u NoopUpdater) Set(_, _, _ string) error { return nil }

// Done implements Updater. It is a no-op.
func (u NoopUpdater) Done() {}

// MakeResettingUpdater makes a ResettingUpdater.
func MakeResettingUpdater(r Registry) ResettingUpdater {
	return ResettingUpdater{
		m: make(map[string]struct{}, len(r)),
		r: r,
	}
}

// Set attempts to parse and update a setting and notes that it was updated.
func (u ResettingUpdater) Set(key, rawValue string, vt string) error {
	d, ok := u.r[key]
	if !ok {
		// Likely a new setting this old node doesn't know about.
		return errors.Errorf("unknown setting '%s'", key)
	}

	u.m[key] = struct{}{}

	if expected := d.Typ(); vt != expected {
		return errors.Errorf("setting '%s' defined as type %s, not %s", key, expected, vt)
	}

	switch setting := d.(type) {
	case *StringSetting:
		return setting.set(rawValue)
	case *BoolSetting:
		b, err := strconv.ParseBool(rawValue)
		if err != nil {
			return err
		}
		setting.set(b)
		return nil
	case numericSetting:
		i, err := strconv.Atoi(rawValue)
		if err != nil {
			return err
		}
		return setting.set(int64(i))
	case *FloatSetting:
		f, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			return err
		}
		return setting.set(f)
	case *DurationSetting:
		d, err := time.ParseDuration(rawValue)
		if err != nil {
			return err
		}
		return setting.set(d)
	case *EnumSetting:
		i, err := strconv.Atoi(rawValue)
		if err != nil {
			return err
		}
		return setting.set(int64(i))
	case *StateMachineSetting:
		return setting.set([]byte(rawValue))
	}
	return nil
}

// Done sets all settings not updated by the updater to their default values.
func (u ResettingUpdater) Done() {
	for k, v := range u.r {
		if _, ok := u.m[k]; !ok {
			v.setToDefault()
		}
	}
}
