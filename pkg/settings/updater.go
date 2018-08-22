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
	return strconv.FormatFloat(f, 'G', -1, 64)
}

type updater struct {
	sv *Values
	m  map[string]struct{}
}

// Updater is a helper for updating the in-memory settings.
//
// RefreshSettings passes the serialized representations of all individual
// settings -- e.g. the rows read from the system.settings table. We update the
// wrapped atomic settings values as we go and note which settings were updated,
// then set the rest to default in ResetRemaining().
type Updater interface {
	Set(k, rawValue, valType string) error
	Reset(k string) error
	ResetRemaining()
}

// A NoopUpdater ignores all updates.
type NoopUpdater struct{}

// Reset implements Updater. It is a no-op.
func (u NoopUpdater) Reset(string) error { return nil }

// Set implements Updater. It is a no-op.
func (u NoopUpdater) Set(_, _, _ string) error { return nil }

// ResetRemaining implements Updater. It is a no-op.
func (u NoopUpdater) ResetRemaining() {}

// NewUpdater makes an Updater.
func NewUpdater(sv *Values) Updater {
	return updater{
		m:  make(map[string]struct{}, len(Registry)),
		sv: sv,
	}
}

func (u updater) get(key string) (Setting, error) {
	d, ok := Registry[key]
	if !ok {
		if _, ok := retiredSettings[key]; ok {
			return nil, nil
		}
		// Likely a new setting this old node doesn't know about.
		return nil, errors.Errorf("unknown setting '%s'", key)
	}
	return d, nil
}

// Set attempts to parse and update a setting and notes that it was updated.
func (u updater) Set(key, rawValue string, vt string) error {
	d, err := u.get(key)
	if err != nil || d == nil {
		return err
	}

	u.m[key] = struct{}{}

	if expected := d.Typ(); vt != expected {
		return errors.Errorf("setting '%s' defined as type %s, not %s", key, expected, vt)
	}

	switch setting := d.(type) {
	case *StringSetting:
		return setting.set(u.sv, rawValue)
	case *BoolSetting:
		b, err := strconv.ParseBool(rawValue)
		if err != nil {
			return err
		}
		setting.set(u.sv, b)
		return nil
	case numericSetting:
		i, err := strconv.Atoi(rawValue)
		if err != nil {
			return err
		}
		return setting.set(u.sv, int64(i))
	case *FloatSetting:
		f, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			return err
		}
		return setting.set(u.sv, f)
	case *DurationSetting:
		d, err := time.ParseDuration(rawValue)
		if err != nil {
			return err
		}
		return setting.set(u.sv, d)
	case *EnumSetting:
		i, err := strconv.Atoi(rawValue)
		if err != nil {
			return err
		}
		return setting.set(u.sv, int64(i))
	case *StateMachineSetting:
		return setting.set(u.sv, []byte(rawValue))
	}
	return nil
}

func (u updater) Reset(key string) error {
	d, err := u.get(key)
	if err != nil || d == nil {
		return err
	}
	d.setToDefault(u.sv)
	return nil
}

// ResetRemaining sets all settings not updated by the updater to their default values.
func (u updater) ResetRemaining() {
	for k, v := range Registry {
		if _, ok := u.m[k]; !ok {
			v.setToDefault(u.sv)
		}
	}
}
