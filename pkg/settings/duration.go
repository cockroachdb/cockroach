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
	"sync/atomic"
	"time"
)

// DurationSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "duration" is updated.
type DurationSetting struct {
	defaultValue time.Duration
	v            int64
}

var _ Setting = &DurationSetting{}

// Get retrieves the duration value in the setting.
func (d *DurationSetting) Get() time.Duration {
	return time.Duration(atomic.LoadInt64(&d.v))
}

func (d *DurationSetting) String() string {
	return EncodeDuration(d.Get())
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*DurationSetting) Typ() string {
	return "d"
}

func (d *DurationSetting) set(v time.Duration) {
	atomic.StoreInt64(&d.v, int64(v))
}

func (d *DurationSetting) setToDefault() {
	d.set(d.defaultValue)
}

// RegisterDurationSetting defines a new setting with type duration.
func RegisterDurationSetting(key, desc string, defaultValue time.Duration) *DurationSetting {
	setting := &DurationSetting{defaultValue: defaultValue}
	register(key, desc, setting)
	return setting
}

// TestingSetDuration returns a mock, unregistered string setting for testing.
// See TestingSetBool for more details.
func TestingSetDuration(s **DurationSetting, v time.Duration) func() {
	saved := *s
	*s = &DurationSetting{v: int64(v)}
	return func() {
		*s = saved
	}
}
