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
)

// updated automatically when the corresponding cluster-wide setting
// of type "int" is updated.
type IntSetting struct {
	defaultValue int64
	v            int64
}

var _ Setting = &IntSetting{}

// Get retrieves the int value in the setting.
func (i *IntSetting) Get() int64 {
	return atomic.LoadInt64(&i.v)
}

func (i *IntSetting) String() string {
	return EncodeInt(i.Get())
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*IntSetting) Typ() string {
	return "i"
}

func (i *IntSetting) set(v int64) {
	atomic.StoreInt64(&i.v, v)
}

func (i *IntSetting) setToDefault() {
	i.set(i.defaultValue)
}

// RegisterIntSetting defines a new setting with type int.
func RegisterIntSetting(key, desc string, defaultValue int64) *IntSetting {
	setting := &IntSetting{defaultValue: defaultValue}
	register(key, desc, setting)
	return setting
}

// TestingSetInt returns a mock, unregistered int setting for testing. See
// TestingSetBool for more details.
func TestingSetInt(s **IntSetting, v int64) func() {
	saved := *s
	*s = &IntSetting{v: v}
	return func() {
		*s = saved
	}
}
