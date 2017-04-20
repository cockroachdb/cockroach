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
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"
)

// The Register functions below return an object of type "Setting".
// This works differently for "large" (string) and "small" (everything
// else) types, as follows:
//
// For small types,
// - the Setting object contains a (pointer to an) int variable
// - the Get accessor uses atomic loads to access the value
// - the register function all registers an async callback to upload
//   the variable atomically when the settings change.
// This mechanism is chosen because an atomic load is inlined
// by the Go compiler and fast, whereas an access via the corresponding
// getXXX() methods would need to acquire a lock.
//
// For large types,
// - the Setting object contains a closure that calls the getXXX method.
//

// BoolSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bool" is updated.
type BoolSetting struct{ v *int32 }

// Get retrieves the bool value in the setting.
func (b BoolSetting) Get() bool {
	return atomic.LoadInt32(b.v) != 0
}

// IntSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "int" is updated.
type IntSetting struct{ v *int64 }

// Get retrieves the int value in the setting.
func (i IntSetting) Get() int {
	return int(atomic.LoadInt64(i.v))
}

// FloatSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "float" is updated.
type FloatSetting struct{ v *uint64 }

// Get retrieves the float value in the setting.
func (f FloatSetting) Get() float64 {
	x := atomic.LoadUint64(f.v)
	return *(*float64)(unsafe.Pointer(&x))
}

// DurationSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "duration" is updated.
type DurationSetting struct{ v *int64 }

// Get retrieves the duration value in the setting.
func (d DurationSetting) Get() time.Duration {
	return time.Duration(atomic.LoadInt64(d.v))
}

// StringSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "string" is updated.
type StringSetting struct{ v *atomic.Value }

// Get retrieves the string value in the setting.
func (s StringSetting) Get() string {
	return s.v.Load().(string)
}

// checkAdd validates that it is indeed possible to add a setting with
// that name to the registry.
func checkAdd(key string) {
	if atomic.LoadInt32(&frozen) > 0 {
		panic(fmt.Sprintf("registration must occur before server start: %s", key))
	}
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
}

// RegisterBoolSetting defines a new setting with type bool.
func RegisterBoolSetting(key, desc string, defVal bool) BoolSetting {
	checkAdd(key)

	v := int32(0)
	setting := BoolSetting{v: &v}
	f := func() {
		b := getBool(key)
		v := int32(0)
		if b {
			v = 1
		}
		atomic.StoreInt32(setting.v, v)
	}

	registry[key] = Value{Typ: BoolValue, Description: desc, B: defVal, asyncUpdate: f}
	f()

	return setting
}

// RegisterIntSetting defines a new setting with type int.
func RegisterIntSetting(key, desc string, defVal int) IntSetting {
	checkAdd(key)

	v := int64(0)
	setting := IntSetting{v: &v}
	f := func() { atomic.StoreInt64(setting.v, int64(getInt(key))) }

	registry[key] = Value{Typ: IntValue, Description: desc, I: defVal, asyncUpdate: f}
	f()

	return setting
}

// RegisterStringSetting defines a new setting with type string.
func RegisterStringSetting(key, desc string, defVal string) StringSetting {
	checkAdd(key)

	setting := StringSetting{v: &atomic.Value{}}
	f := func() { setting.v.Store(getString(key)) }

	registry[key] = Value{Typ: StringValue, Description: desc, S: defVal, asyncUpdate: f}
	f()

	return setting
}

// RegisterFloatSetting defines a new setting with type float.
func RegisterFloatSetting(key, desc string, defVal float64) FloatSetting {
	checkAdd(key)

	v := uint64(0)
	setting := FloatSetting{v: &v}
	f := func() {
		f := getFloat(key)
		atomic.StoreUint64(setting.v, *(*uint64)(unsafe.Pointer(&f)))
	}

	registry[key] = Value{Typ: FloatValue, Description: desc, F: defVal, asyncUpdate: f}
	f()

	return setting
}

// RegisterDurationSetting defines a new setting with type time.Duration.
func RegisterDurationSetting(key, desc string, defVal time.Duration) DurationSetting {
	checkAdd(key)

	v := int64(0)
	setting := DurationSetting{v: &v}

	f := func() { atomic.StoreInt64(setting.v, getDuration(key).Nanoseconds()) }
	registry[key] = Value{Typ: DurationValue, Description: desc, D: defVal, asyncUpdate: f}
	f()

	return setting
}
