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
)

// registry contains all defined settings, their types and default values.
//
// Entries in registry should be accompanied by an exported, typesafe getter
// that then wraps one of the private `getBool`, `getString`, etc helpers.
//
// Registry should never be mutated after init (except in tests), as it is read
// concurrently by different callers.
var registry = map[string]Value{}

// frozen becomes non-zero once the registry is "live".
// This must be accessed atomically because test clusters spawn multiple
// servers within the same process which all call Freeze() possibly
// concurrently.
var frozen int32

// Freeze ensures that no new settings can be defined after the gossip worker
// has started. See settingsworker.go.
func Freeze() { atomic.StoreInt32(&frozen, 1) }

// Value holds the (parsed, typed) value of a setting.
// raw settings are stored in system.settings as human-readable strings, but are
// cached interally after parsing in these appropriately typed fields (which is
// basically a poor-man's union, without boxing).
type Value struct {
	Typ         ValueType
	Description string
	// Exactly one of these will be set, determined by typ.
	S string
	B bool
	I int
	F float64
	D time.Duration
}

// TypeOf returns the type of a setting, if it is defined.
func TypeOf(key string) (ValueType, bool) {
	d, ok := registry[key]
	return d.Typ, ok
}

// RegisterBoolSetting defines a new setting with type bool.
func RegisterBoolSetting(key, desc string, defVal bool) func() bool {
	if atomic.LoadInt32(&frozen) > 0 {
		panic(fmt.Sprintf("registration must occur before server start: %s", key))
	}
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	registry[key] = Value{Typ: BoolValue, Description: desc, B: defVal}
	return func() bool { return getBool(key) }
}

// RegisterIntSetting defines a new setting with type int.
func RegisterIntSetting(key, desc string, defVal int) func() int {
	if atomic.LoadInt32(&frozen) > 0 {
		panic(fmt.Sprintf("registration must occur before server start: %s", key))
	}
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	registry[key] = Value{Typ: IntValue, Description: desc, I: defVal}
	return func() int { return getInt(key) }
}

// RegisterStringSetting defines a new setting with type string.
func RegisterStringSetting(key, desc string, defVal string) func() string {
	if atomic.LoadInt32(&frozen) > 0 {
		panic(fmt.Sprintf("registration must occur before server start: %s", key))
	}
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	registry[key] = Value{Typ: StringValue, Description: desc, S: defVal}
	return func() string { return getString(key) }
}

// RegisterFloatSetting defines a new setting with type float.
func RegisterFloatSetting(key, desc string, defVal float64) func() float64 {
	if atomic.LoadInt32(&frozen) > 0 {
		panic(fmt.Sprintf("registration must occur before server start: %s", key))
	}
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	registry[key] = Value{Typ: FloatValue, Description: desc, F: defVal}
	return func() float64 { return getFloat(key) }
}

// RegisterDurationSetting defines a new setting with type time.Duration.
func RegisterDurationSetting(key, desc string, defVal time.Duration) func() time.Duration {
	if atomic.LoadInt32(&frozen) > 0 {
		panic(fmt.Sprintf("registration must occur before server start: %s", key))
	}
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	registry[key] = Value{Typ: DurationValue, Description: desc, D: defVal}
	return func() time.Duration { return getDuration(key) }
}
