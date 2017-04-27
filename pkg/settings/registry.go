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
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

// Setting implementions wrap a val with atomic access.
type Setting interface {
	setToDefault()
	// Typ returns the short (1 char) string denoting the type of setting.
	Typ() string
	String() string
}

// BoolSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bool" is updated.
type BoolSetting struct {
	defaultValue bool
	v            int32
}

// Get retrieves the bool value in the setting.
func (b *BoolSetting) Get() bool {
	return atomic.LoadInt32(&b.v) != 0
}

func (b *BoolSetting) String() string {
	return EncodeBool(b.Get())
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*BoolSetting) Typ() string {
	return "b"
}

func (b *BoolSetting) set(v bool) {
	if v {
		atomic.StoreInt32(&b.v, 1)
	} else {
		atomic.StoreInt32(&b.v, 0)
	}
}

func (b *BoolSetting) setToDefault() {
	b.set(b.defaultValue)
}

// RegisterBoolSetting defines a new setting with type bool.
func RegisterBoolSetting(key, desc string, defVal bool) *BoolSetting {
	setting := &BoolSetting{defaultValue: defVal}
	register(key, desc, setting)
	return setting
}

// TestingSetBool returns a mock, unregistered bool setting for testing.
func TestingSetBool(s **BoolSetting, v bool) func() {
	saved := *s
	if v {
		*s = &BoolSetting{v: 1}
	} else {
		*s = &BoolSetting{v: 0}
	}
	return func() {
		*s = saved
	}
}

type numericSetting interface {
	Setting
	set(i int64)
}

// IntSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "int" is updated.
type IntSetting struct {
	defaultValue int64
	v            int64
}

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
func RegisterIntSetting(key, desc string, defVal int64) *IntSetting {
	setting := &IntSetting{defaultValue: defVal}
	register(key, desc, setting)
	return setting
}

// TestingSetInt returns a mock, unregistered int setting for testing.
func TestingSetInt(s **IntSetting, v int64) func() {
	saved := *s
	*s = &IntSetting{v: v}
	return func() {
		*s = saved
	}
}

// FloatSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "float" is updated.
type FloatSetting struct {
	defaultValue float64
	v            uint64
}

// Get retrieves the float value in the setting.
func (f *FloatSetting) Get() float64 {
	return math.Float64frombits(atomic.LoadUint64(&f.v))
}

func (f *FloatSetting) String() string {
	return EncodeFloat(f.Get())
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*FloatSetting) Typ() string {
	return "f"
}

func (f *FloatSetting) set(v float64) {
	atomic.StoreUint64(&f.v, math.Float64bits(v))
}

func (f *FloatSetting) setToDefault() {
	f.set(f.defaultValue)
}

// TestingSetFloat returns a mock, unregistered float setting for testing.
func TestingSetFloat(s **FloatSetting, v float64) func() {
	saved := *s
	tmp := &FloatSetting{}
	tmp.set(v)
	*s = tmp
	return func() {
		*s = saved
	}
}

// RegisterFloatSetting defines a new setting with type float.
func RegisterFloatSetting(key, desc string, defVal float64) *FloatSetting {
	setting := &FloatSetting{defaultValue: defVal}
	register(key, desc, setting)
	return setting
}

// DurationSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "duration" is updated.
type DurationSetting struct {
	defaultValue time.Duration
	v            int64
}

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
func RegisterDurationSetting(key, desc string, defVal time.Duration) *DurationSetting {
	setting := &DurationSetting{defaultValue: defVal}
	register(key, desc, setting)
	return setting
}

// TestingSetDuration returns a mock, unregistered string setting for testing.
func TestingSetDuration(s **DurationSetting, v time.Duration) func() {
	saved := *s
	*s = &DurationSetting{v: int64(v)}
	return func() {
		*s = saved
	}
}

// StringSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "string" is updated.
type StringSetting struct {
	defaultValue string
	v            atomic.Value
}

func (s *StringSetting) String() string {
	return s.Get()
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*StringSetting) Typ() string {
	return "s"
}

// Get retrieves the string value in the setting.
func (s *StringSetting) Get() string {
	return s.v.Load().(string)
}

func (s *StringSetting) set(v string) {
	s.v.Store(v)
}

func (s *StringSetting) setToDefault() {
	s.set(s.defaultValue)
}

// RegisterStringSetting defines a new setting with type string.
func RegisterStringSetting(key, desc string, defVal string) *StringSetting {
	setting := &StringSetting{defaultValue: defVal}
	register(key, desc, setting)
	return setting
}

// TestingSetString returns a mock, unregistered string setting for testing.
func TestingSetString(s **StringSetting, v string) func() {
	saved := *s
	tmp := &StringSetting{}
	tmp.set(v)
	*s = tmp
	return func() {
		*s = saved
	}
}

// ByteSizeSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bytesize" is updated.
type ByteSizeSetting struct {
	IntSetting
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*ByteSizeSetting) Typ() string {
	return "z"
}

func (b *ByteSizeSetting) String() string {
	return humanizeutil.IBytes(b.Get())
}

// RegisterByteSizeSetting defines a new setting with type bytesize.
func RegisterByteSizeSetting(key, desc string, defVal int64) *ByteSizeSetting {
	setting := &ByteSizeSetting{IntSetting{defaultValue: defVal}}
	register(key, desc, setting)
	return setting
}

// TestingSetByteSize returns a mock bytesize setting for testing.
func TestingSetByteSize(s **ByteSizeSetting, v int64) func() {
	saved := *s
	*s = &ByteSizeSetting{IntSetting{v: v}}
	return func() {
		*s = saved
	}
}

// registry contains all defined settings, their types and default values.
//
// Entries in registry should be accompanied by an exported, typesafe getter
// that then wraps one of the private `getBool`, `getString`, etc helpers.
//
// Registry should never be mutated after init (except in tests), as it is read
// concurrently by different callers.
var registry = map[string]wrappedSetting{}

// frozen becomes non-zero once the registry is "live".
// This must be accessed atomically because test clusters spawn multiple
// servers within the same process which all call Freeze() possibly
// concurrently.
var frozen int32

// Freeze ensures that no new settings can be defined after the gossip worker
// has started. See settingsworker.go.
func Freeze() { atomic.StoreInt32(&frozen, 1) }

func assertNotFrozen(key string) {
	if atomic.LoadInt32(&frozen) > 0 {
		panic(fmt.Sprintf("registration must occur before server start: %s", key))
	}
}

// register adds a setting to the registry.
func register(key, desc string, s Setting) {
	assertNotFrozen(key)
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	s.setToDefault()
	registry[key] = wrappedSetting{description: desc, setting: s}
}

// Hide prevents a setting from showing up in SHOW ALL CLUSTER SETTINGS. It can
// still be used with SET and SHOW if the exact setting name is known. Use Hide
// for in-development features and other settings that should not be
// user-visible.
func Hide(key string) {
	assertNotFrozen(key)
	s, ok := registry[key]
	if !ok {
		panic(fmt.Sprintf("setting not found: %s", key))
	}
	s.hidden = true
	registry[key] = s
}

// Value holds the (parsed, typed) value of a setting.
// raw settings are stored in system.settings as human-readable strings, but are
// cached interally after parsing in these appropriately typed fields (which is
// basically a poor-man's union, without boxing).
type wrappedSetting struct {
	description string
	hidden      bool
	setting     Setting
}

// Keys returns a sorted string array with all the known keys.
func Keys() (res []string) {
	res = make([]string, 0, len(registry))
	for k := range registry {
		if registry[k].hidden {
			continue
		}
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

// Lookup returns a Setting by name along with its description.
func Lookup(name string) (Setting, string, bool) {
	v, ok := registry[name]
	if !ok {
		return nil, "", false
	}
	return v.setting, v.description, true
}
