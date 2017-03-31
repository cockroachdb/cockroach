// Copyright 2016 The Cockroach Authors.
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
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// cache stores the settings which have values explicitly set, i.e. entires in
// `cache.values` override entries in `registry`, as implemented by `getVal`.
var cache struct {
	syncutil.RWMutex
	values map[string]value
}

// getVal gets the current value for key if it is set or the default value.
// `key` _must_ be a defined setting and _must_ be of the type requested (any
// invalid usage is a panic).
func getVal(key string, t ValueType) value {
	// We consult `registry` first as it serves as the canonical list of defined
	// settings and their types, and can thus verify usage is valid even before we
	// look at the applied settings in `cache`.
	d, ok := registry[key]
	if !ok {
		panic(errors.Errorf("invalid setting %q", key))
	}
	if d.typ != t {
		panic(errors.Errorf("setting %q is defined as %v, not %v)", key, d.typ, t))
	}

	cache.RLock()
	set, ok := cache.values[key]
	cache.RUnlock()

	if ok {
		return set
	}
	return d
}

func getString(key string) string {
	return getVal(key, StringValue).s
}

func getBool(key string) bool {
	return getVal(key, BoolValue).b
}

func getInt(key string) int {
	return getVal(key, IntValue).i
}

func getFloat(key string) float64 {
	return getVal(key, FloatValue).f
}

// Updater is the builder for replacing the global settings map.
// We swap the entire map at once (to minimize contention on the write-lock),
// rather than updating it in place.
//
// A caller passes the serialized representations of all individual settings (
// e.g. the rows read from the system.settings table) to Add(), one at a time,
// before then calling Apply() to swap the global cache.
type Updater struct {
	values map[string]value
}

// NewUpdater returns a new Updater, pre-alloced based on the current settings.
func NewUpdater() *Updater {
	cache.RLock()
	l := len(cache.values)
	cache.RUnlock()
	return &Updater{values: make(map[string]value, l)}
}

// Reset clears the Updater.
func (u *Updater) Reset() {
	if len(u.values) > 0 {
		u.values = make(map[string]value, len(u.values))
	}
}

// Add attempts to parse and add one setting.
// If a given setting is not passed to Add, it will not be in the resulting
// cache (i.e it would be removed and this revert to its default).
// If Add() fails to deserialize a value, it will fallback to preserving the
// previously set value, if there is one, such that Apply() can still be called
// without reverting that setting to default.
func (u *Updater) Add(key, rawValue, vt string) error {
	d, ok := registry[key]
	if !ok {
		// Likely a new setting this old node doesn't know about.
		return errors.Errorf("unknown setting %q", key)
	}
	cache.RLock()
	current, ok := cache.values[key]
	cache.RUnlock()

	// We're hopefully switch this below for the new value, but initializing it
	// here to the current value preserves that if we can't deserialize it.
	if ok {
		u.values[key] = current
	}

	typ, err := valueTypeFromStr(vt)
	if err != nil {
		return err
	}
	if typ != d.typ {
		return errors.Errorf("setting %q defined as type %c, not %c", key, d.typ, typ)
	}

	parsed, err := parseRaw(rawValue, typ)
	if err != nil {
		return err
	}
	u.values[key] = parsed
	return nil
}

// Apply swaps the global cache to our new map.
func (u *Updater) Apply() {
	// TODO(dt): lock and swap only if we actually changed anything.
	cache.Lock()
	cache.values = u.values
	cache.Unlock()
}
