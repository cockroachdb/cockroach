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
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// cache stores the settings which have values explicitly set, i.e. entries in
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
		panic(errors.Errorf("invalid setting '%s'", key))
	}
	if d.typ != t {
		panic(errors.Errorf("setting '%s' is defined as %c, not %c)", key, d.typ, t))
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

// Updater is a helper for replacing the global settings map. It is intended to
// be used in, and only in, the RefreshSettings loop.
//
// We swap the entire map at once (to minimize contention on the write-lock),
// rather than updating it in place.
//
// RefreshSettings passes the serialized representations of all individual
// settings -- e.g. the rows read from the system.settings table -- to Add(),
// one at a time, before then calling Apply() to swap the global cache.
type Updater map[string]value

// MakeUpdater returns a new Updater, pre-alloced based on the current settings.
func MakeUpdater() Updater {
	cache.RLock()
	l := len(cache.values)
	cache.RUnlock()
	return make(Updater, l)
}

// Reset clears the Updater.
func (u Updater) Reset() {
	for k := range u {
		delete(u, k)
	}
}

// Add attempts to parse and add one setting.
//
// If a given setting is not passed to Add, it will not be in the resulting
// cache (i.e it would be removed and thus revert to its default).
// If Add() fails to deserialize a value, it will fallback to preserving the
// previously set value, if there is one, such that Apply() can still be called
// without reverting that setting to default.
func (u Updater) Add(key, rawValue, vt string) error {
	d, ok := registry[key]
	if !ok {
		// Likely a new setting this old node doesn't know about.
		return errors.Errorf("unknown setting '%s'", key)
	}
	cache.RLock()
	current, ok := cache.values[key]
	cache.RUnlock()

	// We'll hopefully switch this below for the new value, but we initialize it
	// here to the current value, to preserve it in case we can't a read new one.
	if ok {
		u[key] = current
	}

	typ, err := valueTypeFromStr(vt)
	if err != nil {
		return err
	}
	if typ != d.typ {
		return errors.Errorf("setting '%s' defined as type %c, not %c", key, d.typ, typ)
	}

	parsed, err := parseRaw(rawValue, typ)
	if err != nil {
		return err
	}
	u[key] = parsed
	return nil
}

// Apply swaps the global cache to our new map, and Closes() the Updater.
func (u Updater) Apply() {
	cache.Lock()
	cache.values = u
	cache.Unlock()

	afterApply.Lock()
	for _, f := range afterApply.hooks {
		f()
	}
	afterApply.Unlock()
}

var afterApply struct {
	syncutil.Mutex
	hooks []func()
}

// RegisterCallback registers a func() f that will be called immediately and
// then after new settings are read (even if there are no changes).
//
// `f` would likely read a setting to update an atomic int or channel, or
// otherwise trigger additional work. `f` should not block, or otherwise do any
// lengthy work itself, as it blocks the settings updater.
//
// RegisterCallback can (and likely should) be called at `init` -- indeed, the
// eager call during registration is intended to ensure it initializes defaults
// even before the first gossip update is received.
func RegisterCallback(f func()) {
	f()
	afterApply.Lock()
	afterApply.hooks = append(afterApply.hooks, f)
	afterApply.Unlock()
}
