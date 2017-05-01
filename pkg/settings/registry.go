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
	"sort"
	"sync/atomic"
)

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
// cached internally after parsing in these appropriately typed fields (which is
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
