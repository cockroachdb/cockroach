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
)

// Registry contains all defined settings, their types and default values.
//
// Entries in registry should be accompanied by an exported, typesafe getter
// that then wraps one of the private `getBool`, `getString`, etc helpers.
//
// Registry should never be mutated after creation (except in tests), as it is
// read concurrently by different callers.
type Registry map[string]Setting

// NewRegistry makes a new Registry.
func NewRegistry() Registry {
	return make(map[string]Setting)
}

// Register adds a setting to the registry.
func (r Registry) register(key, desc string, s Setting) {
	if _, ok := r[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	s.setToDefault()
	s.setDescription(desc)
	r[key] = s
}

// Keys returns a sorted string array with all the known keys.
func (r Registry) Keys() (res []string) {
	res = make([]string, 0, len(r))
	for k := range r {
		if r[k].Hidden() {
			continue
		}
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

// Lookup returns a Setting by name along with its description.
func (r Registry) Lookup(name string) (Setting, bool) {
	v, ok := r[name]
	if !ok {
		return nil, false
	}
	return v, true
}
