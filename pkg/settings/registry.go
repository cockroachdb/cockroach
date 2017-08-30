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
// The registry does not store the current values of the settings; those are
// stored separately in Values, allowing multiple independent instances
// of each setting in the registry.
//
// Registry should never be mutated after creation (except in tests), as it is
// read concurrently by different callers.
var Registry = make(map[string]Setting)

// Register adds a setting to the registry.
func register(key, desc string, s Setting) {
	if _, ok := Registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	s.setDescription(desc)
	Registry[key] = s
	s.setSlotIdx(len(Registry))
}

// Keys returns a sorted string array with all the known keys.
func Keys() (res []string) {
	res = make([]string, 0, len(Registry))
	for k := range Registry {
		if Registry[k].Hidden() {
			continue
		}
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

// Lookup returns a Setting by name along with its description.
func Lookup(name string) (Setting, bool) {
	v, ok := Registry[name]
	return v, ok
}
