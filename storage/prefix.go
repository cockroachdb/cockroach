// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"container/list"
	"sort"

	"github.com/cockroachdb/cockroach/util"
)

// prefixConfig maps from a string prefix to config objects.
// Config objects include accounting, permissions, and zones.
type prefixConfig struct {
	prefix Key         // the prefix the config affects
	config interface{} // the config object
}

// prefixConfigMap holds a slice of prefix configs, sorted by
// prefix. It also contains a map used to locate the canonical
// prefixConfig object for a config value.
type prefixConfigMap struct {
	configs          []*prefixConfig               // sorted slice of prefix configs
	canonicalConfigs map[interface{}]*prefixConfig // map from config value to prefixConfig
}

// rangeResult is returned by splitRangeByPrefixes.
type rangeResult struct {
	start, end Key
	config     interface{}
}

// PrefixEndKey determines the end key given a start key as a prefix. This
// adds "1" to the final byte and propagates the carry. The special
// case of KeyMin ("") always returns KeyMax ("\xff").
func PrefixEndKey(prefix Key) Key {
	if bytes.Compare(prefix, KeyMin) == 0 {
		return KeyMax
	}
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end
		}
	}
	// This statement will only be reached if the key is already a
	// maximal byte string (i.e. already \xff...).
	return prefix
}

// Implementation of sort.Interface.
func (p *prefixConfigMap) Len() int {
	return len(p.configs)
}
func (p *prefixConfigMap) Swap(i, j int) {
	p.configs[i], p.configs[j] = p.configs[j], p.configs[i]
}
func (p *prefixConfigMap) Less(i, j int) bool {
	return bytes.Compare(p.configs[i].prefix, p.configs[j].prefix) < 0
}

// newPrefixConfigMap creates a new prefix config map and sorts
// the entries by key prefix and then adds additional entries to mark
// the ends of each key prefix range. For example, if the map
// contains entries for:
//
//   "/":          config1
//   "/db1":       config2
//   "/db1/table": config3
//   "/db3":       config4
//
// ...then entries will be added for:
//
//   "/db1/tablf": config2
//   "/db2":       config1
//   "/db4":       config1
//
// These additional entries allow for simple lookups by prefix and
// provide a way to split a range by prefixes which affect it. This
// last is necessary for zone configs; ranges must not span zone
// config boundaries.
func newPrefixConfigMap(configs []*prefixConfig) (*prefixConfigMap, error) {
	p := &prefixConfigMap{
		configs:          configs,
		canonicalConfigs: map[interface{}]*prefixConfig{},
	}
	sort.Sort(p)
	for _, pc := range p.configs {
		p.canonicalConfigs[pc.config] = pc
	}

	if len(p.configs) == 0 || bytes.Compare(p.configs[0].prefix, KeyMin) != 0 {
		return nil, util.Errorf("no default prefix specified")
	}

	var newConfigs []*prefixConfig
	stack := list.New()

	for _, entry := range p.configs {
		// Pop entries from the stack which aren't prefixes.
		for stack.Len() > 0 && !bytes.HasPrefix(entry.prefix, stack.Back().Value.(*prefixConfig).prefix) {
			stack.Remove(stack.Back())
		}
		if stack.Len() != 0 {
			newConfigs = append(newConfigs, &prefixConfig{
				prefix: PrefixEndKey(entry.prefix),
				config: stack.Back().Value.(*prefixConfig).config,
			})
		}
		stack.PushBack(entry)
	}

	// Add newly created configs and re-sort.
	for _, config := range newConfigs {
		p.configs = append(p.configs, config)
	}
	sort.Sort(p)

	return p, nil
}

// matchByPrefix returns the longest matching prefixConfig. If the key
// specified does not match an existing prefix, a panic will
// result. Based on the comments in build(), that example will have a
// final list of prefixConfig entries which look like:
//
//   "/":          config1
//   "/db1":       config2
//   "/db1/table": config3
//   "/db1/tablf": config2
//   "/db2":       config1
//   "/db3":       config4
//   "/db4":       config1
//
// To find the longest matching prefix, we take the lower bound of the
// specified key.
func (p *prefixConfigMap) matchByPrefix(key Key) *prefixConfig {
	n := sort.Search(len(p.configs), func(i int) bool {
		return bytes.Compare(key, p.configs[i].prefix) < 0
	})
	if n == 0 || n > len(p.configs) {
		panic("should never match a key outside of default range")
	}
	// Lookup and return canonical prefixConfig.
	return p.canonicalConfigs[p.configs[n-1].config]
}

// matchesByPrefix returns a list of prefixConfig objects with
// prefixes satisfying the specified key. The results are returned in
// order of longest matching prefix to shortest.
func (p *prefixConfigMap) matchesByPrefix(key Key) []*prefixConfig {
	var configs []*prefixConfig
	prefix := key
	for {
		config := p.matchByPrefix(prefix)
		configs = append(configs, config)
		prefix = config.prefix
		if len(prefix) == 0 {
			return configs
		}
		// Truncate final character and loop.
		prefix = prefix[0 : len(prefix)-1]
	}
}

// splitRangeByPrefixes returns a list of key ranges with
// corresponding configs. The split is done using matching prefix
// config entries. For example, consider the following set of configs
// and prefixes:
//
//   /:    config1
//   /db1: config2
//
// A range containing keys from /0 - /db3 will map to
// the following split ranges and corresponding configs:
//
//   /0   - /db1: config1
//   /db1 - /db2: config2
//   /db2 - /db3: config1
//
// After calling prefixConfigMap.build(), our prefixes will look
// like:
//
//   /:    config1
//   /db1: config2
//   /db2: config1
//
// The algorithm is straightforward for splitting a range by existing
// prefixes. Lookup start key; that is first config. Lookup end key:
// that is last config. We then step through the intervening
// prefixConfig records and create a rangeResult for each.
func (p *prefixConfigMap) splitRangeByPrefixes(start, end Key) ([]*rangeResult, error) {
	if bytes.Compare(start, end) >= 0 {
		return nil, util.Errorf("start key %q not less than end key %q", start, end)
	}
	startIdx := sort.Search(len(p.configs), func(i int) bool {
		return bytes.Compare(start, p.configs[i].prefix) < 0
	})
	endIdx := sort.Search(len(p.configs), func(i int) bool {
		return bytes.Compare(end, p.configs[i].prefix) < 0
	})

	if startIdx >= len(p.configs) || endIdx > len(p.configs) {
		return nil, util.Errorf("start and/or end keys (%q, %q) fall outside prefix range; "+
			"was default prefix not added?", start, end)
	}

	// Create the first range result which goes from start -> end and
	// uses the config specified for the start key.
	var results []*rangeResult
	result := &rangeResult{start: start, end: end, config: p.configs[startIdx-1].config}
	results = append(results, result)

	// Now, cycle through from startIdx to endIdx, adding a new
	// rangeResult at each step.
	for i := startIdx; i < endIdx; i++ {
		result.end = p.configs[i].prefix
		if bytes.Compare(result.end, end) == 0 {
			break
		}
		result = &rangeResult{start: result.end, end: end, config: p.configs[i].config}
		results = append(results, result)
	}

	return results, nil
}
