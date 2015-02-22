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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"container/list"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

// PrefixConfig relate a string prefix to a config object. Config
// objects include accounting, permissions, and zones. PrefixConfig
// objects are the constituents of PrefixConfigMap objects. In order
// to support binary searches of hierarchical prefixes (see the
// comments in NewPrefixConfigMap), PrefixConfig objects are
// additionally added to a PrefixConfigMap to demarcate the end of a
// prefix range. Such end-of-range sentinels need to refer back to the
// next "higher-up" prefix in the hierarchy (many times this is the
// default prefix which covers the entire range of keys). The Canonical
// key refers to this "higher-up" PrefixConfig by specifying its prefix
// so it can be binary searched from within a PrefixConfigMap.
type PrefixConfig struct {
	Prefix    proto.Key   // the prefix the config affects
	Canonical proto.Key   // the prefix for the canonical config, if applicable
	Config    interface{} // the config object
}

// PrefixConfigMap is a slice of prefix configs, sorted by
// prefix. Along with various accessor methods, the config map
// also contains additional prefix configs in the slice to
// account for the ends of prefix ranges.
type PrefixConfigMap []*PrefixConfig

// RangeResult is returned by SplitRangeByPrefixes.
type RangeResult struct {
	start, end proto.Key
	config     interface{}
}

// Implementation of sort.Interface.
func (p PrefixConfigMap) Len() int {
	return len(p)
}
func (p PrefixConfigMap) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p PrefixConfigMap) Less(i, j int) bool {
	return p[i].Prefix.Less(p[j].Prefix)
}

// NewPrefixConfigMap creates a new prefix config map and sorts
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
// last is necessary for accounting and zone configs; ranges must not
// span accounting or zone config boundaries.
//
// Similarly, if the map contains successive prefix entries:
//
//   "/":           config1
//   "/db1":        config2
//   "/db1/table1": config3
//   "/db1/table2": config4
//   "/db2":        config5
//
// ...then entries will be added for (note that we don't add a
// redundant entry for /db2 or /db1/table2).:
//
//   "/db1/table3": config2
//   "/db3":        config1
func NewPrefixConfigMap(configs []*PrefixConfig) (PrefixConfigMap, error) {
	p := PrefixConfigMap(configs)
	sort.Sort(p)

	if len(p) == 0 || !p[0].Prefix.Equal(engine.KeyMin) {
		return nil, util.Errorf("no default prefix specified")
	}

	prefixSet := map[string]struct{}{}
	for _, entry := range p {
		prefixSet[string(entry.Prefix)] = struct{}{}
	}

	var newConfigs []*PrefixConfig
	stack := list.New()

	for i, entry := range p {
		// Check for duplicates in the original set of prefix configs.
		if i > 0 && entry.Prefix.Equal(p[i-1].Prefix) {
			return nil, util.Errorf("duplicate prefix found while building map: %q", entry.Prefix)
		}
		// Pop entries from the stack which aren't prefixes.
		for stack.Len() > 0 && !bytes.HasPrefix(entry.Prefix, stack.Back().Value.(*PrefixConfig).Prefix) {
			stack.Remove(stack.Back())
		}
		// Add additional entry to mark the end of key prefix range as
		// long as there's not an existing range that starts there.
		if _, ok := prefixSet[string(entry.Prefix.PrefixEnd())]; !ok && stack.Len() != 0 {
			newConfigs = append(newConfigs, &PrefixConfig{
				Prefix:    entry.Prefix.PrefixEnd(),
				Canonical: stack.Back().Value.(*PrefixConfig).Prefix,
				Config:    stack.Back().Value.(*PrefixConfig).Config,
			})
		}
		stack.PushBack(entry)
	}

	// Add newly created configs and re-sort.
	for _, config := range newConfigs {
		p = append(p, config)
	}
	sort.Sort(p)

	return p, nil
}

// MatchByPrefix returns the longest matching PrefixConfig. If the key
// specified does not match an existing prefix, a panic will
// result. Based on the comments in build(), that example will have a
// final list of PrefixConfig entries which look like:
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
func (p PrefixConfigMap) MatchByPrefix(key proto.Key) *PrefixConfig {
	n := sort.Search(len(p), func(i int) bool {
		return key.Compare(p[i].Prefix) < 0
	})
	if n == 0 || n > len(p) {
		panic("should never match a key outside of default range")
	}
	// If the matched prefix config is already canonical, return it immediately.
	pc := p[n-1]
	if pc.Canonical == nil {
		return pc
	}
	// Otherwise, search for the canonical prefix config.
	n = sort.Search(len(p), func(i int) bool {
		return pc.Canonical.Compare(p[i].Prefix) <= 0
	})
	// Should find an exact match every time.
	if n >= len(p) || !pc.Canonical.Equal(p[n].Prefix) {
		panic(fmt.Sprintf("canonical lookup for key %q failed", string(pc.Canonical)))
	}
	return p[n]
}

// MatchesByPrefix returns a list of PrefixConfig objects with
// prefixes satisfying the specified key. The results are returned in
// order of longest matching prefix to shortest.
func (p PrefixConfigMap) MatchesByPrefix(key proto.Key) []*PrefixConfig {
	var configs []*PrefixConfig
	prefix := key
	for {
		config := p.MatchByPrefix(prefix)
		configs = append(configs, config)
		prefix = config.Prefix
		if len(prefix) == 0 {
			return configs
		}
		// Truncate final character and loop.
		prefix = prefix[0 : len(prefix)-1]
	}
}

// VisitPrefixesHierarchically invokes the visitor function for each
// prefix matching the key argument, from longest matching prefix to
// shortest. If visitor returns done=true or an error, the visitation
// is halted.
func (p PrefixConfigMap) VisitPrefixesHierarchically(key proto.Key,
	visitor func(start, end proto.Key, config interface{}) (bool, error)) error {
	prefixConfigs := p.MatchesByPrefix(key)
	for _, pc := range prefixConfigs {
		done, err := visitor(pc.Prefix, pc.Prefix.PrefixEnd(), pc.Config)
		if done || err != nil {
			return err
		}
	}
	return nil
}

// VisitPrefixes invokes the visitor function for each prefix overlapped
// by the specified key range [start, end). If visitor returns done=true
// or an error, the visitation is halted.
func (p PrefixConfigMap) VisitPrefixes(start, end proto.Key,
	visitor func(start, end proto.Key, config interface{}) (bool, error)) error {
	comp := start.Compare(end)
	if comp > 0 {
		return util.Errorf("start key %q not less than or equal to end key %q", start, end)
	}
	startIdx := sort.Search(len(p), func(i int) bool {
		return start.Compare(p[i].Prefix) < 0
	})
	// Common case of start == end.
	endIdx := startIdx
	if comp != 0 {
		endIdx = sort.Search(len(p), func(i int) bool {
			return end.Compare(p[i].Prefix) < 0
		})
	}

	if startIdx > len(p) || endIdx > len(p) {
		return util.Errorf("start and/or end keys (%q, %q) fall outside prefix range; "+
			"startIdx: %d, endIdx: %d, len(p): %d", start, end, startIdx, endIdx, len(p))
	}

	if startIdx == endIdx {
		_, err := visitor(start, end, p[startIdx-1].Config)
		return err
	}
	for i := startIdx; i < endIdx; i++ {
		done, err := visitor(start, p[i].Prefix, p[i-1].Config)
		if done || err != nil {
			return err
		}
		if p[i].Prefix.Equal(end) {
			return nil
		}
		start = p[i].Prefix
	}
	done, err := visitor(start, end, p[endIdx-1].Config)
	if done || err != nil {
		return err
	}

	return nil
}

// SplitRangeByPrefixes returns a list of key ranges with
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
// After calling PrefixConfigMap.build(), our prefixes will look
// like:
//
//   /:    config1
//   /db1: config2
//   /db2: config1
//
// The algorithm is straightforward for splitting a range by existing
// prefixes. Lookup start key; that is first config. Lookup end key:
// that is last config. We then step through the intervening
// PrefixConfig records and create a RangeResult for each.
func (p PrefixConfigMap) SplitRangeByPrefixes(start, end proto.Key) ([]*RangeResult, error) {
	var results []*RangeResult
	err := p.VisitPrefixes(start, end, func(start, end proto.Key, config interface{}) (bool, error) {
		results = append(results, &RangeResult{start: start, end: end, config: config})
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}
