// Copyright 2018 The Cockroach Authors.
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

package gossip

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// SystemConfigDeltaFilter keeps track of SystemConfig values so that unmodified
// values can be filtered out from a SystemConfig update. This can prevent
// repeatedly unmarshaling and processing the same SystemConfig values.
//
// A SystemConfigDeltaFilter is not safe for concurrent use by multiple
// goroutines.
type SystemConfigDeltaFilter struct {
	keyPrefix roachpb.Key
	lastCfg   *config.SystemConfig
}

// MakeSystemConfigDeltaFilter creates a new SystemConfigDeltaFilter. The filter
// will ignore all key-values without the specified key prefix, if one is
// provided.
func MakeSystemConfigDeltaFilter(keyPrefix roachpb.Key) SystemConfigDeltaFilter {
	return SystemConfigDeltaFilter{
		keyPrefix: keyPrefix,
		lastCfg:   config.NewSystemConfig(),
	}
}

// ForModified calls the provided function for all SystemConfig kvs that were modified
// since the last call to this method.
func (df *SystemConfigDeltaFilter) ForModified(
	newCfg *config.SystemConfig, fn func(kv roachpb.KeyValue),
) {
	// Save newCfg in the filter.
	lastCfg := df.lastCfg
	df.lastCfg = config.NewSystemConfig()
	df.lastCfg.Values = newCfg.Values

	// SystemConfig values are always sorted by key, so scan over new and old
	// configs in order to find new keys and modified values. Before doing so,
	// skip all keys in each list of values that are less than the keyPrefix.
	lastIdx, newIdx := 0, 0
	if df.keyPrefix != nil {
		lastIdx = sort.Search(len(lastCfg.Values), func(i int) bool {
			return bytes.Compare(lastCfg.Values[i].Key, df.keyPrefix) >= 0
		})
		newIdx = sort.Search(len(newCfg.Values), func(i int) bool {
			return bytes.Compare(newCfg.Values[i].Key, df.keyPrefix) >= 0
		})
	}

	for {
		if newIdx == len(newCfg.Values) {
			// All out of new keys.
			break
		}

		newKV := newCfg.Values[newIdx]
		if df.keyPrefix != nil && !bytes.HasPrefix(newKV.Key, df.keyPrefix) {
			// All out of new keys matching prefix.
			break
		}

		if lastIdx < len(lastCfg.Values) {
			oldKV := lastCfg.Values[lastIdx]
			switch oldKV.Key.Compare(newKV.Key) {
			case -1:
				// Deleted key.
				lastIdx++
			case 0:
				if !newKV.Value.EqualData(oldKV.Value) {
					// Modified value.
					fn(newKV)
				}
				lastIdx++
				newIdx++
			case 1:
				// New key.
				fn(newKV)
				newIdx++
			}
		} else {
			// New key.
			fn(newKV)
			newIdx++
		}
	}
}
