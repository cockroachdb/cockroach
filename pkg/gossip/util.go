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
	lastCfg   config.SystemConfig
}

// MakeSystemConfigDeltaFilter creates a new SystemConfigDeltaFilter. The filter
// will ignore all key-values without the specified key prefix, if one is
// provided.
func MakeSystemConfigDeltaFilter(keyPrefix roachpb.Key) SystemConfigDeltaFilter {
	return SystemConfigDeltaFilter{
		keyPrefix: keyPrefix,
	}
}

// ForModified calls the provided function for all SystemConfig kvs that were modified
// since the last call to this method.
func (df *SystemConfigDeltaFilter) ForModified(
	newCfg config.SystemConfig, fn func(kv roachpb.KeyValue),
) {
	// SystemConfig values are always sorted by key, so scan over new and old
	// configs in order to find new keys and modified values.
	lastIdx := 0
	for _, newKV := range newCfg.Values {
		// Skip unimportant kvs.
		if df.keyPrefix != nil && !bytes.HasPrefix(newKV.Key, df.keyPrefix) {
			continue
		}

		// Find matching key in lastCfg.
		for {
			if lastIdx >= len(df.lastCfg.Values) {
				// New key.
				fn(newKV)
				break
			}

			oldKV := df.lastCfg.Values[lastIdx]
			if cmp := oldKV.Key.Compare(newKV.Key); cmp < 0 {
				// Deleted key.
				lastIdx++
			} else if cmp == 0 {
				// Matching key.
				lastIdx++
				if !bytes.Equal(newKV.Value.RawBytes, oldKV.Value.RawBytes) {
					// Modified value.
					fn(newKV)
				}
				break
			} else if cmp > 0 {
				// New key.
				fn(newKV)
				break
			}
		}
	}
	df.lastCfg = newCfg
}
