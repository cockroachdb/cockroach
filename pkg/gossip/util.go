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
	lastVals  map[string]*filterVal
	epoch     bool
}

type filterVal struct {
	epoch bool
	val   roachpb.Value
}

// MakeSystemConfigDeltaFilter creates a new SystemConfigDeltaFilter. The filter
// will ignore all key-values without the specified key prefix, if one is
// provided.
func MakeSystemConfigDeltaFilter(keyPrefix roachpb.Key) SystemConfigDeltaFilter {
	return SystemConfigDeltaFilter{
		keyPrefix: keyPrefix,
		lastVals:  make(map[string]*filterVal),
	}
}

// ForModified calls the provided function for all SystemConfig kvs that were modified
// since the last call to this method.
func (df *SystemConfigDeltaFilter) ForModified(
	cfg config.SystemConfig, fn func(kv roachpb.KeyValue),
) {
	// Invert epoch counter so we can detect which kvs were removed from the
	// map after updating all those that remain.
	df.epoch = !df.epoch
	for _, kv := range cfg.Values {
		// Skip unimportant kvs.
		if df.keyPrefix != nil && !bytes.HasPrefix(kv.Key, df.keyPrefix) {
			continue
		}

		keySlice := kv.Key[len(df.keyPrefix):]
		keyStr := string(keySlice)
		prevVal, ok := df.lastVals[keyStr]
		if !ok || !bytes.Equal(kv.Value.RawBytes, prevVal.val.RawBytes) {
			// The key is new or the value changed.
			fn(kv)
		}

		newVal := filterVal{epoch: df.epoch, val: kv.Value}
		if ok {
			// No allocations.
			*df.lastVals[keyStr] = newVal
		} else {
			// Both of these variables are split from a copy in the outer scope.
			// The first is so that all uses of keyStr can take advantage of
			// https://github.com/golang/go/wiki/CompilerOptimizations#string-and-byte.
			// The second is so that newVal doesn't escape onto the heap. Look
			// at memory benchmarks when making any changes here.
			keyStrAlloc := string(keySlice)
			newValAlloc := newVal
			df.lastVals[keyStrAlloc] = &newValAlloc
		}
	}

	// Delete the kvs that no longer exist in the system config from the map.
	//
	// We can expose the kvs that are being removed in this loop, if we ever
	// need to.
	for key, val := range df.lastVals {
		if val.epoch != df.epoch {
			delete(df.lastVals, key)
		}
	}
}
