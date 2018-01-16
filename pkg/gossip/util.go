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
	"hash"
	"hash/crc32"

	"github.com/pkg/errors"

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
	hasher    hash.Hash32
	hashes    map[uint32]hashVal
	epoch     bool
}

type hashVal struct {
	epoch bool
	kv    roachpb.KeyValue
}

// MakeSystemConfigDeltaFilter creates a new SystemConfigDeltaFilter. The filter
// will ignore all key-values without the specified key prefix, if one is
// provided.
func MakeSystemConfigDeltaFilter(keyPrefix roachpb.Key) SystemConfigDeltaFilter {
	return SystemConfigDeltaFilter{
		keyPrefix: keyPrefix,
		hasher:    crc32.New(crc32.MakeTable(crc32.Castagnoli)),
		hashes:    make(map[uint32]hashVal),
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

		keyHash := df.hashKey(kv.Key)
		prevVal, ok := df.hashes[keyHash]
		if !ok || !bytes.Equal(kv.Value.RawBytes, prevVal.kv.Value.RawBytes) {
			// The key is new or the value changed.
			fn(kv)
		}

		df.hashes[keyHash] = hashVal{
			epoch: df.epoch,
			kv:    kv,
		}
	}

	// Delete the kvs that no longer exist in the system config from the map.
	//
	// We can expose the kvs that are being removed in this loop, if we ever
	// need to.
	for hash, val := range df.hashes {
		if val.epoch != df.epoch {
			delete(df.hashes, hash)
		}
	}
}

func (df *SystemConfigDeltaFilter) hashKey(key roachpb.Key) uint32 {
	if _, err := df.hasher.Write(key); err != nil {
		panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	hashVal := df.hasher.Sum32()
	df.hasher.Reset()
	return hashVal
}
