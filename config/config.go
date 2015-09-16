// Copyright 2015 The Cockroach Authors.
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
// Author: Marc Berhault (marc@cockroachlabs.com)

package config

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

var (
	// DefaultZoneConfig is the default zone configuration
	// used when no custom config has been specified.
	DefaultZoneConfig = &ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{},
			{},
			{},
		},
		RangeMinBytes: 1048576,
		RangeMaxBytes: 67108864,
		GC: &GCPolicy{
			TTLSeconds: 24 * 60 * 60, // 1 day
		},
	}

	// ZoneConfigHook is a function used to lookup a zone config given a table
	// or database ID.
	// This is also used by testing to simplify fake configs.
	ZoneConfigHook func(*SystemConfig, uint32) (*ZoneConfig, error)

	// TestingLargestIDHook is a function used to bypass GetLargestObjectID
	// in tests.
	TestingLargestIDHook func() uint32

	// TestingDisableTableSplits is a testing-only variable that disables
	// splits of tables into separate ranges.
	TestingDisableTableSplits bool
)

// ObjectIDForKey returns the object ID (table or database) for 'key',
// or (_, false) if not within the structured key space.
func ObjectIDForKey(key proto.Key) (uint32, bool) {
	if key.Equal(proto.KeyMax) {
		return 0, false
	}
	if key.Equal(keys.TableDataPrefix) {
		// TODO(marc): this should eventually return SystemDatabaseID.
		return 0, false
	}
	remaining := bytes.TrimPrefix(key, keys.TableDataPrefix)
	if len(remaining) == len(key) {
		// TrimPrefix returns the input untouched if the prefix doesn't match.
		return 0, false
	}

	// Consume first encoded int.
	defer func() {
		// Nothing to do, default return values mean "could not decode", which is
		// definitely the case if DecodeUvarint panics.
		_ = recover()
	}()
	_, id64 := encoding.DecodeUvarint(remaining)
	return uint32(id64), true
}

// Get searches the kv list for 'key' and returns its
// raw byte value if found. ok is true only if the key is found.
func (s *SystemConfig) Get(key proto.Key) ([]byte, bool) {
	l := len(s.Values)
	index := sort.Search(l, func(i int) bool {
		return bytes.Compare(s.Values[i].Key, key) >= 0
	})
	if index == l || !key.Equal(s.Values[index].Key) {
		return nil, false
	}
	// TODO(marc): I'm pretty sure a Value returned by MVCCScan can
	// never be nil. Should check.
	return s.Values[index].Value.Bytes, true
}

// GetLargestObjectID returns the largest object ID found in the config.
// This could be either a table or a database.
func (s *SystemConfig) GetLargestObjectID() (uint32, error) {
	if TestingLargestIDHook != nil {
		return TestingLargestIDHook(), nil
	}

	if len(s.Values) == 0 {
		return 0, fmt.Errorf("empty system values in config")
	}

	// Search for the first key after the descriptor table.
	// We can't use Get as we don't mind if there is nothing after
	// the descriptor table.
	key := keys.MakeTablePrefix(keys.DescriptorTableID + 1)
	index := sort.Search(len(s.Values), func(i int) bool {
		return bytes.Compare(s.Values[i].Key, key) >= 0
	})

	if index == 0 {
		return 0, fmt.Errorf("descriptor table not found in system config of %d values", len(s.Values))
	}

	// This is the last key of the descriptor table.
	key = s.Values[index-1].Key

	// Extract object ID from key.
	// TODO(marc): move sql/keys.go to keys (or similar) and use a DecodeDescMetadataKey.
	// We should also check proper encoding.
	descriptorPrefix := keys.MakeTablePrefix(keys.DescriptorTableID)
	remaining := bytes.TrimPrefix(key, descriptorPrefix)
	// TrimPrefix returns the bytes unchanged if the prefix does not match.
	if len(remaining) == len(key) {
		return 0, fmt.Errorf("descriptor table not found in system config of %d values", len(s.Values))
	}
	// DescriptorTable.PrimaryIndex.ID
	remaining, _ = encoding.DecodeUvarint(remaining)
	// descID
	_, id := encoding.DecodeUvarint(remaining)
	return uint32(id), nil
}

// GetZoneConfigForKey looks up the zone config for the range containing 'key'.
// It is the caller's responsibility to ensure that the range does not need to be split.
func (s *SystemConfig) GetZoneConfigForKey(key proto.Key) (*ZoneConfig, error) {
	if objectID, ok := ObjectIDForKey(key); ok {
		return s.GetZoneConfigForID(objectID)
	}
	// Not in the structured data namespace.
	return DefaultZoneConfig, nil
}

// GetZoneConfigForID looks up the zone config for the object (table or database)
// with 'id'.
func (s *SystemConfig) GetZoneConfigForID(id uint32) (*ZoneConfig, error) {
	if ZoneConfigHook == nil {
		return nil, util.Errorf("ZoneConfigHook not set, unable to lookup zone config")
	}
	// For now, only user databases and tables get custom zone configs.
	if id <= keys.MaxReservedDescID {
		return DefaultZoneConfig, nil
	}
	return ZoneConfigHook(s, id)
}

// ComputeSplitKeys takes a start and end key and returns an array of keys
// at which to split the span [start, end).
// The only required splits are at each user table prefix.
func (s *SystemConfig) ComputeSplitKeys(startKey, endKey proto.Key) []proto.Key {
	if TestingDisableTableSplits {
		return nil
	}

	tableStart := proto.Key(keys.UserTableDataMin)
	if !tableStart.Less(endKey) {
		// This range is before the user tables span: no required splits.
		return nil
	}

	startID, ok := ObjectIDForKey(startKey)
	if !ok || startID <= keys.MaxReservedDescID {
		// The start key is either:
		// - not part of the structured data span
		// - part of the system span
		// In either case, start looking for splits at the first ID usable
		// by the user data span.
		startID = keys.MaxReservedDescID + 1
	} else {
		// The start key is either already a split key, or after the split
		// key for its ID. We can skip straight to the next one.
		startID++
	}

	// Find the largest object ID.
	// We can't keep splitting until we reach endKey as it could be proto.KeyMax.
	endID, err := s.GetLargestObjectID()
	if err != nil {
		log.Errorf("unable to determine largest object ID from system config: %s", err)
		return nil
	}

	// Build key prefixes for sequential table IDs until we reach endKey.
	var splitKeys proto.KeySlice
	var key proto.Key
	// endID could be smaller than startID if we don't have user tables.
	for id := startID; id <= endID; id++ {
		key = keys.MakeTablePrefix(id)
		// Skip if the range starts on a split key.
		if !startKey.Less(key) {
			continue
		}
		// Handle the case where EndKey is already a table prefix.
		if !key.Less(endKey) {
			break
		}
		splitKeys = append(splitKeys, key)
	}

	return splitKeys
}
