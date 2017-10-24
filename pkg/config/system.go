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
// permissions and limitations under the License.

package config

import (
	"bytes"
	"fmt"
	"sort"

	"golang.org/x/net/context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type zoneConfigHook func(
	sysCfg SystemConfig, objectID uint32, keySuffix []byte,
) (zoneCfg ZoneConfig, found bool, err error)

var (
	// ZoneConfigHook is a function used to lookup a zone config given a table
	// or database ID.
	// This is also used by testing to simplify fake configs.
	ZoneConfigHook zoneConfigHook

	// testingLargestIDHook is a function used to bypass GetLargestObjectID
	// in tests.
	testingLargestIDHook func(uint32) uint32
)

// Equal checks for equality.
//
// It assumes that s.Values and other.Values are sorted in key order.
func (s SystemConfig) Equal(other SystemConfig) bool {
	if len(s.Values) != len(other.Values) {
		return false
	}
	for i := range s.Values {
		leftKV, rightKV := s.Values[i], other.Values[i]
		if !leftKV.Key.Equal(rightKV.Key) {
			return false
		}
		leftVal, rightVal := leftKV.Value, rightKV.Value
		if !bytes.Equal(leftVal.RawBytes, rightVal.RawBytes) {
			return false
		}
		if leftVal.Timestamp != rightVal.Timestamp {
			return false
		}
	}
	return true
}

// GetValue searches the kv list for 'key' and returns its
// roachpb.Value if found.
func (s SystemConfig) GetValue(key roachpb.Key) *roachpb.Value {
	if kv := s.get(key); kv != nil {
		return &kv.Value
	}
	return nil
}

// get searches the kv list for 'key' and returns its roachpb.KeyValue
// if found.
func (s SystemConfig) get(key roachpb.Key) *roachpb.KeyValue {
	if index, found := s.GetIndex(key); found {
		// TODO(marc): I'm pretty sure a Value returned by MVCCScan can
		// never be nil. Should check.
		return &s.Values[index]
	}
	return nil
}

// GetIndex searches the kv list for 'key' and returns its index if found.
func (s SystemConfig) GetIndex(key roachpb.Key) (int, bool) {
	l := len(s.Values)
	index := sort.Search(l, func(i int) bool {
		return bytes.Compare(s.Values[i].Key, key) >= 0
	})
	if index == l || !key.Equal(s.Values[index].Key) {
		return 0, false
	}
	return index, true
}

func decodeDescMetadataID(key roachpb.Key) (uint64, error) {
	// Extract object ID from key.
	// TODO(marc): move sql/keys.go to keys (or similar) and use a DecodeDescMetadataKey.
	// We should also check proper encoding.
	remaining, tableID, err := keys.DecodeTablePrefix(key)
	if err != nil {
		return 0, err
	}
	if tableID != keys.DescriptorTableID {
		return 0, errors.Errorf("key is not a descriptor table entry: %v", key)
	}
	// DescriptorTable.PrimaryIndex.ID
	remaining, _, err = encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return 0, err
	}
	// descID
	_, id, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// GetLargestObjectID returns the largest object ID found in the config which is
// less than or equal to maxID. If maxID is 0, returns the largest ID in the
// config.
func (s SystemConfig) GetLargestObjectID(maxID uint32) (uint32, error) {
	testingLock.Lock()
	hook := testingLargestIDHook
	testingLock.Unlock()
	if hook != nil {
		return hook(maxID), nil
	}

	// Search for the descriptor table entries within the SystemConfig.
	highBound := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID + 1))
	highIndex := sort.Search(len(s.Values), func(i int) bool {
		return bytes.Compare(s.Values[i].Key, highBound) >= 0
	})
	lowBound := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	lowIndex := sort.Search(len(s.Values), func(i int) bool {
		return bytes.Compare(s.Values[i].Key, lowBound) >= 0
	})

	if highIndex == lowIndex {
		return 0, fmt.Errorf("descriptor table not found in system config of %d values", len(s.Values))
	}

	// No maximum specified; maximum ID is the last entry in the descriptor
	// table.
	if maxID == 0 {
		id, err := decodeDescMetadataID(s.Values[highIndex-1].Key)
		if err != nil {
			return 0, err
		}
		return uint32(id), nil
	}

	// Maximum specified: need to search the descriptor table.  Binary search
	// through all descriptor table values to find the first descriptor with ID
	// >= maxID.
	searchSlice := s.Values[lowIndex:highIndex]
	var err error
	maxIdx := sort.Search(len(searchSlice), func(i int) bool {
		var id uint64
		id, err = decodeDescMetadataID(searchSlice[i].Key)
		if err != nil {
			return false
		}
		return uint32(id) >= maxID
	})
	if err != nil {
		return 0, err
	}

	// If we found an index within the list, maxIdx might point to a descriptor
	// with exactly maxID.
	if maxIdx < len(searchSlice) {
		id, err := decodeDescMetadataID(searchSlice[maxIdx].Key)
		if err != nil {
			return 0, err
		}
		if uint32(id) == maxID {
			return uint32(id), nil
		}
	}

	if maxIdx == 0 {
		return 0, fmt.Errorf("no descriptors present with ID < %d", maxID)
	}

	// Return ID of the immediately preceding descriptor.
	id, err := decodeDescMetadataID(searchSlice[maxIdx-1].Key)
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}

// GetZoneConfigForKey looks up the zone config for the range containing 'key'.
// It is the caller's responsibility to ensure that the range does not need to be split.
func (s SystemConfig) GetZoneConfigForKey(key roachpb.RKey) (ZoneConfig, error) {
	objectID, keySuffix, ok := DecodeObjectID(key)
	if !ok {
		// Not in the structured data namespace.
		objectID = keys.RootNamespaceID
	} else if objectID <= keys.MaxReservedDescID {
		// For now, you can only set a zone config on the system database as a whole,
		// not on any of its constituent tables. This is largely because all the
		// "system config" tables are colocated in the same range by default and
		// thus couldn't be managed separately.
		objectID = keys.SystemDatabaseID
	}

	// Special-case known system ranges to their special zone configs.
	if key.Equal(roachpb.RKeyMin) || bytes.HasPrefix(key, keys.Meta1Prefix) || bytes.HasPrefix(key, keys.Meta2Prefix) {
		objectID = keys.MetaRangesID
	} else if bytes.HasPrefix(key, keys.TimeseriesPrefix) {
		objectID = keys.TimeseriesRangesID
	} else if bytes.HasPrefix(key, keys.SystemPrefix) {
		objectID = keys.SystemRangesID
	}

	return s.getZoneConfigForID(objectID, keySuffix)
}

// getZoneConfigForID looks up the zone config for the object (table or database)
// with 'id'.
func (s SystemConfig) getZoneConfigForID(id uint32, keySuffix []byte) (ZoneConfig, error) {
	testingLock.Lock()
	hook := ZoneConfigHook
	testingLock.Unlock()
	if cfg, found, err := hook(s, id, keySuffix); err != nil || found {
		return cfg, err
	}
	return DefaultZoneConfig(), nil
}

// StaticSplits is the list of pre-defined split points in the beginning of
// the keyspace that are there to support separate zone configs for different
// parts of the system / system config ranges.
// Exposed publicly so that its ordering can be tested.
var StaticSplits = []struct {
	SplitPoint roachpb.RKey
	SplitKey   roachpb.RKey
}{
	// End of meta records / start of system ranges
	{
		SplitPoint: roachpb.RKey(keys.SystemPrefix),
		SplitKey:   roachpb.RKey(keys.SystemPrefix),
	},
	// Start of node liveness span.
	{
		SplitPoint: roachpb.RKey(keys.NodeLivenessPrefix),
		SplitKey:   roachpb.RKey(keys.NodeLivenessPrefix),
	},
	// End of node liveness span.
	{
		SplitPoint: roachpb.RKey(keys.NodeLivenessKeyMax),
		SplitKey:   roachpb.RKey(keys.NodeLivenessKeyMax),
	},
	// Start of timeseries ranges (within system ranges)
	{
		SplitPoint: roachpb.RKey(keys.TimeseriesPrefix),
		SplitKey:   roachpb.RKey(keys.TimeseriesPrefix),
	},
	// End of timeseries ranges (continuation of system ranges)
	{
		SplitPoint: roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()),
		SplitKey:   roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()),
	},
	// System config tables (end of system ranges)
	{
		SplitPoint: roachpb.RKey(keys.TableDataMin),
		SplitKey:   keys.SystemConfigSplitKey,
	},
}

// ComputeSplitKey takes a start and end key and returns the first key at which
// to split the span [start, end). Returns nil if no splits are required.
//
// Splits are required between user tables (i.e. /table/<id>), at the start
// of the system-config tables (i.e. /table/0), and at certain points within the
// system ranges that come before the system tables. The system-config range is
// somewhat special in that it can contain multiple SQL tables
// (/table/0-/table/<max-system-config-desc>) within a single range.
func (s SystemConfig) ComputeSplitKey(startKey, endKey roachpb.RKey) roachpb.RKey {
	// Before dealing with splits necessitated by SQL tables, handle all of the
	// static splits earlier in the keyspace. Note that this list must be kept in
	// the proper order (ascending in the keyspace) for the logic below to work.
	for _, split := range StaticSplits {
		if startKey.Less(split.SplitPoint) {
			if split.SplitPoint.Less(endKey) {
				// The split point is contained within [startKey, endKey), so we need to
				// create the split.
				return split.SplitKey
			}
			// [startKey, endKey) is contained between the previous split point and
			// this split point.
			return nil
		}
		// [startKey, endKey) is somewhere greater than this split point. Continue.
	}

	// If the above iteration over the static split points didn't decide anything,
	// the key range must be somewhere in the SQL table part of the keyspace.
	startID, _, ok := DecodeObjectID(startKey)
	if !ok || startID <= keys.MaxSystemConfigDescID {
		// The start key is either:
		// - not part of the structured data span
		// - part of the system span
		// In either case, start looking for splits at the first ID usable
		// by the user data span.
		startID = keys.MaxSystemConfigDescID + 1
	} else {
		// The start key is either already a split key, or after the split
		// key for its ID. We can skip straight to the next one.
		startID++
	}

	// Build key prefixes for sequential table IDs until we reach endKey. Note
	// that there are two disjoint sets of sequential keys: non-system reserved
	// tables have sequential IDs, as do user tables, but the two ranges contain a
	// gap.

	// findSplitKey returns the first possible split key between the given range
	// of IDs.
	findSplitKey := func(startID, endID uint32) roachpb.RKey {
		// endID could be smaller than startID if we don't have user tables.
		for id := startID; id <= endID; id++ {
			key := roachpb.RKey(keys.MakeTablePrefix(id))
			// Skip if this ID matches the provided startKey.
			if !startKey.Less(key) {
				continue
			}
			// Handle the case where EndKey is already a table prefix.
			if !key.Less(endKey) {
				break
			}
			return key
		}
		return nil
	}

	// If the startKey falls within the non-system reserved range, compute those
	// keys first.
	if startID <= keys.MaxReservedDescID {
		endID, err := s.GetLargestObjectID(keys.MaxReservedDescID)
		if err != nil {
			log.Errorf(context.TODO(), "unable to determine largest reserved object ID from system config: %s", err)
			return nil
		}
		if splitKey := findSplitKey(startID, endID); splitKey != nil {
			return splitKey
		}
		startID = keys.MaxReservedDescID + 1
	}

	// Find the split key in the user space.
	endID, err := s.GetLargestObjectID(0)
	if err != nil {
		log.Errorf(context.TODO(), "unable to determine largest object ID from system config: %s", err)
		return nil
	}
	return findSplitKey(startID, endID)
}

// NeedsSplit returns whether the range [startKey, endKey) needs a split due
// to zone configs.
func (s SystemConfig) NeedsSplit(startKey, endKey roachpb.RKey) bool {
	return len(s.ComputeSplitKey(startKey, endKey)) > 0
}
