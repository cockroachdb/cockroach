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
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
		if !leftVal.EqualData(rightVal) {
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
		id, err := keys.DecodeDescMetadataID(s.Values[highIndex-1].Key)
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
		id, err = keys.DecodeDescMetadataID(searchSlice[i].Key)
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
		id, err := keys.DecodeDescMetadataID(searchSlice[maxIdx].Key)
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
	id, err := keys.DecodeDescMetadataID(searchSlice[maxIdx-1].Key)
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
	} else if bytes.HasPrefix(key, keys.SystemPrefix) {
		if bytes.HasPrefix(key, keys.NodeLivenessPrefix) {
			objectID = keys.LivenessRangesID
		} else if bytes.HasPrefix(key, keys.TimeseriesPrefix) {
			objectID = keys.TimeseriesRangesID
		} else {
			objectID = keys.SystemRangesID
		}
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

var staticSplits = []roachpb.RKey{
	roachpb.RKey(keys.SystemPrefix),                 // end of meta records / start of system ranges
	roachpb.RKey(keys.NodeLivenessPrefix),           // start of node liveness span
	roachpb.RKey(keys.NodeLivenessKeyMax),           // end of node liveness span
	roachpb.RKey(keys.TimeseriesPrefix),             // start of timeseries span
	roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), // end of timeseries span
	roachpb.RKey(keys.TableDataMin),                 // end of system ranges / start of system config tables
}

// StaticSplits are predefined split points in the system keyspace.
//
// There are two reasons for a static split. First, spans that are critical to
// cluster stability, like the node liveness span, are split into their own
// ranges to ease debugging (see #17297). Second, spans in the system keyspace
// that can be targeted by zone configs, like the meta span and the timeseries
// span, are split off into their own ranges because zone configs cannot apply
// to fractions of a range.
func StaticSplits() []roachpb.RKey {
	return staticSplits
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
	for _, split := range staticSplits {
		if startKey.Less(split) {
			if split.Less(endKey) {
				// The split point is contained within [startKey, endKey), so we need to
				// create the split.
				return split
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
			tableKey := roachpb.RKey(keys.MakeTablePrefix(id))
			// This logic is analogous to the well-commented static split logic above.
			if startKey.Less(tableKey) {
				if tableKey.Less(endKey) {
					return tableKey
				}
				return nil
			}

			zoneVal := s.GetValue(MakeZoneKey(id))
			if zoneVal == nil {
				continue
			}
			var zone ZoneConfig
			if err := zoneVal.GetProto(&zone); err != nil {
				// An error while decoding the zone proto is unfortunate, but logging a
				// message here would be excessively spammy. Just move on, which
				// effectively assumes there are no subzones for this table.
				continue
			}
			// This logic is analogous to the well-commented static split logic above.
			for _, s := range zone.subzoneSplits() {
				subzoneKey := append(tableKey, s...)
				if startKey.Less(subzoneKey) {
					if subzoneKey.Less(endKey) {
						return subzoneKey
					}
					return nil
				}
			}
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
