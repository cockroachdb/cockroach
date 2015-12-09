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
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// minRangeMaxBytes is the minimum value for range max bytes.
	// TODO(marc): should we revise this lower?
	minRangeMaxBytes = 1 << 20
)

var (
	// DefaultZoneConfig is the default zone configuration
	// used when no custom config has been specified.
	DefaultZoneConfig = &ZoneConfig{
		ReplicaAttrs: []roachpb.Attributes{
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
	ZoneConfigHook func(SystemConfig, uint32) (*ZoneConfig, error)

	// testingLargestIDHook is a function used to bypass GetLargestObjectID
	// in tests.
	testingLargestIDHook func() uint32

	// testingDisableTableSplits is a testing-only variable that disables
	// splits of tables into separate ranges.
	testingDisableTableSplits bool
)

// TestingDisableTableSplits is a testing-only function that disables
// splits of tables into separate ranges. It returns a function
// that re-enables this splitting.
func TestingDisableTableSplits() func() {
	testingLock.Lock()
	testingDisableTableSplits = true
	testingLock.Unlock()
	return func() {
		testingLock.Lock()
		testingDisableTableSplits = false
		testingLock.Unlock()
	}
}

// Validate verifies some ZoneConfig fields.
// This should be used to validate user input when setting a new zone config.
func (z ZoneConfig) Validate() error {
	if len(z.ReplicaAttrs) == 0 {
		return util.Errorf("attributes for at least one replica must be specified in zone config")
	}
	if z.RangeMaxBytes < minRangeMaxBytes {
		return util.Errorf("RangeMaxBytes %d less than minimum allowed %d", z.RangeMaxBytes, minRangeMaxBytes)
	}
	if z.RangeMinBytes >= z.RangeMaxBytes {
		return util.Errorf("RangeMinBytes %d is greater than or equal to RangeMaxBytes %d",
			z.RangeMinBytes, z.RangeMaxBytes)
	}
	return nil
}

// ObjectIDForKey returns the object ID (table or database) for 'key',
// or (_, false) if not within the structured key space.
func ObjectIDForKey(key roachpb.RKey) (uint32, bool) {
	if key.Equal(roachpb.RKeyMax) {
		return 0, false
	}
	if encoding.PeekType(key) != encoding.Int {
		// TODO(marc): this should eventually return SystemDatabaseID.
		return 0, false
	}
	// Consume first encoded int.
	_, id64, err := encoding.DecodeUvarint(key)
	return uint32(id64), err == nil
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

// GetLargestObjectID returns the largest object ID found in the config.
// This could be either a table or a database.
func (s SystemConfig) GetLargestObjectID() (uint32, error) {
	testingLock.Lock()
	hook := testingLargestIDHook
	testingLock.Unlock()
	if hook != nil {
		return hook(), nil
	}

	if len(s.Values) == 0 {
		return 0, fmt.Errorf("empty system values in config")
	}

	// Search for the first key after the descriptor table.
	// We can't use GetValue as we don't mind if there is nothing after
	// the descriptor table.
	key := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID + 1))
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
	remaining, _, err := encoding.DecodeUvarint(remaining)
	if err != nil {
		return 0, err
	}
	// descID
	_, id, err := encoding.DecodeUvarint(remaining)
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}

// GetZoneConfigForKey looks up the zone config for the range containing 'key'.
// It is the caller's responsibility to ensure that the range does not need to be split.
func (s SystemConfig) GetZoneConfigForKey(key roachpb.RKey) (*ZoneConfig, error) {
	if objectID, ok := ObjectIDForKey(key); ok {
		return s.getZoneConfigForID(objectID)
	}
	// Not in the structured data namespace.
	return DefaultZoneConfig, nil
}

// getZoneConfigForID looks up the zone config for the object (table or database)
// with 'id'.
func (s SystemConfig) getZoneConfigForID(id uint32) (*ZoneConfig, error) {
	testingLock.Lock()
	hook := ZoneConfigHook
	testingLock.Unlock()
	if hook == nil {
		return nil, util.Errorf("ZoneConfigHook not set, unable to lookup zone config")
	}
	// For now, only user databases and tables get custom zone configs.
	if id <= keys.MaxReservedDescID {
		return DefaultZoneConfig, nil
	}
	return hook(s, id)
}

// ComputeSplitKeys takes a start and end key and returns an array of keys
// at which to split the span [start, end).
// The only required splits are at each user table prefix.
func (s SystemConfig) ComputeSplitKeys(startKey, endKey roachpb.RKey) []roachpb.RKey {
	testingLock.Lock()
	tableSplitsDisabled := testingDisableTableSplits
	testingLock.Unlock()
	if tableSplitsDisabled {
		return nil
	}

	tableStart := roachpb.RKey(keys.UserTableDataMin)
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
	// We can't keep splitting until we reach endKey as it could be roachpb.KeyMax.
	endID, err := s.GetLargestObjectID()
	if err != nil {
		log.Errorf("unable to determine largest object ID from system config: %s", err)
		return nil
	}

	// Build key prefixes for sequential table IDs until we reach endKey.
	var splitKeys []roachpb.RKey
	var key roachpb.RKey
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

// NeedsSplit returns whether the range [startKey, endKey) needs a split due
// to zone configs.
func (s SystemConfig) NeedsSplit(startKey, endKey roachpb.RKey) bool {
	return len(s.ComputeSplitKeys(startKey, endKey)) > 0
}
