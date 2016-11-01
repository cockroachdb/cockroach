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
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package config

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"sort"
	"strings"

	yaml "gopkg.in/yaml.v2"

	"golang.org/x/net/context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// minRangeMaxBytes is the minimum value for range max bytes.
	minRangeMaxBytes = 64 << 10 // 64 KB
)

type zoneConfigHook func(SystemConfig, uint32) (ZoneConfig, bool, error)

var (
	// defaultZoneConfig is the default zone configuration used when no custom
	// config has been specified.
	defaultZoneConfig = ZoneConfig{
		NumReplicas:   3,
		RangeMinBytes: 1 << 20,  // 1 MB
		RangeMaxBytes: 64 << 20, // 64 MB
		GC: GCPolicy{
			TTLSeconds: 24 * 60 * 60, // 1 day
		},
	}

	// ZoneConfigHook is a function used to lookup a zone config given a table
	// or database ID.
	// This is also used by testing to simplify fake configs.
	ZoneConfigHook zoneConfigHook

	// testingLargestIDHook is a function used to bypass GetLargestObjectID
	// in tests.
	testingLargestIDHook func(uint32) uint32
)

func (c Constraint) String() string {
	var str string
	switch c.Type {
	case Constraint_REQUIRED:
		str += "+"
	case Constraint_PROHIBITED:
		str += "-"
	}
	if len(c.Key) > 0 {
		str += c.Key + "="
	}
	str += c.Value
	return str
}

// FromString populates the constraint from the constraint shorthand notation.
func (c *Constraint) FromString(short string) error {
	switch short[0] {
	case '+':
		c.Type = Constraint_REQUIRED
		short = short[1:]
	case '-':
		c.Type = Constraint_PROHIBITED
		short = short[1:]
	default:
		c.Type = Constraint_POSITIVE
	}
	parts := strings.Split(short, "=")
	if len(parts) == 1 {
		c.Value = parts[0]
	} else if len(parts) == 2 {
		c.Key = parts[0]
		c.Value = parts[1]
	} else {
		return errors.Errorf("constraint needs to be in the form \"(key=)value\", not %q", short)
	}
	return nil
}

var _ yaml.Marshaler = Constraints{}
var _ yaml.Unmarshaler = &Constraints{}

// MarshalYAML implements yaml.Marshaler.
func (c Constraints) MarshalYAML() (interface{}, error) {
	short := make([]string, len(c.Constraints))
	for i, c := range c.Constraints {
		short[i] = c.String()
	}
	return short, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *Constraints) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var shortConstraints []string
	if err := unmarshal(&shortConstraints); err != nil {
		return err
	}
	constraints := make([]Constraint, len(shortConstraints))
	for i, short := range shortConstraints {
		if err := constraints[i].FromString(short); err != nil {
			return err
		}
	}
	c.Constraints = constraints
	return nil
}

// DefaultZoneConfig is the default zone configuration used when no custom
// config has been specified.
func DefaultZoneConfig() ZoneConfig {
	testingLock.Lock()
	defer testingLock.Unlock()
	return defaultZoneConfig
}

// TestingSetDefaultZoneConfig is a testing-only function that changes the
// default zone config and returns a function that reverts the change.
func TestingSetDefaultZoneConfig(cfg ZoneConfig) func() {
	testingLock.Lock()
	oldConfig := defaultZoneConfig
	defaultZoneConfig = cfg
	testingLock.Unlock()

	return func() {
		testingLock.Lock()
		defaultZoneConfig = oldConfig
		testingLock.Unlock()
	}
}

// Validate verifies some ZoneConfig fields.
// This should be used to validate user input when setting a new zone config.
func (z ZoneConfig) Validate() error {
	switch z.NumReplicas {
	case 0:
		return fmt.Errorf("attributes for at least one replica must be specified in zone config")
	case 2:
		return fmt.Errorf("at least 3 replicas are required for multi-replica configurations")
	}
	if z.RangeMaxBytes < minRangeMaxBytes {
		return fmt.Errorf("RangeMaxBytes %d less than minimum allowed %d",
			z.RangeMaxBytes, minRangeMaxBytes)
	}
	if z.RangeMinBytes >= z.RangeMaxBytes {
		return fmt.Errorf("RangeMinBytes %d is greater than or equal to RangeMaxBytes %d",
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
	_, id64, err := encoding.DecodeUvarintAscending(key)
	return uint32(id64), err == nil
}

// Hash returns a SHA1 hash of the SystemConfig contents.
func (s SystemConfig) Hash() []byte {
	sha := sha1.New()
	for _, kv := range s.Values {
		_, _ = sha.Write(kv.Key)
		// There are all kinds of different types here, so we can't use the typed
		// getters.
		_, _ = sha.Write(kv.Value.RawBytes)
	}
	return sha.Sum(nil)
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
	objectID, ok := ObjectIDForKey(key)
	if !ok {
		// Not in the structured data namespace.
		objectID = keys.RootNamespaceID
	} else if objectID <= keys.MaxReservedDescID {
		// For now, only user databases and tables get custom zone configs.
		objectID = keys.RootNamespaceID
	}
	return s.getZoneConfigForID(objectID)
}

// getZoneConfigForID looks up the zone config for the object (table or database)
// with 'id'.
func (s SystemConfig) getZoneConfigForID(id uint32) (ZoneConfig, error) {
	testingLock.Lock()
	hook := ZoneConfigHook
	testingLock.Unlock()
	if cfg, found, err := hook(s, id); err != nil || found {
		return cfg, err
	}
	return DefaultZoneConfig(), nil
}

// ComputeSplitKeys takes a start and end key and returns an array of keys
// at which to split the span [start, end).
// The only required splits are at each user table prefix.
func (s SystemConfig) ComputeSplitKeys(startKey, endKey roachpb.RKey) []roachpb.RKey {
	tableStart := roachpb.RKey(keys.SystemConfigTableDataMax)
	if !tableStart.Less(endKey) {
		// This range is before the user tables span: no required splits.
		return nil
	}

	startID, ok := ObjectIDForKey(startKey)
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
	var splitKeys []roachpb.RKey
	var key roachpb.RKey

	// appendSplitKeys generates all possible split keys between the given range
	// of IDs and adds them to splitKeys.
	appendSplitKeys := func(startID, endID uint32) {
		// endID could be smaller than startID if we don't have user tables.
		for id := startID; id <= endID; id++ {
			key = keys.MakeRowSentinelKey(keys.MakeTablePrefix(id))
			// Skip if this ID matches the startKey passed to ComputeSplitKeys.
			if !startKey.Less(key) {
				continue
			}
			// Handle the case where EndKey is already a table prefix.
			if !key.Less(endKey) {
				break
			}
			splitKeys = append(splitKeys, key)
		}
	}

	// If the startKey falls within the non-system reserved range, compute those
	// keys first.
	if startID <= keys.MaxReservedDescID {
		endID, err := s.GetLargestObjectID(keys.MaxReservedDescID)
		if err != nil {
			log.Errorf(context.TODO(), "unable to determine largest reserved object ID from system config: %s", err)
			return nil
		}
		appendSplitKeys(startID, endID)
		startID = keys.MaxReservedDescID + 1
	}

	// Append keys in the user space.
	endID, err := s.GetLargestObjectID(0)
	if err != nil {
		log.Errorf(context.TODO(), "unable to determine largest object ID from system config: %s", err)
		return nil
	}
	appendSplitKeys(startID, endID)

	return splitKeys
}

// NeedsSplit returns whether the range [startKey, endKey) needs a split due
// to zone configs.
func (s SystemConfig) NeedsSplit(startKey, endKey roachpb.RKey) bool {
	return len(s.ComputeSplitKeys(startKey, endKey)) > 0
}
