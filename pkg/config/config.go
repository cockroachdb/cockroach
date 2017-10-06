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
	"strings"

	yaml "gopkg.in/yaml.v2"

	"golang.org/x/net/context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Several ranges outside of the SQL keyspace are given special names so they
// can be targeted by zone configs.
const (
	DefaultZoneName    = "default"
	MetaZoneName       = "meta"
	SystemZoneName     = "system"
	TimeseriesZoneName = "timeseries"
)

// NamedZones maps named zones to their psuedo-table ID that can be used to
// install an entry into the system.zones table.
var NamedZones = map[string]uint32{
	DefaultZoneName:    keys.RootNamespaceID,
	MetaZoneName:       keys.MetaRangesID,
	SystemZoneName:     keys.SystemRangesID,
	TimeseriesZoneName: keys.TimeseriesRangesID,
}

// NamedZonesByID is the inverse of NamedZones: it maps psuedo-table IDs to
// their zone names.
var NamedZonesByID = func() map[uint32]string {
	out := map[uint32]string{}
	for name, id := range NamedZones {
		out[id] = name
	}
	return out
}()

// ZoneSpecifier identifies either a special named zone or the zone associated
// with a particular database or table. Either NamedZone or Database must be
// non-nil; Table should only be non-nil if Database is non-nil.
type ZoneSpecifier struct {
	NamedZone *string
	Database  *string
	Table     *string
}

// ZoneSpecifierFromID creates a zone specifier for the zone with the given ID.
func ZoneSpecifierFromID(
	id uint32,
	resolveID func(id uint32) (parentID uint32, name string, err error),
) (ZoneSpecifier, error) {
	if name, ok := NamedZonesByID[id]; ok {
		return ZoneSpecifier{NamedZone: &name}, nil
	}
	parentID, name, err := resolveID(id)
	if err != nil {
		return ZoneSpecifier{}, err
	}
	if parentID == keys.RootNamespaceID {
		return ZoneSpecifier{Database: &name}, nil
	}
	_, dbName, err := resolveID(parentID)
	if err != nil {
		return ZoneSpecifier{}, err
	}
	return ZoneSpecifier{Database: &dbName, Table: &name}, nil
}

// ParseCLIZoneSpecifier converts a single string s identifying a zone, as would
// be used to name a zone on the command line, to a ZoneSpecifier. A valid CLI
// zone specifier is either 1) a database or table reference of the form
// DATABASE[.TABLE], or 2) a special named zone of the form [.NAME].
func ParseCLIZoneSpecifier(s string) (ZoneSpecifier, error) {
	if len(s) > 0 && s[0] == '.' {
		name := s[1:]
		return ZoneSpecifier{NamedZone: &name}, nil
	}
	// ParseTableName is not vulnerable to SQL injection, so passing s directly
	// is safe. See #8389 for details.
	tn, err := parser.ParseTableName(s)
	if err != nil {
		return ZoneSpecifier{}, fmt.Errorf("malformed name: %s", s)
	}
	db := tn.Database()
	if db == "" {
		// No database was specified, so interpret the table name as the database.
		db = tn.Table()
		return ZoneSpecifier{Database: &db}, nil
	}
	table := tn.Table()
	return ZoneSpecifier{Database: &db, Table: &table}, nil
}

// ResolvePath converts a zone specifier to a slice of zone config IDs that
// might apply to the zone. The last zone config ID to actually exist in
// system.zones is the zone config that applies.
//
// Note that GetZoneConfigForKey performs the same logic without explicitly
// constructing a path. This function exists to support the CLI.
func (zs *ZoneSpecifier) ResolvePath(
	resolveName func(parentID uint32, name string) (id uint32, err error),
) ([]uint32, error) {
	path := []uint32{keys.RootNamespaceID}
	if zs.NamedZone != nil {
		if *zs.NamedZone == DefaultZoneName {
			return path, nil
		}
		if id, ok := NamedZones[*zs.NamedZone]; ok {
			return append(path, id), nil
		}
		return nil, fmt.Errorf("%q is not a built-in zone", *zs.NamedZone)
	}
	if zs.Database != nil {
		databaseID, err := resolveName(path[len(path)-1], *zs.Database)
		if err != nil {
			return nil, err
		}
		path = append(path, databaseID)
	}
	if zs.Table != nil {
		tableID, err := resolveName(path[len(path)-1], *zs.Table)
		if err != nil {
			return nil, err
		}
		path = append(path, tableID)
	}
	return path, nil
}

// CLISpecifier converts a ZoneSpecifier to a CLI zone specifier as described in
// ParseCLIZoneSpecifier.
func (zs *ZoneSpecifier) CLISpecifier() string {
	if zs.NamedZone != nil {
		return "." + *zs.NamedZone
	}
	if zs.Database == nil {
		panic("invalid ZoneSpecifier: both NamedZone and Database are nil")
	}
	if zs.Table != nil {
		return (&parser.TableName{
			DatabaseName: parser.Name(*zs.Database),
			TableName:    parser.Name(*zs.Table),
		}).String()
	}
	return parser.Name(*zs.Database).String()
}

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
			// Use 25 hours instead of the previous 24 to make users successful by
			// default. Users desiring to take incremental backups every 24h may
			// incorrectly assume that the previous default 24h was sufficient to do
			// that. But the equation for incremental backups is:
			// 	GC TTLSeconds >= (desired backup interval) + (time to perform incremental backup)
			// We think most new users' incremental backups will complete within an
			// hour, and larger clusters will have more experienced operators and will
			// understand how to change these settings if needed.
			TTLSeconds: 25 * 60 * 60,
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
	objectID, ok := ObjectIDForKey(key)
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
