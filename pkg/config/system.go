// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ObjectID is an identifier for an object (e.g. database or table)
// in the system tenant. Each object in the system tenant is capable of being
// associated with a zone configuration, which describes how and where the
// object's data is stored in KV. Some objects in the system tenant also serve
// as Range split point boundaries, see ShouldSplitAtDesc.
//
// Identifiers for objects in secondary tenants are treated differently for the
// purposes of the system configuration. An individual object in a secondary
// tenant cannot be associated with a zone configuration. Instead, all secondary
// tenant data is associated with the "tenants" zone which is accessible only to
// the system tenant. Additionally, individual objects in secondary tenants do
// not serve as Range split boundaries. However, each tenant is guaranteed to be
// split off into its own range.
type ObjectID uint32

type zoneConfigHook func(
	sysCfg *SystemConfig, codec keys.SQLCodec, objectID ObjectID,
) (zone *zonepb.ZoneConfig, placeholder *zonepb.ZoneConfig, cache bool, err error)

var (
	// ZoneConfigHook is a function used to lookup a zone config given a system
	// tenant table or database ID.
	// This is also used by testing to simplify fake configs.
	ZoneConfigHook zoneConfigHook

	// testingLargestIDHook is a function used to bypass GetLargestObjectID
	// in tests.
	testingLargestIDHook func(maxID ObjectID) ObjectID
)

type zoneEntry struct {
	zone        *zonepb.ZoneConfig
	placeholder *zonepb.ZoneConfig

	// combined merges the zone and placeholder configs into a combined config.
	// If both have subzone information, the placeholder information is preferred.
	// This may never happen, but while the existing code gives preference to the
	// placeholder, there appear to be no guarantees that there can be no overlap.
	//
	// TODO(andyk): Use the combined value everywhere early in 19.2, so there's
	// enough bake time to ensure this is OK to do. Until then, only use the
	// combined value in GetZoneConfigForObject, which is only used by the
	// optimizer.
	combined *zonepb.ZoneConfig
}

// SystemConfig embeds a SystemConfigEntries message which contains an
// entry for every system descriptor (e.g. databases, tables, zone
// configs). It also has a map from object ID to unmarshaled zone
// config for caching.
// The shouldSplitCache caches information about the descriptor ID,
// saying whether or not it should be considered for splitting at all.
// A database descriptor or a table view descriptor are examples of IDs
// that should not be considered for splits.
// NB: SystemConfig can be updated to only contain system.descriptor and
// system.zones. We still need SystemConfig for SystemConfigProvider which is
// used in replication reports and the opt catalog.
type SystemConfig struct {
	SystemConfigEntries
	DefaultZoneConfig *zonepb.ZoneConfig
	mu                struct {
		syncutil.RWMutex
		zoneCache        map[ObjectID]zoneEntry
		shouldSplitCache map[ObjectID]bool
	}
}

// NewSystemConfig returns an initialized instance of SystemConfig.
func NewSystemConfig(defaultZoneConfig *zonepb.ZoneConfig) *SystemConfig {
	sc := &SystemConfig{}
	sc.DefaultZoneConfig = defaultZoneConfig
	sc.mu.zoneCache = map[ObjectID]zoneEntry{}
	sc.mu.shouldSplitCache = map[ObjectID]bool{}
	return sc
}

// Equal checks for equality.
//
// It assumes that s.Values and other.Values are sorted in key order.
func (s *SystemConfig) Equal(other *SystemConfigEntries) bool {
	if len(s.Values) != len(other.Values) {
		return false
	}
	for i := range s.Values {
		leftKV, rightKV := s.Values[i], other.Values[i]
		if !leftKV.Key.Equal(rightKV.Key) {
			return false
		}
		leftVal, rightVal := leftKV.Value, rightKV.Value
		if !leftVal.EqualTagAndData(rightVal) {
			return false
		}
		if leftVal.Timestamp != rightVal.Timestamp {
			return false
		}
	}
	return true
}

// getSystemTenantDesc looks for the descriptor value given a key, if a
// zone is created in a test without creating a Descriptor, a dummy
// descriptor is returned. If the key is invalid in decoding an ID,
// getDesc panics.
func (s *SystemConfig) getSystemTenantDesc(key roachpb.Key) *roachpb.Value {
	if getVal := s.GetValue(key); getVal != nil {
		return getVal
	}

	id, err := keys.SystemSQLCodec.DecodeDescMetadataID(key)
	if err != nil {
		// No ID found for key. No roachpb.Value corresponds to this key.
		panic(err)
	}

	testingLock.Lock()
	_, ok := testingZoneConfig[ObjectID(id)]
	testingLock.Unlock()

	if ok {
		// A test installed a zone config for this ID, but no descriptor.
		// Synthesize an empty descriptor to force split to occur, or else the
		// zone config won't apply to any ranges. Most tests that use
		// TestingSetZoneConfig are too low-level to create tables and zone
		// configs through proper channels.
		//
		// Getting here outside tests is impossible.
		desc := &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{}}}
		var val roachpb.Value
		if err := val.SetProto(desc); err != nil {
			panic(err)
		}
		return &val
	}
	return nil
}

// GetValue searches the kv list for 'key' and returns its
// roachpb.Value if found.
func (s *SystemConfig) GetValue(key roachpb.Key) *roachpb.Value {
	if kv := s.get(key); kv != nil {
		return &kv.Value
	}
	return nil
}

// get searches the kv list for 'key' and returns its roachpb.KeyValue
// if found.
func (s *SystemConfig) get(key roachpb.Key) *roachpb.KeyValue {
	if i, ok := s.GetIndex(key); ok {
		return &s.Values[i]
	}
	return nil
}

// GetIndex searches the kv list for 'key' and returns its index if found.
func (s *SystemConfig) GetIndex(key roachpb.Key) (int, bool) {
	i := s.getIndexBound(key)
	if i == len(s.Values) || !key.Equal(s.Values[i].Key) {
		return 0, false
	}
	return i, true
}

// getIndexBound searches the kv list for 'key' and returns its index if found
// or the index it would be placed at if not found.
func (s *SystemConfig) getIndexBound(key roachpb.Key) int {
	return sort.Search(len(s.Values), func(i int) bool {
		return key.Compare(s.Values[i].Key) <= 0
	})
}

// GetLargestObjectID returns the largest object ID found in the config which is
// a system ID. The objects in the config are augmented with the provided pseudo
// IDs. If idChecker is nil, returns the largest ID in the config
// (again, augmented by the pseudo IDs).
func (s *SystemConfig) GetLargestObjectID(
	maxReservedDescID ObjectID, pseudoIDs []uint32,
) (ObjectID, error) {
	testingLock.Lock()
	hook := testingLargestIDHook
	testingLock.Unlock()
	if hook != nil {
		return hook(maxReservedDescID), nil
	}

	// Search for the descriptor table entries within the SystemConfig. lowIndex
	// (in s.Values) is the first and highIndex one past the last KV pair in the
	// descriptor table.
	lowBound := keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)
	lowIndex := s.getIndexBound(lowBound)
	highBound := keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID + 1)
	highIndex := s.getIndexBound(highBound)
	if lowIndex == highIndex {
		return 0, fmt.Errorf("descriptor table not found in system config of %d values", len(s.Values))
	}

	// Determine the largest pseudo table ID equal to or below maxID.
	maxPseudoID := ObjectID(0)
	for _, id := range pseudoIDs {
		objID := ObjectID(id)
		if objID > maxPseudoID && (maxReservedDescID == 0 || objID <= maxReservedDescID) {
			maxPseudoID = objID
		}
	}

	// No maximum specified; maximum ID is the last entry in the descriptor
	// table or the largest pseudo ID, whichever is larger.
	if maxReservedDescID == 0 {
		id, err := keys.SystemSQLCodec.DecodeDescMetadataID(s.Values[highIndex-1].Key)
		if err != nil {
			return 0, err
		}
		objID := ObjectID(id)
		if objID < maxPseudoID {
			objID = maxPseudoID
		}
		return objID, nil
	}

	// Maximum specified: need to search the descriptor table. Binary search
	// through all descriptor table values to find the first descriptor with ID
	// >= maxID and pick either it or maxPseudoID, whichever is larger.
	searchSlice := s.Values[lowIndex:highIndex]
	var err error
	maxIdx := sort.Search(len(searchSlice), func(i int) bool {
		if err != nil {
			return false
		}
		var id uint32
		id, err = keys.SystemSQLCodec.DecodeDescMetadataID(searchSlice[i].Key)
		return uint32(maxReservedDescID) < id
	})
	if err != nil {
		return 0, err
	}

	// If we found an index within the list, maxIdx might point to a descriptor
	// with exactly maxID.
	if maxIdx < len(searchSlice) {
		id, err := keys.SystemSQLCodec.DecodeDescMetadataID(searchSlice[maxIdx].Key)
		if err != nil {
			return 0, err
		}
		if id <= uint32(maxReservedDescID) {
			return ObjectID(id), nil
		}
	}

	if maxIdx == 0 {
		return 0, fmt.Errorf("no system descriptors present")
	}

	// Return ID of the immediately preceding descriptor.
	id, err := keys.SystemSQLCodec.DecodeDescMetadataID(searchSlice[maxIdx-1].Key)
	if err != nil {
		return 0, err
	}
	objID := ObjectID(id)
	if objID < maxPseudoID {
		objID = maxPseudoID
	}
	return objID, nil
}

// TestingGetSystemTenantZoneConfigForKey looks up the zone config the
// provided key. This is exposed to facilitate testing the underlying
// logic.
func TestingGetSystemTenantZoneConfigForKey(
	s *SystemConfig, key roachpb.RKey,
) (ObjectID, *zonepb.ZoneConfig, error) {
	return s.getZoneConfigForKey(keys.SystemSQLCodec, key)
}

// getZoneConfigForKey looks up the zone config for the object (table
// or database, specified by key.id). It is the caller's
// responsibility to ensure that the range does not need to be split.
func (s *SystemConfig) getZoneConfigForKey(
	codec keys.SQLCodec, key roachpb.RKey,
) (ObjectID, *zonepb.ZoneConfig, error) {
	id, suffix := DecodeKeyIntoZoneIDAndSuffix(codec, key)
	entry, err := s.getZoneEntry(codec, id)
	if err != nil {
		return 0, nil, err
	}
	if entry.zone != nil {
		if entry.placeholder != nil {
			if subzone, _ := entry.placeholder.GetSubzoneForKeySuffix(suffix); subzone != nil {
				if indexSubzone := entry.placeholder.GetSubzone(subzone.IndexID, ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(entry.zone)
				return id, &subzone.Config, nil
			}
		} else if subzone, _ := entry.zone.GetSubzoneForKeySuffix(suffix); subzone != nil {
			if indexSubzone := entry.zone.GetSubzone(subzone.IndexID, ""); indexSubzone != nil {
				subzone.Config.InheritFromParent(&indexSubzone.Config)
			}
			subzone.Config.InheritFromParent(entry.zone)
			return id, &subzone.Config, nil
		}
		return id, entry.zone, nil
	}
	return id, s.DefaultZoneConfig, nil
}

// GetSpanConfigForKey looks of the span config for the given key. It's part of
// spanconfig.StoreReader interface. Note that it is only usable for the system
// tenant config.
func (s *SystemConfig) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	id, zone, err := s.getZoneConfigForKey(keys.SystemSQLCodec, key)
	if err != nil {
		return roachpb.SpanConfig{}, err
	}
	spanConfig := zone.AsSpanConfig()
	if id <= keys.MaxReservedDescID {
		// We enable rangefeeds for system tables; various internal subsystems
		// (leveraging system tables) rely on rangefeeds to function. We also do the
		// same for the tenant pseudo range ID for forwards compatibility with the
		// span configs infrastructure.
		spanConfig.RangefeedEnabled = true
		// We exclude system tables from strict GC enforcement, it's only really
		// applicable to user tables.
		spanConfig.GCPolicy.IgnoreStrictEnforcement = true
	}
	return spanConfig, nil
}

// DecodeKeyIntoZoneIDAndSuffix figures out the zone that the key belongs to.
func DecodeKeyIntoZoneIDAndSuffix(
	codec keys.SQLCodec, key roachpb.RKey,
) (id ObjectID, keySuffix []byte) {
	objectID, keySuffix, ok := DecodeObjectID(codec, key)
	if !ok {
		// Not in the structured data namespace.
		objectID = keys.RootNamespaceID
	} else if objectID <= keys.SystemDatabaseID || keys.IsPseudoTableID(uint32(objectID)) {
		// Pseudo-table ids should be considered to be a part of the
		// system database as they aren't real tables.
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
	} else if bytes.HasPrefix(key, keys.TenantPrefix) {
		objectID = keys.TenantsRangesID
	}
	return objectID, keySuffix
}

// GetZoneConfigForObject returns the combined zone config for the given object
// identifier and SQL codec.
//
// NOTE: any subzones from the zone placeholder will be automatically merged
// into the cached zone so the caller doesn't need special-case handling code.
func (s *SystemConfig) GetZoneConfigForObject(
	codec keys.SQLCodec, id ObjectID,
) (*zonepb.ZoneConfig, error) {
	var entry zoneEntry
	var err error
	entry, err = s.getZoneEntry(codec, id)
	if err != nil {
		return nil, err
	}
	return entry.combined, nil
}

// PurgeZoneConfigCache allocates a new zone config cache in this system config
// so that tables with stale zone config information could have this info
// looked up from using the most up-to-date zone config the next time it's
// requested. Note, this function is only intended to be called during test
// execution, such as logic tests.
func (s *SystemConfig) PurgeZoneConfigCache() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.zoneCache) != 0 {
		s.mu.zoneCache = map[ObjectID]zoneEntry{}
	}
	if len(s.mu.shouldSplitCache) != 0 {
		s.mu.shouldSplitCache = map[ObjectID]bool{}
	}
}

// getZoneEntry returns the zone entry for the given system-tenant
// object ID. In the fast path, the zone is already in the cache, and is
// directly returned. Otherwise, getZoneEntry will hydrate new
// zonepb.ZoneConfig(s) from the SystemConfig and install them as an
// entry in the cache.
func (s *SystemConfig) getZoneEntry(codec keys.SQLCodec, id ObjectID) (zoneEntry, error) {
	s.mu.RLock()
	entry, ok := s.mu.zoneCache[id]
	s.mu.RUnlock()
	if ok {
		return entry, nil
	}
	testingLock.Lock()
	hook := ZoneConfigHook
	testingLock.Unlock()
	zone, placeholder, cache, err := hook(s, codec, id)
	if err != nil {
		return zoneEntry{}, err
	}
	if zone != nil {
		entry := zoneEntry{zone: zone, placeholder: placeholder, combined: zone}
		if placeholder != nil {
			// Merge placeholder with zone by copying over subzone information.
			// Placeholders should only define the Subzones and SubzoneSpans fields.
			combined := *zone
			combined.Subzones = placeholder.Subzones
			combined.SubzoneSpans = placeholder.SubzoneSpans
			entry.combined = &combined
		}

		if cache {
			s.mu.Lock()
			s.mu.zoneCache[id] = entry
			s.mu.Unlock()
		}
		return entry, nil
	}
	return zoneEntry{}, nil
}

var staticSplits = []roachpb.RKey{
	roachpb.RKey(keys.NodeLivenessPrefix),           // end of meta records / start of node liveness span
	roachpb.RKey(keys.NodeLivenessKeyMax),           // end of node liveness span
	roachpb.RKey(keys.TimeseriesPrefix),             // start of timeseries span
	roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), // end of timeseries span
	roachpb.RKey(keys.TableDataMin),                 // end of system ranges / start of system config tables
}

// StaticSplits are predefined split points in the system keyspace.
// Corresponding ranges are created at cluster bootstrap time.
//
// There are two reasons for a static split. First, spans that are critical to
// cluster stability, like the node liveness span, are split into their own
// ranges to ease debugging (see #17297). Second, spans in the system keyspace
// that can be targeted by zone configs, like the meta span and the timeseries
// span, are split off into their own ranges because zone configs cannot apply
// to fractions of a range.
//
// Note that these are not the only splits created at cluster bootstrap; splits
// between various system tables are also created.
func StaticSplits() []roachpb.RKey {
	return staticSplits
}

// ShouldSplitAtDesc determines whether a specific descriptor should be
// considered for a split. Only plain tables are considered for split.
func ShouldSplitAtDesc(rawDesc *roachpb.Value) bool {
	var desc descpb.Descriptor
	if err := rawDesc.GetProto(&desc); err != nil {
		return false
	}
	switch t := desc.GetUnion().(type) {
	case *descpb.Descriptor_Table:
		if t.Table.IsView() && !t.Table.MaterializedView() {
			return false
		}
		return true
	case *descpb.Descriptor_Database:
		return false
	case *descpb.Descriptor_Type:
		return false
	case *descpb.Descriptor_Schema:
		return false
	default:
		panic(errors.AssertionFailedf("unexpected descriptor type %#v", &desc))
	}
}
