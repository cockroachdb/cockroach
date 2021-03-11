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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SystemTenantObjectID is an identifier for an object (e.g. database or table)
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
type SystemTenantObjectID uint32

type zoneConfigHook func(
	sysCfg *SystemConfig, objectID SystemTenantObjectID,
) (zone *zonepb.ZoneConfig, placeholder *zonepb.ZoneConfig, cache bool, err error)

var (
	// ZoneConfigHook is a function used to lookup a zone config given a system
	// tenant table or database ID.
	// This is also used by testing to simplify fake configs.
	ZoneConfigHook zoneConfigHook

	// testingLargestIDHook is a function used to bypass GetLargestObjectID
	// in tests.
	testingLargestIDHook func(SystemTenantObjectID) SystemTenantObjectID
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
type SystemConfig struct {
	SystemConfigEntries
	DefaultZoneConfig *zonepb.ZoneConfig
	mu                struct {
		syncutil.RWMutex
		zoneCache        map[SystemTenantObjectID]zoneEntry
		shouldSplitCache map[SystemTenantObjectID]bool
	}
}

// NewSystemConfig returns an initialized instance of SystemConfig.
func NewSystemConfig(defaultZoneConfig *zonepb.ZoneConfig) *SystemConfig {
	sc := &SystemConfig{}
	sc.DefaultZoneConfig = defaultZoneConfig
	sc.mu.zoneCache = map[SystemTenantObjectID]zoneEntry{}
	sc.mu.shouldSplitCache = map[SystemTenantObjectID]bool{}
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
	_, ok := testingZoneConfig[SystemTenantObjectID(id)]
	testingLock.Unlock()

	if ok {
		// A test installed a zone config for this ID, but no descriptor.
		// Synthesize an empty descriptor to force split to occur, or else the
		// zone config won't apply to any ranges. Most tests that use
		// TestingSetZoneConfig are too low-level to create tables and zone
		// configs through proper channels.
		//
		// Getting here outside tests is impossible.
		desc := tabledesc.NewBuilder(&descpb.TableDescriptor{}).BuildImmutable().DescriptorProto()
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
// less than or equal to maxID. The objects in the config are augmented with the
// provided pseudo IDs. If maxID is 0, returns the largest ID in the config
// (again, augmented by the pseudo IDs).
func (s *SystemConfig) GetLargestObjectID(
	maxID SystemTenantObjectID, pseudoIDs []uint32,
) (SystemTenantObjectID, error) {
	testingLock.Lock()
	hook := testingLargestIDHook
	testingLock.Unlock()
	if hook != nil {
		return hook(maxID), nil
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
	maxPseudoID := SystemTenantObjectID(0)
	for _, id := range pseudoIDs {
		objID := SystemTenantObjectID(id)
		if objID > maxPseudoID && (maxID == 0 || objID <= maxID) {
			maxPseudoID = objID
		}
	}

	// No maximum specified; maximum ID is the last entry in the descriptor
	// table or the largest pseudo ID, whichever is larger.
	if maxID == 0 {
		id, err := keys.SystemSQLCodec.DecodeDescMetadataID(s.Values[highIndex-1].Key)
		if err != nil {
			return 0, err
		}
		objID := SystemTenantObjectID(id)
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
		return SystemTenantObjectID(id) >= maxID
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
		if SystemTenantObjectID(id) == maxID {
			return SystemTenantObjectID(id), nil
		}
	}

	if maxIdx == 0 {
		return 0, fmt.Errorf("no descriptors present with ID < %d", maxID)
	}

	// Return ID of the immediately preceding descriptor.
	id, err := keys.SystemSQLCodec.DecodeDescMetadataID(searchSlice[maxIdx-1].Key)
	if err != nil {
		return 0, err
	}
	objID := SystemTenantObjectID(id)
	if objID < maxPseudoID {
		objID = maxPseudoID
	}
	return objID, nil
}

// GetZoneConfigForKey looks up the zone config for the object (table
// or database, specified by key.id). It is the caller's
// responsibility to ensure that the range does not need to be split.
func (s *SystemConfig) GetZoneConfigForKey(key roachpb.RKey) (*zonepb.ZoneConfig, error) {
	return s.getZoneConfigForKey(DecodeKeyIntoZoneIDAndSuffix(key))
}

// DecodeKeyIntoZoneIDAndSuffix figures out the zone that the key belongs to.
func DecodeKeyIntoZoneIDAndSuffix(key roachpb.RKey) (id SystemTenantObjectID, keySuffix []byte) {
	objectID, keySuffix, ok := DecodeSystemTenantObjectID(key)
	if !ok {
		// Not in the structured data namespace.
		objectID = keys.RootNamespaceID
	} else if objectID <= keys.MaxSystemConfigDescID || isPseudoTableID(uint32(objectID)) {
		// For now, you cannot set the zone config on gossiped tables. The only
		// way to set a zone config on these tables is to modify config for the
		// system database as a whole. This is largely because all the
		// "system config" tables are colocated in the same range by default and
		// thus couldn't be managed separately.
		// Furthermore pseudo-table ids should be considered to be a part of the
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

// isPseudoTableID returns true if id is in keys.PseudoTableIDs.
func isPseudoTableID(id uint32) bool {
	for _, pseudoTableID := range keys.PseudoTableIDs {
		if id == pseudoTableID {
			return true
		}
	}
	return false
}

// GetZoneConfigForObject returns the combined zone config for the given object
// identifier and SQL codec.
// NOTE: any subzones from the zone placeholder will be automatically merged
// into the cached zone so the caller doesn't need special-case handling code.
func (s *SystemConfig) GetZoneConfigForObject(
	codec keys.SQLCodec, id uint32,
) (*zonepb.ZoneConfig, error) {
	var sysID SystemTenantObjectID
	if codec.ForSystemTenant() {
		sysID = SystemTenantObjectID(id)
	} else {
		sysID = keys.TenantsRangesID
	}
	entry, err := s.getZoneEntry(sysID)
	if err != nil {
		return nil, err
	}
	return entry.combined, nil
}

// getZoneEntry returns the zone entry for the given system-tenant
// object ID. In the fast path, the zone is already in the cache, and is
// directly returned. Otherwise, getZoneEntry will hydrate new
// zonepb.ZoneConfig(s) from the SystemConfig and install them as an
// entry in the cache.
func (s *SystemConfig) getZoneEntry(id SystemTenantObjectID) (zoneEntry, error) {
	s.mu.RLock()
	entry, ok := s.mu.zoneCache[id]
	s.mu.RUnlock()
	if ok {
		return entry, nil
	}
	testingLock.Lock()
	hook := ZoneConfigHook
	testingLock.Unlock()
	zone, placeholder, cache, err := hook(s, id)
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

func (s *SystemConfig) getZoneConfigForKey(
	id SystemTenantObjectID, keySuffix []byte,
) (*zonepb.ZoneConfig, error) {
	entry, err := s.getZoneEntry(id)
	if err != nil {
		return nil, err
	}
	if entry.zone != nil {
		if entry.placeholder != nil {
			if subzone, _ := entry.placeholder.GetSubzoneForKeySuffix(keySuffix); subzone != nil {
				if indexSubzone := entry.placeholder.GetSubzone(subzone.IndexID, ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(entry.zone)
				return &subzone.Config, nil
			}
		} else if subzone, _ := entry.zone.GetSubzoneForKeySuffix(keySuffix); subzone != nil {
			if indexSubzone := entry.zone.GetSubzone(subzone.IndexID, ""); indexSubzone != nil {
				subzone.Config.InheritFromParent(&indexSubzone.Config)
			}
			subzone.Config.InheritFromParent(entry.zone)
			return &subzone.Config, nil
		}
		return entry.zone, nil
	}
	return s.DefaultZoneConfig, nil
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

// ComputeSplitKey takes a start and end key and returns the first key at which
// to split the span [start, end). Returns nil if no splits are required.
//
// Splits are required between user tables (i.e. /table/<id>) in the system
// tenant, at the start of the system-config tables (i.e. /table/0), and at
// certain points within the system ranges that come before the system tables.
// The system-config range is somewhat special in that it can contain multiple
// SQL tables (/table/0-/table/<max-system-config-desc>) within a single range.
//
// Splits are also required between secondary tenants (i.e. /tenant/<id>).
// However, splits are not required between the tables of secondary tenants.
func (s *SystemConfig) ComputeSplitKey(
	ctx context.Context, startKey, endKey roachpb.RKey,
) (rr roachpb.RKey) {
	// Before dealing with splits necessitated by SQL tables, handle all of the
	// static splits earlier in the keyspace. Note that this list must be kept in
	// the proper order (ascending in the keyspace) for the logic below to work.
	//
	// For new clusters, the static splits correspond to ranges created at
	// bootstrap time. Older stores might be used with a version with more
	// staticSplits though, in which case this code is useful.
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

	// If the above iteration over the static split points didn't decide
	// anything, the key range must be somewhere in the SQL table part of the
	// keyspace. First, look for split keys within the system-tenant's keyspace.
	if split := s.systemTenantTableBoundarySplitKey(ctx, startKey, endKey); split != nil {
		return split
	}

	// If the system tenant does not have any splits, look for split keys at the
	// boundary of each secondary tenant.
	return s.tenantBoundarySplitKey(ctx, startKey, endKey)
}

func (s *SystemConfig) systemTenantTableBoundarySplitKey(
	ctx context.Context, startKey, endKey roachpb.RKey,
) roachpb.RKey {
	if bytes.HasPrefix(startKey, keys.TenantPrefix) {
		// If the start key has a tenant prefix, don't try to find a split key
		// between the system tenant's tables. The rest of this method will
		// still work even without this short-circuiting, but we might as well
		// avoid it.
		return nil
	}

	startID, _, ok := DecodeSystemTenantObjectID(startKey)
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

	// findSplitKey returns the first possible split key between the given
	// range of IDs.
	findSplitKey := func(startID, endID SystemTenantObjectID) roachpb.RKey {
		// endID could be smaller than startID if we don't have user tables.
		for id := startID; id <= endID; id++ {
			tableKey := roachpb.RKey(keys.SystemSQLCodec.TablePrefix(uint32(id)))
			// This logic is analogous to the well-commented static split logic above.
			if startKey.Less(tableKey) && s.shouldSplitOnSystemTenantObject(id) {
				if tableKey.Less(endKey) {
					return tableKey
				}
				return nil
			}

			zoneVal := s.GetValue(MakeZoneKey(id))
			if zoneVal == nil {
				continue
			}
			var zone zonepb.ZoneConfig
			if err := zoneVal.GetProto(&zone); err != nil {
				// An error while decoding the zone proto is unfortunate, but logging a
				// message here would be excessively spammy. Just move on, which
				// effectively assumes there are no subzones for this table.
				continue
			}
			// This logic is analogous to the well-commented static split logic above.
			for _, s := range zone.SubzoneSplits() {
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
		endID, err := s.GetLargestObjectID(keys.MaxReservedDescID, keys.PseudoTableIDs)
		if err != nil {
			log.Errorf(ctx, "unable to determine largest reserved object ID from system config: %s", err)
			return nil
		}
		if splitKey := findSplitKey(startID, endID); splitKey != nil {
			return splitKey
		}
		startID = keys.MaxReservedDescID + 1
	}

	// Find the split key in the system tenant's user space.
	endID, err := s.GetLargestObjectID(0, keys.PseudoTableIDs)
	if err != nil {
		log.Errorf(ctx, "unable to determine largest object ID from system config: %s", err)
		return nil
	}
	return findSplitKey(startID, endID)
}

func (s *SystemConfig) tenantBoundarySplitKey(
	ctx context.Context, startKey, endKey roachpb.RKey,
) roachpb.RKey {
	// Bail early if there's no overlap with the secondary tenant keyspace.
	searchSpan := roachpb.Span{Key: startKey.AsRawKey(), EndKey: endKey.AsRawKey()}
	tenantSpan := roachpb.Span{Key: keys.TenantTableDataMin, EndKey: keys.TenantTableDataMax}
	if !searchSpan.Overlaps(tenantSpan) {
		return nil
	}

	// Determine tenant ID range being searched: [lowTenID, highTenID].
	var lowTenID, highTenID roachpb.TenantID
	if searchSpan.Key.Compare(tenantSpan.Key) < 0 {
		// startKey before tenant keyspace.
		lowTenID = roachpb.MinTenantID
	} else {
		// MakeTenantPrefix(lowTenIDExcl) is either the start key of this key
		// range or is outside of this key range. We're only searching for split
		// points within the specified key range, so the first tenant ID that we
		// would consider splitting on is the following ID.
		_, lowTenIDExcl, err := keys.DecodeTenantPrefix(searchSpan.Key)
		if err != nil {
			log.Errorf(ctx, "unable to decode tenant ID from start key: %s", err)
			return nil
		}
		if lowTenIDExcl == roachpb.MaxTenantID {
			// MaxTenantID already split or outside range.
			return nil
		}
		lowTenID = roachpb.MakeTenantID(lowTenIDExcl.ToUint64() + 1)
	}
	if searchSpan.EndKey.Compare(tenantSpan.EndKey) >= 0 {
		// endKey after tenant keyspace.
		highTenID = roachpb.MaxTenantID
	} else {
		rem, highTenIDExcl, err := keys.DecodeTenantPrefix(searchSpan.EndKey)
		if err != nil {
			log.Errorf(ctx, "unable to decode tenant ID from end key: %s", err)
			return nil
		}
		if len(rem) == 0 {
			// MakeTenantPrefix(highTenIDExcl) is the end key of this key range.
			// The key range is exclusive but we're looking for an inclusive
			// range of tenant IDs, so the last tenant ID that we would consider
			// splitting on is the previous ID.
			//
			// Unlike with searchSpan.Key and MaxTenantID, there is no exception
			// for DecodeTenantPrefix(searchSpan.EndKey) == MinTenantID. This is
			// because tenantSpan.Key is set to MakeTenantPrefix(MinTenantID),
			// so we would have already returned early in that case.
			highTenID = roachpb.MakeTenantID(highTenIDExcl.ToUint64() - 1)
		} else {
			highTenID = highTenIDExcl
		}
	}

	// Bail if there is no chance of any tenant boundaries between these IDs.
	if lowTenID.ToUint64() > highTenID.ToUint64() {
		return nil
	}

	// Search for the tenants table entries in the SystemConfig within the
	// desired tenant ID range.
	lowBound := keys.SystemSQLCodec.TenantMetadataKey(lowTenID)
	lowIndex := s.getIndexBound(lowBound)
	if lowIndex == len(s.Values) {
		// No keys within range found.
		return nil
	}

	// Choose the first key in this range. Extract its tenant ID and check
	// whether its within the desired tenant ID range.
	splitKey := s.Values[lowIndex].Key
	splitTenID, err := keys.SystemSQLCodec.DecodeTenantMetadataID(splitKey)
	if err != nil {
		log.Errorf(ctx, "unable to decode tenant ID from system config: %s", err)
		return nil
	}
	if splitTenID.ToUint64() > highTenID.ToUint64() {
		// No keys within range found.
		return nil
	}
	return roachpb.RKey(keys.MakeTenantPrefix(splitTenID))
}

// NeedsSplit returns whether the range [startKey, endKey) needs a split due
// to zone configs.
func (s *SystemConfig) NeedsSplit(ctx context.Context, startKey, endKey roachpb.RKey) bool {
	return len(s.ComputeSplitKey(ctx, startKey, endKey)) > 0
}

// shouldSplitOnSystemTenantObject checks if the ID is eligible for a split at
// all. It uses the internal cache to find a value, and tries to find it using
// the hook if ID isn't found in the cache.
func (s *SystemConfig) shouldSplitOnSystemTenantObject(id SystemTenantObjectID) bool {
	// Check the cache.
	{
		s.mu.RLock()
		shouldSplit, ok := s.mu.shouldSplitCache[id]
		s.mu.RUnlock()
		if ok {
			return shouldSplit
		}
	}

	var shouldSplit bool
	if id < keys.MinUserDescID {
		// The ID might be one of the reserved IDs that refer to ranges but not any
		// actual descriptors.
		shouldSplit = true
	} else {
		desc := s.getSystemTenantDesc(keys.SystemSQLCodec.DescMetadataKey(uint32(id)))
		shouldSplit = desc != nil && systemschema.ShouldSplitAtDesc(desc)
	}
	// Populate the cache.
	s.mu.Lock()
	s.mu.shouldSplitCache[id] = shouldSplit
	s.mu.Unlock()
	return shouldSplit
}
