// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config_test

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TODO(benesch): Don't reinvent the key encoding here.

func plainKV(k, v string) roachpb.KeyValue {
	return kv([]byte(k), []byte(v))
}

func tkey(tableID uint32, chunks ...string) []byte {
	key := keys.SystemSQLCodec.TablePrefix(tableID)
	for _, c := range chunks {
		key = append(key, []byte(c)...)
	}
	return key
}

func tenantPrefix(tenID uint64) []byte {
	return keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenID))
}

func tenantTkey(tenID uint64, tableID uint32, chunks ...string) []byte {
	key := keys.MakeSQLCodec(roachpb.MustMakeTenantID(tenID)).TablePrefix(tableID)
	for _, c := range chunks {
		key = append(key, []byte(c)...)
	}
	return key
}

func sqlKV(tableID uint32, indexID, descID uint64) roachpb.KeyValue {
	k := tkey(tableID)
	k = encoding.EncodeUvarintAscending(k, indexID)
	k = encoding.EncodeUvarintAscending(k, descID)
	k = encoding.EncodeUvarintAscending(k, 12345) // Column ID, but could be anything.
	return kv(k, nil)
}

func descriptor(descID uint32) roachpb.KeyValue {
	id := descpb.ID(descID)
	k := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, id)
	v := &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{ID: id}}}
	kv := roachpb.KeyValue{Key: k}
	if err := kv.Value.SetProto(v); err != nil {
		panic(err)
	}
	return kv
}

func tenant(tenID uint64) roachpb.KeyValue {
	k := keys.SystemSQLCodec.TenantMetadataKey(roachpb.MustMakeTenantID(tenID))
	return kv(k, nil)
}

func zoneConfig(descID descpb.ID, spans ...zonepb.SubzoneSpan) roachpb.KeyValue {
	kv := roachpb.KeyValue{
		Key: config.MakeZoneKey(keys.SystemSQLCodec, descID),
	}
	if err := kv.Value.SetProto(&zonepb.ZoneConfig{SubzoneSpans: spans}); err != nil {
		panic(err)
	}
	return kv
}

func subzone(start, end string) zonepb.SubzoneSpan {
	return zonepb.SubzoneSpan{Key: []byte(start), EndKey: []byte(end)}
}

func kv(k, v []byte) roachpb.KeyValue {
	return roachpb.KeyValue{
		Key:   k,
		Value: roachpb.MakeValueFromBytes(v),
	}
}

func TestGet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	emptyKeys := []roachpb.KeyValue{}
	someKeys := []roachpb.KeyValue{
		plainKV("a", "vala"),
		plainKV("c", "valc"),
		plainKV("d", "vald"),
	}

	aVal := roachpb.MakeValueFromString("vala")
	bVal := roachpb.MakeValueFromString("valc")
	cVal := roachpb.MakeValueFromString("vald")

	testCases := []struct {
		values []roachpb.KeyValue
		key    string
		value  *roachpb.Value
	}{
		{emptyKeys, "a", nil},
		{emptyKeys, "b", nil},
		{emptyKeys, "c", nil},
		{emptyKeys, "d", nil},
		{emptyKeys, "e", nil},

		{someKeys, "", nil},
		{someKeys, "b", nil},
		{someKeys, "e", nil},
		{someKeys, "a0", nil},

		{someKeys, "a", &aVal},
		{someKeys, "c", &bVal},
		{someKeys, "d", &cVal},
	}

	cfg := config.NewSystemConfig(zonepb.DefaultZoneConfigRef())
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		if val := cfg.GetValue([]byte(tc.key)); !reflect.DeepEqual(val, tc.value) {
			t.Errorf("#%d: expected=%s, found=%s", tcNum, tc.value, val)
		}
	}
}

func TestGetLargestID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		values    []roachpb.KeyValue
		largest   config.ObjectID
		maxID     config.ObjectID
		pseudoIDs []uint32
		errStr    string
	}

	testCases := []testCase{
		// No data.
		{nil, 0, 0, nil, "descriptor table not found"},

		// Some data, but not from the system span.
		{[]roachpb.KeyValue{plainKV("a", "b")}, 0, 0, nil, "descriptor table not found"},

		// Some real data, but no descriptors.
		{[]roachpb.KeyValue{
			sqlKV(keys.NamespaceTableID, 1, 1),
			sqlKV(keys.NamespaceTableID, 1, 2),
			sqlKV(keys.UsersTableID, 1, 3),
		}, 0, 0, nil, "descriptor table not found"},

		// Decoding error, unbounded max.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 1, math.MaxUint64),
		}, 0, 0, nil, "descriptor ID 18446744073709551615 exceeds uint32 bounds"},

		// Decoding error, bounded max.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 1, math.MaxUint64),
			sqlKV(keys.DescriptorTableID, 2, 1),
		}, 0, 5, nil, "descriptor ID 18446744073709551615 exceeds uint32 bounds"},

		// Single correct descriptor entry.
		{[]roachpb.KeyValue{sqlKV(keys.DescriptorTableID, 1, 1)}, 1, 0, nil, ""},

		// Surrounded by other data.
		{[]roachpb.KeyValue{
			sqlKV(keys.NamespaceTableID, 1, 20),
			sqlKV(keys.NamespaceTableID, 1, 30),
			sqlKV(keys.DescriptorTableID, 1, 8),
			sqlKV(keys.ZonesTableID, 1, 40),
		}, 8, 0, nil, ""},

		// Descriptors with holes. Index ID does not matter.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 2, 5),
			sqlKV(keys.DescriptorTableID, 3, 8),
			sqlKV(keys.DescriptorTableID, 4, 12),
		}, 12, 0, nil, ""},

		// Real SQL layout.
		func() testCase {
			ms := bootstrap.MakeMetadataSchema(keys.SystemSQLCodec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
			descIDs := ms.DescriptorIDs()
			maxDescID := config.ObjectID(descIDs[len(descIDs)-1])
			kvs, _ /* splits */ := ms.GetInitialValues()
			pseudoIDs := keys.PseudoTableIDs
			const pseudoIDIsMax = false // NOTE: change to false if adding new system not pseudo objects.
			if pseudoIDIsMax {
				maxDescID = config.ObjectID(keys.MaxPseudoTableID)
			}
			return testCase{kvs, maxDescID, 0, pseudoIDs, ""}
		}(),

		// Test non-zero max.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 2, 5),
			sqlKV(keys.DescriptorTableID, 3, 8),
			sqlKV(keys.DescriptorTableID, 4, 12),
		}, 8, 8, nil, ""},

		// Test non-zero max.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 2, 5),
			sqlKV(keys.DescriptorTableID, 3, 8),
			sqlKV(keys.DescriptorTableID, 4, 12),
		}, 5, 7, nil, ""},

		// Test pseudo ID (MetaRangesID = 16), exact.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 4, 12),
			sqlKV(keys.DescriptorTableID, 4, 19),
			sqlKV(keys.DescriptorTableID, 4, 22),
		}, 16, 16, []uint32{16, 17, 18}, ""},

		// Test pseudo ID (TimeseriesRangesID = 18), above.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 4, 12),
			sqlKV(keys.DescriptorTableID, 4, 21),
			sqlKV(keys.DescriptorTableID, 4, 22),
		}, 18, 20, []uint32{16, 17, 18}, ""},

		// Test pseudo ID (TimeseriesRangesID = 18), largest.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 4, 12),
		}, 18, 0, []uint32{16, 17, 18}, ""},
	}

	cfg := config.NewSystemConfig(zonepb.DefaultZoneConfigRef())
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		ret, err := cfg.GetLargestObjectID(tc.maxID, tc.pseudoIDs)
		if !testutils.IsError(err, tc.errStr) {
			t.Errorf("#%d: expected err=%q, got %v", tcNum, tc.errStr, err)
			continue
		}
		if err != nil {
			continue
		}
		if ret != tc.largest {
			t.Errorf("#%d: expected largest=%d, got %d", tcNum, tc.largest, ret)
		}
	}
}

func TestStaticSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	splits := config.StaticSplits()
	for i := 1; i < len(splits); i++ {
		if !splits[i-1].Less(splits[i]) {
			t.Errorf("previous split %q should be less than next split %q", splits[i-1], splits[i])
		}
	}
}

func TestGetZoneConfigForKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	testCases := []struct {
		key        roachpb.RKey
		expectedID config.ObjectID
	}{
		{roachpb.RKeyMin, keys.MetaRangesID},
		{roachpb.RKey(keys.Meta1Prefix), keys.MetaRangesID},
		{roachpb.RKey(keys.Meta1Prefix.Next()), keys.MetaRangesID},
		{roachpb.RKey(keys.Meta2Prefix), keys.MetaRangesID},
		{roachpb.RKey(keys.Meta2Prefix.Next()), keys.MetaRangesID},
		{roachpb.RKey(keys.MetaMax), keys.SystemRangesID},
		{roachpb.RKey(keys.SystemPrefix), keys.SystemRangesID},
		{roachpb.RKey(keys.SystemPrefix.Next()), keys.SystemRangesID},
		{roachpb.RKey(keys.NodeLivenessPrefix), keys.LivenessRangesID},
		{roachpb.RKey(keys.LegacyDescIDGenerator), keys.SystemRangesID},
		{roachpb.RKey(keys.NodeIDGenerator), keys.SystemRangesID},
		{roachpb.RKey(keys.RangeIDGenerator), keys.SystemRangesID},
		{roachpb.RKey(keys.StoreIDGenerator), keys.SystemRangesID},
		{roachpb.RKey(keys.StatusPrefix), keys.SystemRangesID},
		{roachpb.RKey(keys.TimeseriesPrefix), keys.TimeseriesRangesID},
		{roachpb.RKey(keys.TimeseriesPrefix.Next()), keys.TimeseriesRangesID},
		{roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), keys.SystemRangesID},
		{roachpb.RKey(keys.TableDataMin), keys.SystemDatabaseID},
		{roachpb.RKey(keys.SystemConfigSplitKey), keys.SystemDatabaseID},

		{tkey(keys.ZonesTableID), keys.ZonesTableID},
		{roachpb.RKey(keys.SystemZonesTableSpan.Key), keys.ZonesTableID},
		{tkey(keys.DescriptorTableID), keys.DescriptorTableID},
		{roachpb.RKey(keys.SystemDescriptorTableSpan.Key), keys.DescriptorTableID},

		// Non-gossiped system tables should refer to themselves.
		{tkey(keys.LeaseTableID), keys.LeaseTableID},
		{tkey(uint32(systemschema.JobsTable.GetID())), config.ObjectID(systemschema.JobsTable.GetID())},
		{tkey(keys.LocationsTableID), keys.LocationsTableID},
		{tkey(keys.NamespaceTableID), keys.NamespaceTableID},

		// Pseudo-tables should refer to the SystemDatabaseID.
		{tkey(keys.MetaRangesID), keys.SystemDatabaseID},
		{tkey(keys.SystemRangesID), keys.SystemDatabaseID},
		{tkey(keys.TimeseriesRangesID), keys.SystemDatabaseID},
		{tkey(keys.LivenessRangesID), keys.SystemDatabaseID},

		// User tables should refer to themselves.
		{tkey(bootstrap.TestingUserDescID(0)), config.ObjectID(bootstrap.TestingUserDescID(0))},
		{tkey(bootstrap.TestingUserDescID(22)), config.ObjectID(bootstrap.TestingUserDescID(22))},
		{roachpb.RKeyMax, keys.RootNamespaceID},

		// Secondary tenant tables should refer to the TenantsRangesID.
		{tenantTkey(5, bootstrap.TestingUserDescID(0)), keys.TenantsRangesID},
		{tenantTkey(5, bootstrap.TestingUserDescID(22)), keys.TenantsRangesID},
		{tenantTkey(10, bootstrap.TestingUserDescID(0)), keys.TenantsRangesID},
		{tenantTkey(10, bootstrap.TestingUserDescID(22)), keys.TenantsRangesID},
	}

	originalZoneConfigHook := config.ZoneConfigHook
	defer func() {
		config.ZoneConfigHook = originalZoneConfigHook
	}()
	cfg := config.NewSystemConfig(zonepb.DefaultZoneConfigRef())

	kvs, _ /* splits */ := bootstrap.MakeMetadataSchema(
		keys.SystemSQLCodec, cfg.DefaultZoneConfig, zonepb.DefaultSystemZoneConfigRef(),
	).GetInitialValues()
	cfg.SystemConfigEntries = config.SystemConfigEntries{
		Values: kvs,
	}
	for tcNum, tc := range testCases {
		var objectID config.ObjectID
		config.ZoneConfigHook = func(
			_ *config.SystemConfig, codec keys.SQLCodec, id config.ObjectID,
		) (*zonepb.ZoneConfig, *zonepb.ZoneConfig, bool, error) {
			objectID = id
			return cfg.DefaultZoneConfig, nil, false, nil
		}
		_, err := cfg.GetSpanConfigForKey(ctx, tc.key)
		if err != nil {
			t.Errorf("#%d: GetSpanConfigForKey(%v) got error: %v", tcNum, tc.key, err)
		}
		if objectID != tc.expectedID {
			t.Errorf("#%d: GetSpanConfigForKey(%v) got %d; want %d", tcNum, tc.key, objectID, tc.expectedID)
		}
	}
}

func TestSystemConfigMask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	entries := config.SystemConfigEntries{Values: []roachpb.KeyValue{
		plainKV("k1", "v1"),
		plainKV("k2", "v2"),
		plainKV("k3", "v3"),
		plainKV("k4", "v4"),
		plainKV("k5", "v5"),
		plainKV("k6", "v6"),
		plainKV("k7", "v7"),
	}}
	mask := config.MakeSystemConfigMask(
		[]byte("k1"),
		[]byte("k6"),
		[]byte("k3"),
	)

	exp := config.SystemConfigEntries{Values: []roachpb.KeyValue{
		plainKV("k1", "v1"),
		plainKV("k3", "v3"),
		plainKV("k6", "v6"),
	}}
	res := mask.Apply(entries)
	require.Equal(t, exp, res)
}
