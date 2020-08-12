// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func TestPrettyPrint(t *testing.T) {
	tenSysCodec := keys.SystemSQLCodec
	ten5Codec := keys.MakeSQLCodec(roachpb.MakeTenantID(5))
	tm, _ := time.Parse(time.RFC3339Nano, "2016-03-30T13:40:35.053725008Z")
	duration := duration.MakeDuration(1*time.Second.Nanoseconds(), 1, 1)
	durationAsc, _ := encoding.EncodeDurationAscending(nil, duration)
	durationDesc, _ := encoding.EncodeDurationDescending(nil, duration)
	bitArray := bitarray.MakeBitArrayFromInt64(8, 58, 7)
	txnID := uuid.MakeV4()

	// Support for asserting that the ugly printer supports a key was added after
	// most of the tests here were written.
	revertSupportUnknown := false
	revertMustSupport := true

	// The following test cases encode keys with a mixture of ascending and descending direction,
	// but always decode keys in the ascending direction. This is why some of the decoded values
	// seem bizarre.
	testCases := []struct {
		key                   roachpb.Key
		exp                   string
		assertRevertSupported bool
	}{
		// local
		{keys.StoreIdentKey(), "/Local/Store/storeIdent", revertSupportUnknown},
		{keys.StoreGossipKey(), "/Local/Store/gossipBootstrap", revertSupportUnknown},
		{keys.StoreClusterVersionKey(), "/Local/Store/clusterVersion", revertSupportUnknown},
		{keys.StoreSuggestedCompactionKey(keys.MinKey, roachpb.Key("b")), `/Local/Store/suggestedCompaction/{/Min-"b"}`, revertSupportUnknown},
		{keys.StoreSuggestedCompactionKey(roachpb.Key("a"), roachpb.Key("b")), `/Local/Store/suggestedCompaction/{"a"-"b"}`, revertSupportUnknown},
		{keys.StoreSuggestedCompactionKey(roachpb.Key("a"), keys.MaxKey), `/Local/Store/suggestedCompaction/{"a"-/Max}`, revertSupportUnknown},

		{keys.AbortSpanKey(roachpb.RangeID(1000001), txnID), fmt.Sprintf(`/Local/RangeID/1000001/r/AbortSpan/%q`, txnID), revertSupportUnknown},
		{keys.RangeAppliedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeAppliedState", revertSupportUnknown},
		{keys.RaftAppliedIndexLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftAppliedIndex", revertSupportUnknown},
		{keys.LeaseAppliedIndexLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/LeaseAppliedIndex", revertSupportUnknown},
		{keys.RaftTruncatedStateLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftTruncatedState", revertSupportUnknown},
		{keys.RaftTruncatedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftTruncatedState", revertSupportUnknown},
		{keys.RangeLeaseKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeLease", revertSupportUnknown},
		{keys.RangeStatsLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeStats", revertSupportUnknown},
		{keys.RangeLastGCKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeLastGC", revertSupportUnknown},

		{keys.RaftHardStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftHardState", revertSupportUnknown},
		{keys.RangeTombstoneKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeTombstone", revertSupportUnknown},
		{keys.RaftLogKey(roachpb.RangeID(1000001), uint64(200001)), "/Local/RangeID/1000001/u/RaftLog/logIndex:200001", revertSupportUnknown},
		{keys.RangeLastReplicaGCTimestampKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeLastReplicaGCTimestamp", revertSupportUnknown},

		{keys.MakeRangeKeyPrefix(roachpb.RKey(tenSysCodec.TablePrefix(42))), `/Local/Range/Table/42`, revertSupportUnknown},
		{keys.RangeDescriptorKey(roachpb.RKey(tenSysCodec.TablePrefix(42))), `/Local/Range/Table/42/RangeDescriptor`, revertSupportUnknown},
		{keys.TransactionKey(tenSysCodec.TablePrefix(42), txnID), fmt.Sprintf(`/Local/Range/Table/42/Transaction/%q`, txnID), revertSupportUnknown},
		{keys.QueueLastProcessedKey(roachpb.RKey(tenSysCodec.TablePrefix(42)), "foo"), `/Local/Range/Table/42/QueueLastProcessed/"foo"`, revertSupportUnknown},

		{keys.MakeRangeKeyPrefix(roachpb.RKey(ten5Codec.TenantPrefix())), `/Local/Range/Tenant/5`, revertSupportUnknown},
		{keys.MakeRangeKeyPrefix(roachpb.RKey(ten5Codec.TablePrefix(42))), `/Local/Range/Tenant/5/Table/42`, revertSupportUnknown},
		{keys.RangeDescriptorKey(roachpb.RKey(ten5Codec.TablePrefix(42))), `/Local/Range/Tenant/5/Table/42/RangeDescriptor`, revertSupportUnknown},
		{keys.TransactionKey(ten5Codec.TablePrefix(42), txnID), fmt.Sprintf(`/Local/Range/Tenant/5/Table/42/Transaction/%q`, txnID), revertSupportUnknown},
		{keys.QueueLastProcessedKey(roachpb.RKey(ten5Codec.TablePrefix(42)), "foo"), `/Local/Range/Tenant/5/Table/42/QueueLastProcessed/"foo"`, revertSupportUnknown},

		{keys.LocalMax, `/Meta1/""`, revertSupportUnknown}, // LocalMax == Meta1Prefix

		// system
		{makeKey(keys.Meta2Prefix, roachpb.Key("foo")), `/Meta2/"foo"`, revertSupportUnknown},
		{makeKey(keys.Meta1Prefix, roachpb.Key("foo")), `/Meta1/"foo"`, revertSupportUnknown},
		{keys.RangeMetaKey(roachpb.RKey("f")).AsRawKey(), `/Meta2/"f"`, revertSupportUnknown},

		{keys.NodeLivenessKey(10033), "/System/NodeLiveness/10033", revertSupportUnknown},
		{keys.NodeStatusKey(1111), "/System/StatusNode/1111", revertSupportUnknown},

		{keys.SystemMax, "/System/Max", revertSupportUnknown},

		// key of key
		{keys.RangeMetaKey(roachpb.RKey(keys.MakeRangeKeyPrefix(roachpb.RKey(tenSysCodec.TablePrefix(42))))).AsRawKey(), `/Meta2/Local/Range/Table/42`, revertSupportUnknown},
		{keys.RangeMetaKey(roachpb.RKey(makeKey(tenSysCodec.TablePrefix(42), roachpb.RKey("foo")))).AsRawKey(), `/Meta2/Table/42/"foo"`, revertSupportUnknown},
		{keys.RangeMetaKey(roachpb.RKey(makeKey(keys.Meta2Prefix, roachpb.Key("foo")))).AsRawKey(), `/Meta1/"foo"`, revertSupportUnknown},

		// table
		{keys.SystemConfigSpan.Key, "/Table/SystemConfigSpan/Start", revertSupportUnknown},
		{keys.UserTableDataMin, "/Table/50", revertMustSupport},
		{tenSysCodec.TablePrefix(111), "/Table/111", revertMustSupport},
		{makeKey(tenSysCodec.TablePrefix(42), encoding.EncodeUvarintAscending(nil, 1)), `/Table/42/1`, revertMustSupport},
		{makeKey(tenSysCodec.TablePrefix(42), roachpb.RKey("foo")), `/Table/42/"foo"`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, float64(233.221112)))),
			"/Table/42/233.221112", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatDescending(nil, float64(-233.221112)))),
			"/Table/42/233.221112", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.Inf(1)))),
			"/Table/42/+Inf", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.NaN()))),
			"/Table/42/NaN", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222)),
			roachpb.RKey(encoding.EncodeStringAscending(nil, "handsome man"))),
			`/Table/42/1222/"handsome man"`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222))),
			`/Table/42/1222`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintDescending(nil, 1222))),
			`/Table/42/-1223`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255}))),
			`/Table/42/"\x01\x02\b\xff"`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Table/42/"\x01\x02\b\xff"/"bar"`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesDescending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Table/42/"\x01\x02\b\xff"/"bar"`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeNullAscending(nil))), "/Table/42/NULL", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeNullDescending(nil))), "/Table/42/NULL", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullAscending(nil))), "/Table/42/!NULL", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullDescending(nil))), "/Table/42/#", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeAscending(nil, tm))),
			"/Table/42/2016-03-30T13:40:35.053725008Z", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeDescending(nil, tm))),
			"/Table/42/1923-10-04T10:19:23.946274991Z", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalAscending(nil, apd.New(1234, -2)))),
			"/Table/42/12.34", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalDescending(nil, apd.New(1234, -2)))),
			"/Table/42/-12.34", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayAscending(nil, bitArray))),
			"/Table/42/B00111010", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayDescending(nil, bitArray))),
			"/Table/42/B00111010", revertSupportUnknown},
		// Regression test for #31115.
		{roachpb.Key(makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayAscending(nil, bitarray.MakeZeroBitArray(64))),
		)).PrefixEnd(),
			"/Table/42/B0000000000000000000000000000000000000000000000000000000000000000/PrefixEnd", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(durationAsc)),
			"/Table/42/1 mon 1 day 00:00:01", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42),
			roachpb.RKey(durationDesc)),
			"/Table/42/-2 mons -2 days +743:59:58.999999+999ns", revertSupportUnknown},
		// sequence
		{tenSysCodec.SequenceKey(55), `/Table/55/1/0/0`, revertSupportUnknown},

		// tenant table
		{ten5Codec.TenantPrefix(), "/Tenant/5", revertMustSupport},
		{ten5Codec.TablePrefix(0), "/Tenant/5/Table/SystemConfigSpan/Start", revertSupportUnknown},
		{ten5Codec.TablePrefix(keys.MinUserDescID), "/Tenant/5/Table/50", revertMustSupport},
		{ten5Codec.TablePrefix(111), "/Tenant/5/Table/111", revertMustSupport},
		{makeKey(ten5Codec.TablePrefix(42), encoding.EncodeUvarintAscending(nil, 1)), `/Tenant/5/Table/42/1`, revertMustSupport},
		{makeKey(ten5Codec.TablePrefix(42), roachpb.RKey("foo")), `/Tenant/5/Table/42/"foo"`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, float64(233.221112)))),
			"/Tenant/5/Table/42/233.221112", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatDescending(nil, float64(-233.221112)))),
			"/Tenant/5/Table/42/233.221112", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.Inf(1)))),
			"/Tenant/5/Table/42/+Inf", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.NaN()))),
			"/Tenant/5/Table/42/NaN", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222)),
			roachpb.RKey(encoding.EncodeStringAscending(nil, "handsome man"))),
			`/Tenant/5/Table/42/1222/"handsome man"`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222))),
			`/Tenant/5/Table/42/1222`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintDescending(nil, 1222))),
			`/Tenant/5/Table/42/-1223`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255}))),
			`/Tenant/5/Table/42/"\x01\x02\b\xff"`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Tenant/5/Table/42/"\x01\x02\b\xff"/"bar"`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesDescending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Tenant/5/Table/42/"\x01\x02\b\xff"/"bar"`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeNullAscending(nil))), "/Tenant/5/Table/42/NULL", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeNullDescending(nil))), "/Tenant/5/Table/42/NULL", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullAscending(nil))), "/Tenant/5/Table/42/!NULL", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullDescending(nil))), "/Tenant/5/Table/42/#", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeAscending(nil, tm))),
			"/Tenant/5/Table/42/2016-03-30T13:40:35.053725008Z", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeDescending(nil, tm))),
			"/Tenant/5/Table/42/1923-10-04T10:19:23.946274991Z", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalAscending(nil, apd.New(1234, -2)))),
			"/Tenant/5/Table/42/12.34", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalDescending(nil, apd.New(1234, -2)))),
			"/Tenant/5/Table/42/-12.34", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayAscending(nil, bitArray))),
			"/Tenant/5/Table/42/B00111010", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayDescending(nil, bitArray))),
			"/Tenant/5/Table/42/B00111010", revertSupportUnknown},
		// Regression test for #31115.
		{roachpb.Key(makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayAscending(nil, bitarray.MakeZeroBitArray(64))),
		)).PrefixEnd(),
			"/Tenant/5/Table/42/B0000000000000000000000000000000000000000000000000000000000000000/PrefixEnd", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(durationAsc)),
			"/Tenant/5/Table/42/1 mon 1 day 00:00:01", revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42),
			roachpb.RKey(durationDesc)),
			"/Tenant/5/Table/42/-2 mons -2 days +743:59:58.999999+999ns", revertSupportUnknown},
		// sequence
		{ten5Codec.SequenceKey(55), `/Tenant/5/Table/55/1/0/0`, revertSupportUnknown},

		// others
		{makeKey([]byte("")), "/Min", revertSupportUnknown},
		{keys.Meta1KeyMax, "/Meta1/Max", revertSupportUnknown},
		{keys.Meta2KeyMax, "/Meta2/Max", revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42), roachpb.RKey([]byte{0xf6})), `/Table/42/109/PrefixEnd`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42), roachpb.RKey([]byte{0xf7})), `/Table/42/255/PrefixEnd`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x02})), `/Table/42/"a"/PrefixEnd`, revertSupportUnknown},
		{makeKey(tenSysCodec.TablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x03})), `/Table/42/???`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42), roachpb.RKey([]byte{0xf6})), `/Tenant/5/Table/42/109/PrefixEnd`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42), roachpb.RKey([]byte{0xf7})), `/Tenant/5/Table/42/255/PrefixEnd`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x02})), `/Tenant/5/Table/42/"a"/PrefixEnd`, revertSupportUnknown},
		{makeKey(ten5Codec.TablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x03})), `/Tenant/5/Table/42/???`, revertSupportUnknown},
	}
	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			keyInfo := keys.MassagePrettyPrintedSpanForTest(keys.PrettyPrint(nil, /* valDirs */
				test.key), nil)
			exp := keys.MassagePrettyPrintedSpanForTest(test.exp, nil)
			t.Logf(`---- test case #%d:
input:  %q
output: %s
exp:    %s
`, i+1, []byte(test.key), keyInfo, exp)
			if exp != keyInfo {
				t.Errorf("%d: expected %s, got %s", i, exp, keyInfo)
			}

			if exp != keys.MassagePrettyPrintedSpanForTest(test.key.String(), nil) {
				t.Errorf("%d: from string expected %s, got %s", i, exp, test.key.String())
			}

			scanner := keysutil.MakePrettyScanner(nil /* tableParser */)
			parsed, err := scanner.Scan(keyInfo)
			if err != nil {
				if !errors.HasType(err, (*keys.ErrUglifyUnsupported)(nil)) {
					t.Errorf("%d: %s: %s", i, keyInfo, err)
				} else if !test.assertRevertSupported {
					t.Logf("%d: skipping parsing of %s; key is unsupported: %v", i, keyInfo, err)
				} else {
					t.Errorf("%d: ugly print expected unexpectedly unsupported (%s)", i, test.exp)
				}
			} else if exp, act := test.key, parsed; !bytes.Equal(exp, act) {
				t.Errorf("%d: ugly print expected %q, got %q", i, exp, act)
			}
			if t.Failed() {
				return
			}
		})
	}
}

func TestPrettyPrintRange(t *testing.T) {
	tenSysCodec := keys.SystemSQLCodec
	ten5Codec := keys.MakeSQLCodec(roachpb.MakeTenantID(5))
	key := makeKey([]byte("a"))
	key2 := makeKey([]byte("z"))
	tableKey := makeKey(tenSysCodec.TablePrefix(61), encoding.EncodeVarintAscending(nil, 4))
	tableKey2 := makeKey(tenSysCodec.TablePrefix(61), encoding.EncodeVarintAscending(nil, 500))
	tenTableKey := makeKey(ten5Codec.TablePrefix(61), encoding.EncodeVarintAscending(nil, 999))

	testCases := []struct {
		start, end roachpb.Key
		maxChars   int
		expected   string
	}{
		{key, nil, 20, "a"},
		{tableKey, nil, 10, "/Table/61…"},
		{tenTableKey, nil, 20, "/Tenant/5/Table/61/…"},
		{key, key2, 20, "{a-z}"},
		{keys.MinKey, tableKey, 8, "/{M…-T…}"},
		{keys.MinKey, tableKey, 15, "/{Min-Tabl…}"},
		{keys.MinKey, tableKey, 20, "/{Min-Table/6…}"},
		{keys.MinKey, tableKey, 25, "/{Min-Table/61/4}"},
		{keys.MinKey, tenTableKey, 8, "/{M…-T…}"},
		{keys.MinKey, tenTableKey, 15, "/{Min-Tena…}"},
		{keys.MinKey, tenTableKey, 20, "/{Min-Tenant/…}"},
		{keys.MinKey, tenTableKey, 25, "/{Min-Tenant/5/…}"},
		{keys.MinKey, tenTableKey, 30, "/{Min-Tenant/5/Tab…}"},
		{tableKey, tableKey2, 8, "/Table/…"},
		{tableKey, tableKey2, 15, "/Table/61/…"},
		{tableKey, tableKey2, 20, "/Table/61/{4-500}"},
		{tableKey, keys.MaxKey, 10, "/{Ta…-Max}"},
		{tableKey, keys.MaxKey, 20, "/{Table/6…-Max}"},
		{tableKey, keys.MaxKey, 25, "/{Table/61/4-Max}"},
		{tenTableKey, keys.MaxKey, 10, "/{Te…-Max}"},
		{tenTableKey, keys.MaxKey, 20, "/{Tenant/…-Max}"},
		{tenTableKey, keys.MaxKey, 25, "/{Tenant/5/…-Max}"},
		{tenTableKey, keys.MaxKey, 30, "/{Tenant/5/Tab…-Max}"},
	}

	for i, tc := range testCases {
		str := keys.PrettyPrintRange(tc.start, tc.end, tc.maxChars)
		if str != tc.expected {
			t.Errorf("%d: expected \"%s\", got \"%s\"", i, tc.expected, str)
		}
	}
}

func TestFormatHexKey(t *testing.T) {
	// Verify that we properly handling the 'x' formatting verb in
	// roachpb.Key.Format.
	key := keys.StoreIdentKey()
	decoded, err := hex.DecodeString(fmt.Sprintf("%x", key))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key, decoded) {
		t.Fatalf("expected %s, but found %s", key, decoded)
	}
}

func makeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}
