// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestPrettyPrint(t *testing.T) {
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
		{StoreIdentKey(), "/Local/Store/storeIdent", revertSupportUnknown},
		{StoreGossipKey(), "/Local/Store/gossipBootstrap", revertSupportUnknown},
		{StoreClusterVersionKey(), "/Local/Store/clusterVersion", revertSupportUnknown},
		{StoreSuggestedCompactionKey(MinKey, roachpb.Key("b")), `/Local/Store/suggestedCompaction/{/Min-"b"}`, revertSupportUnknown},
		{StoreSuggestedCompactionKey(roachpb.Key("a"), roachpb.Key("b")), `/Local/Store/suggestedCompaction/{"a"-"b"}`, revertSupportUnknown},
		{StoreSuggestedCompactionKey(roachpb.Key("a"), MaxKey), `/Local/Store/suggestedCompaction/{"a"-/Max}`, revertSupportUnknown},

		{AbortSpanKey(roachpb.RangeID(1000001), txnID), fmt.Sprintf(`/Local/RangeID/1000001/r/AbortSpan/%q`, txnID), revertSupportUnknown},
		{RaftTombstoneIncorrectLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftTombstone", revertSupportUnknown},
		{RangeAppliedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeAppliedState", revertSupportUnknown},
		{RaftAppliedIndexLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftAppliedIndex", revertSupportUnknown},
		{LeaseAppliedIndexLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/LeaseAppliedIndex", revertSupportUnknown},
		{RaftTruncatedStateLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftTruncatedState", revertSupportUnknown},
		{RaftTruncatedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftTruncatedState", revertSupportUnknown},
		{RangeLeaseKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeLease", revertSupportUnknown},
		{RangeStatsLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeStats", revertSupportUnknown},
		{RangeTxnSpanGCThresholdKey(roachpb.RangeID(1000001)), `/Local/RangeID/1000001/r/RangeTxnSpanGCThreshold`, revertSupportUnknown},
		{RangeFrozenStatusKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeFrozenStatus", revertSupportUnknown},
		{RangeLastGCKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeLastGC", revertSupportUnknown},

		{RaftHardStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftHardState", revertSupportUnknown},
		{RaftLastIndexKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftLastIndex", revertSupportUnknown},
		{RaftTombstoneKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftTombstone", revertSupportUnknown},
		{RaftLogKey(roachpb.RangeID(1000001), uint64(200001)), "/Local/RangeID/1000001/u/RaftLog/logIndex:200001", revertSupportUnknown},
		{RangeLastReplicaGCTimestampKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeLastReplicaGCTimestamp", revertSupportUnknown},
		{RangeLastVerificationTimestampKeyDeprecated(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeLastVerificationTimestamp", revertSupportUnknown},

		{MakeRangeKeyPrefix(roachpb.RKey(MakeTablePrefix(42))), `/Local/Range/Table/42`, revertSupportUnknown},
		{RangeDescriptorKey(roachpb.RKey(MakeTablePrefix(42))), `/Local/Range/Table/42/RangeDescriptor`, revertSupportUnknown},
		{TransactionKey(roachpb.Key(MakeTablePrefix(42)), txnID), fmt.Sprintf(`/Local/Range/Table/42/Transaction/%q`, txnID), revertSupportUnknown},
		{QueueLastProcessedKey(roachpb.RKey(MakeTablePrefix(42)), "foo"), `/Local/Range/Table/42/QueueLastProcessed/"foo"`, revertSupportUnknown},

		{LocalMax, `/Meta1/""`, revertSupportUnknown}, // LocalMax == Meta1Prefix

		// system
		{makeKey(Meta2Prefix, roachpb.Key("foo")), `/Meta2/"foo"`, revertSupportUnknown},
		{makeKey(Meta1Prefix, roachpb.Key("foo")), `/Meta1/"foo"`, revertSupportUnknown},
		{RangeMetaKey(roachpb.RKey("f")).AsRawKey(), `/Meta2/"f"`, revertSupportUnknown},

		{NodeLivenessKey(10033), "/System/NodeLiveness/10033", revertSupportUnknown},
		{NodeStatusKey(1111), "/System/StatusNode/1111", revertSupportUnknown},

		{SystemMax, "/System/Max", revertSupportUnknown},

		// key of key
		{RangeMetaKey(roachpb.RKey(MakeRangeKeyPrefix(MakeTablePrefix(42)))).AsRawKey(), `/Meta2/Local/Range/Table/42`, revertSupportUnknown},
		{RangeMetaKey(roachpb.RKey(makeKey(MakeTablePrefix(42), roachpb.RKey("foo")))).AsRawKey(), `/Meta2/Table/42/"foo"`, revertSupportUnknown},
		{RangeMetaKey(roachpb.RKey(makeKey(Meta2Prefix, roachpb.Key("foo")))).AsRawKey(), `/Meta1/"foo"`, revertSupportUnknown},

		// table
		{SystemConfigSpan.Key, "/Table/SystemConfigSpan/Start", revertSupportUnknown},
		{UserTableDataMin, "/Table/50", revertMustSupport},
		{MakeTablePrefix(111), "/Table/111", revertMustSupport},
		{makeKey(MakeTablePrefix(42), encoding.EncodeUvarintAscending(nil, 1)), `/Table/42/1`, revertMustSupport},
		{makeKey(MakeTablePrefix(42), roachpb.RKey("foo")), `/Table/42/"foo"`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, float64(233.221112)))),
			"/Table/42/233.221112", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatDescending(nil, float64(-233.221112)))),
			"/Table/42/233.221112", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.Inf(1)))),
			"/Table/42/+Inf", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.NaN()))),
			"/Table/42/NaN", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222)),
			roachpb.RKey(encoding.EncodeStringAscending(nil, "handsome man"))),
			`/Table/42/1222/"handsome man"`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222))),
			`/Table/42/1222`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintDescending(nil, 1222))),
			`/Table/42/-1223`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255}))),
			`/Table/42/"\x01\x02\b\xff"`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Table/42/"\x01\x02\b\xff"/"bar"`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesDescending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Table/42/"\x01\x02\b\xff"/"bar"`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNullAscending(nil))), "/Table/42/NULL", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNullDescending(nil))), "/Table/42/NULL", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullAscending(nil))), "/Table/42/!NULL", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullDescending(nil))), "/Table/42/#", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeAscending(nil, tm))),
			"/Table/42/2016-03-30T13:40:35.053725008Z", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeDescending(nil, tm))),
			"/Table/42/1923-10-04T10:19:23.946274991Z", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalAscending(nil, apd.New(1234, -2)))),
			"/Table/42/12.34", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalDescending(nil, apd.New(1234, -2)))),
			"/Table/42/-12.34", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayAscending(nil, bitArray))),
			"/Table/42/B00111010", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayDescending(nil, bitArray))),
			"/Table/42/B00111010", revertSupportUnknown},
		// Regression test for #31115.
		{roachpb.Key(makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayAscending(nil, bitarray.MakeZeroBitArray(64))),
		)).PrefixEnd(),
			"/Table/42/B0000000000000000000000000000000000000000000000000000000000000000/PrefixEnd", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(durationAsc)),
			"/Table/42/1 mon 1 day 00:00:01", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(durationDesc)),
			"/Table/42/-2 mons -2 days +743:59:58.999999+999ns", revertSupportUnknown},

		// sequence
		{MakeSequenceKey(55), `/Table/55/1/0/0`, revertSupportUnknown},

		// others
		{makeKey([]byte("")), "/Min", revertSupportUnknown},
		{Meta1KeyMax, "/Meta1/Max", revertSupportUnknown},
		{Meta2KeyMax, "/Meta2/Max", revertSupportUnknown},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0xf6})), `/Table/42/109/PrefixEnd`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0xf7})), `/Table/42/255/PrefixEnd`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x02})), `/Table/42/"a"/PrefixEnd`, revertSupportUnknown},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x03})), `/Table/42/???`, revertSupportUnknown},
	}
	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			keyInfo := MassagePrettyPrintedSpanForTest(PrettyPrint(nil /* valDirs */, test.key), nil)
			exp := MassagePrettyPrintedSpanForTest(test.exp, nil)
			t.Logf(`---- test case #%d:
input:  %q
output: %s
exp:    %s
`, i+1, []byte(test.key), keyInfo, exp)
			if exp != keyInfo {
				t.Errorf("%d: expected %s, got %s", i, exp, keyInfo)
			}

			if exp != MassagePrettyPrintedSpanForTest(test.key.String(), nil) {
				t.Errorf("%d: from string expected %s, got %s", i, exp, test.key.String())
			}

			parsed, err := UglyPrint(keyInfo)
			if err != nil {
				if _, ok := err.(*errUglifyUnsupported); !ok {
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
	key := makeKey([]byte("a"))
	key2 := makeKey([]byte("z"))
	tableKey := makeKey(MakeTablePrefix(61), encoding.EncodeVarintAscending(nil, 4))
	tableKey2 := makeKey(MakeTablePrefix(61), encoding.EncodeVarintAscending(nil, 500))

	testCases := []struct {
		start, end roachpb.Key
		maxChars   int
		expected   string
	}{
		{key, nil, 20, "a"},
		{tableKey, nil, 10, "/Table/61…"},
		{key, key2, 20, "{a-z}"},
		{MinKey, tableKey, 8, "/{M…-T…}"},
		{MinKey, tableKey, 15, "/{Min-Tabl…}"},
		{MinKey, tableKey, 20, "/{Min-Table/6…}"},
		{MinKey, tableKey, 25, "/{Min-Table/61/4}"},
		{tableKey, tableKey2, 8, "/Table/…"},
		{tableKey, tableKey2, 15, "/Table/61/…"},
		{tableKey, tableKey2, 20, "/Table/61/{4-500}"},
		{tableKey, MaxKey, 10, "/{Ta…-Max}"},
		{tableKey, MaxKey, 20, "/{Table/6…-Max}"},
		{tableKey, MaxKey, 25, "/{Table/61/4-Max}"},
	}

	for i, tc := range testCases {
		str := PrettyPrintRange(tc.start, tc.end, tc.maxChars)
		if str != tc.expected {
			t.Errorf("%d: expected \"%s\", got \"%s\"", i, tc.expected, str)
		}
	}
}

func TestFormatHexKey(t *testing.T) {
	// Verify that we properly handling the 'x' formatting verb in
	// roachpb.Key.Format.
	key := StoreIdentKey()
	decoded, err := hex.DecodeString(fmt.Sprintf("%x", key))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key, decoded) {
		t.Fatalf("expected %s, but found %s", key, decoded)
	}
}
