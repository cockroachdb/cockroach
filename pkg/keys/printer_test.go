// Copyright 2014 The Cockroach Authors.
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

	// The following test cases encode keys with a mixture of ascending and descending direction,
	// but always decode keys in the ascending direction. This is why some of the decoded values
	// seem bizarre.
	testCases := []struct {
		key roachpb.Key
		exp string
	}{
		// local
		{StoreIdentKey(), "/Local/Store/storeIdent"},
		{StoreGossipKey(), "/Local/Store/gossipBootstrap"},
		{StoreClusterVersionKey(), "/Local/Store/clusterVersion"},
		{StoreSuggestedCompactionKey(MinKey, roachpb.Key("b")), `/Local/Store/suggestedCompaction/{/Min-"b"}`},
		{StoreSuggestedCompactionKey(roachpb.Key("a"), roachpb.Key("b")), `/Local/Store/suggestedCompaction/{"a"-"b"}`},
		{StoreSuggestedCompactionKey(roachpb.Key("a"), MaxKey), `/Local/Store/suggestedCompaction/{"a"-/Max}`},

		{AbortSpanKey(roachpb.RangeID(1000001), txnID), fmt.Sprintf(`/Local/RangeID/1000001/r/AbortSpan/%q`, txnID)},
		{RaftTombstoneIncorrectLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftTombstone"},
		{RangeAppliedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeAppliedState"},
		{RaftAppliedIndexLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftAppliedIndex"},
		{LeaseAppliedIndexLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/LeaseAppliedIndex"},
		{RaftTruncatedStateLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftTruncatedState"},
		{RaftTruncatedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftTruncatedState"},
		{RangeLeaseKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeLease"},
		{RangeStatsLegacyKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeStats"},
		{RangeTxnSpanGCThresholdKey(roachpb.RangeID(1000001)), `/Local/RangeID/1000001/r/RangeTxnSpanGCThreshold`},
		{RangeFrozenStatusKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeFrozenStatus"},
		{RangeLastGCKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeLastGC"},

		{RaftHardStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftHardState"},
		{RaftLastIndexKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftLastIndex"},
		{RaftTombstoneKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftTombstone"},
		{RaftLogKey(roachpb.RangeID(1000001), uint64(200001)), "/Local/RangeID/1000001/u/RaftLog/logIndex:200001"},
		{RangeLastReplicaGCTimestampKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeLastReplicaGCTimestamp"},
		{RangeLastVerificationTimestampKeyDeprecated(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeLastVerificationTimestamp"},

		{MakeRangeKeyPrefix(roachpb.RKey(MakeTablePrefix(42))), `/Local/Range/Table/42`},
		{RangeDescriptorKey(roachpb.RKey(MakeTablePrefix(42))), `/Local/Range/Table/42/RangeDescriptor`},
		{TransactionKey(roachpb.Key(MakeTablePrefix(42)), txnID), fmt.Sprintf(`/Local/Range/Table/42/Transaction/%q`, txnID)},
		{QueueLastProcessedKey(roachpb.RKey(MakeTablePrefix(42)), "foo"), `/Local/Range/Table/42/QueueLastProcessed/"foo"`},

		{LocalMax, `/Meta1/""`}, // LocalMax == Meta1Prefix

		// system
		{makeKey(Meta2Prefix, roachpb.Key("foo")), `/Meta2/"foo"`},
		{makeKey(Meta1Prefix, roachpb.Key("foo")), `/Meta1/"foo"`},
		{RangeMetaKey(roachpb.RKey("f")).AsRawKey(), `/Meta2/"f"`},

		{NodeLivenessKey(10033), "/System/NodeLiveness/10033"},
		{NodeStatusKey(1111), "/System/StatusNode/1111"},

		{SystemMax, "/System/Max"},

		// key of key
		{RangeMetaKey(roachpb.RKey(MakeRangeKeyPrefix(MakeTablePrefix(42)))).AsRawKey(), `/Meta2/Local/Range/Table/42`},
		{RangeMetaKey(roachpb.RKey(makeKey(MakeTablePrefix(42), roachpb.RKey("foo")))).AsRawKey(), `/Meta2/Table/42/"foo"`},
		{RangeMetaKey(roachpb.RKey(makeKey(Meta2Prefix, roachpb.Key("foo")))).AsRawKey(), `/Meta1/"foo"`},

		// table
		{SystemConfigSpan.Key, "/Table/SystemConfigSpan/Start"},
		{UserTableDataMin, "/Table/50"},
		{MakeTablePrefix(111), "/Table/111"},
		{makeKey(MakeTablePrefix(42), roachpb.RKey("foo")), `/Table/42/"foo"`},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, float64(233.221112)))),
			"/Table/42/233.221112"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatDescending(nil, float64(-233.221112)))),
			"/Table/42/233.221112"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.Inf(1)))),
			"/Table/42/+Inf"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.NaN()))),
			"/Table/42/NaN"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222)),
			roachpb.RKey(encoding.EncodeStringAscending(nil, "handsome man"))),
			`/Table/42/1222/"handsome man"`},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222))),
			`/Table/42/1222`},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintDescending(nil, 1222))),
			`/Table/42/-1223`},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255}))),
			`/Table/42/"\x01\x02\b\xff"`},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Table/42/"\x01\x02\b\xff"/"bar"`},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesDescending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Table/42/"\x01\x02\b\xff"/"bar"`},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNullAscending(nil))), "/Table/42/NULL"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNullDescending(nil))), "/Table/42/NULL"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullAscending(nil))), "/Table/42/!NULL"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullDescending(nil))), "/Table/42/#"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeAscending(nil, tm))),
			"/Table/42/2016-03-30T13:40:35.053725008Z"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeDescending(nil, tm))),
			"/Table/42/1923-10-04T10:19:23.946274991Z"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalAscending(nil, apd.New(1234, -2)))),
			"/Table/42/12.34"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalDescending(nil, apd.New(1234, -2)))),
			"/Table/42/-12.34"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayAscending(nil, bitArray))),
			"/Table/42/B00111010"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayDescending(nil, bitArray))),
			"/Table/42/B00111010"},
		// Regression test for #31115.
		{roachpb.Key(makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBitArrayAscending(nil, bitarray.MakeZeroBitArray(64))),
		)).PrefixEnd(),
			"/Table/42/B0000000000000000000000000000000000000000000000000000000000000000/PrefixEnd"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(durationAsc)),
			"/Table/42/1 mon 1 day 00:00:01"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(durationDesc)),
			"/Table/42/-2 mons -2 days +743:59:58.999999+999ns"},

		// sequence
		{MakeSequenceKey(55), `/Table/55/1/0/0`},

		// others
		{makeKey([]byte("")), "/Min"},
		{Meta1KeyMax, "/Meta1/Max"},
		{Meta2KeyMax, "/Meta2/Max"},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0xf6})), `/Table/42/109/PrefixEnd`},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0xf7})), `/Table/42/255/PrefixEnd`},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x02})), `/Table/42/"a"/PrefixEnd`},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x03})), `/Table/42/???`},
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
				} else {
					t.Logf("%d: skipping parsing of %s; key is unsupported: %v", i, keyInfo, err)
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
