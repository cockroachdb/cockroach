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
//
// Author: Veteran Lu (23907238@qq.com)

package keys

import (
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/decimal"
)

func TestPrettyPrint(t *testing.T) {
	defer leaktest.AfterTest(t)

	tm, _ := time.Parse(time.UnixDate, "Sat Mar  7 11:06:39 UTC 2015")

	testCases := []struct {
		key roachpb.Key
		exp string
	}{
		// local
		{StoreIdentKey(), "/Local/Store/storeIdent"},
		{StoreGossipKey(), "/Local/Store/gossipBootstrap"},
		{SequenceCacheKeyPrefix(roachpb.RangeID(1000001), []byte("test0")), `/Local/RangeID/1000001/SequenceCache/"test0"`},
		{SequenceCacheKey(roachpb.RangeID(1000001), []byte("test0"), uint32(111), uint32(222)), `/Local/RangeID/1000001/SequenceCache/"test0"/epoch:111/seq:222`},
		{RaftLeaderLeaseKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftLeaderLease"},
		{RaftTombstoneKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftTombstone"},
		{RaftHardStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftHardState"},
		{RaftAppliedIndexKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftAppliedIndex"},
		{RaftLogKey(roachpb.RangeID(1000001), uint64(200001)), "/Local/RangeID/1000001/RaftLog/logIndex:200001"},
		{RaftTruncatedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftTruncatedState"},
		{RaftLastIndexKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftLastIndex"},
		{RangeLastVerificationTimestampKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RangeLastVerificationTimestamp"},
		{RangeStatsKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RangeStats"},

		{MakeRangeKeyPrefix(roachpb.RKey("ok")), `/Local/Range/"ok"`},
		{RangeDescriptorKey(roachpb.RKey("111")), `/Local/Range/RangeDescriptor/"111"`},
		{RangeTreeNodeKey(roachpb.RKey("111")), `/Local/Range/RangeTreeNode/"111"`},
		{TransactionKey(roachpb.Key("111"), []byte("22222")), `/Local/Range/Transaction/addrKey:/"111"/id:"22222"`},

		{LocalMax, `/Meta1/""`}, // LocalMax == Meta1Prefix

		// system
		{roachpb.MakeKey(Meta2Prefix, roachpb.Key("foo")), `/Meta2/"foo"`},
		{roachpb.MakeKey(Meta1Prefix, roachpb.Key("foo")), `/Meta1/"foo"`},
		{RangeMetaKey(roachpb.RKey("f")), `/Meta2/"f"`},

		{StoreStatusKey(2222), "/System/StatusStore/2222"},
		{NodeStatusKey(1111), "/System/StatusNode/1111"},

		{SystemMax, "/System/Max"},

		// key of key
		{RangeMetaKey(roachpb.RKey(MakeRangeKeyPrefix(roachpb.RKey("ok")))), `/Meta2/Local/Range/"ok"`},
		{RangeMetaKey(roachpb.RKey(MakeKey(MakeTablePrefix(42), roachpb.RKey("foo")))), `/Meta2/Table/42/"foo"`},
		{RangeMetaKey(roachpb.RKey(roachpb.MakeKey(Meta2Prefix, roachpb.Key("foo")))), `/Meta1/"foo"`},

		// table
		{UserTableDataMin, "/Table/50"},
		{MakeTablePrefix(111), "/Table/111"},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey("foo")), `/Table/42/"foo"`},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, float64(233.221112)))),
			"/Table/42/233.221112"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatDescending(nil, float64(-233.221112)))),
			"/Table/42/233.221112"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.Inf(1)))),
			"/Table/42/+Inf"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeFloatAscending(nil, math.NaN()))),
			"/Table/42/NaN"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222)),
			roachpb.RKey(encoding.EncodeStringAscending(nil, "handsome man"))),
			`/Table/42/1222/"handsome man"`},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintAscending(nil, 1222))),
			`/Table/42/1222`},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeVarintDescending(nil, 1222))),
			`/Table/42/-1223`},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255}))),
			`/Table/42/"\x01\x02\b\xff"`},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesAscending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Table/42/"\x01\x02\b\xff"/"bar"`},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeBytesDescending(nil, []byte{1, 2, 8, 255})),
			roachpb.RKey("bar")), `/Table/42/"\x01\x02\b\xff"/"bar"`},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNullAscending(nil))), "/Table/42/NULL"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeNotNullAscending(nil))), "/Table/42/#"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeAscending(nil, tm))),
			"/Table/42/Sat Mar  7 11:06:39 UTC 2015"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeDescending(nil, tm))),
			"/Table/42/Sat Mar  7 11:06:39 UTC 2015"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalAscending(nil, decimal.New(1234, -2)))),
			"/Table/42/12.34"},
		{MakeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalDescending(nil, decimal.New(1234, -2)))),
			"/Table/42/-12.34"},

		// others
		{MakeKey([]byte("")), "/Min"},
		{Meta1KeyMax, "/Meta1/Max"},
		{Meta2KeyMax, "/Meta2/Max"},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0x21, 'a', 0x00, 0x02})), "/Table/42/<util/encoding/encoding.go:9999: unknown escape sequence: 0x0 0x2>"},
	}
	for i, test := range testCases {
		keyInfo := MassagePrettyPrintedSpanForTest(PrettyPrint(test.key), nil)
		exp := MassagePrettyPrintedSpanForTest(test.exp, nil)
		if exp != keyInfo {
			t.Fatalf("%d: expected %s, got %s", i, exp, keyInfo)
		}

		if exp != MassagePrettyPrintedSpanForTest(test.key.String(), nil) {
			t.Fatalf("%d: expected %s, got %s", i, exp, test.key.String())
		}
	}
}
