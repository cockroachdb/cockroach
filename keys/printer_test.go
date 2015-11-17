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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Veteran Lu (23907238@qq.com)

package keys

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
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
		{SequenceCacheKeyPrefix(roachpb.RangeID(1000001), []byte("test0")), `/Local/RangeID/1000001/SequenceCache/"test0"`},
		{SequenceCacheKey(roachpb.RangeID(1000001), []byte("test0"), uint32(111)), `/Local/RangeID/1000001/SequenceCache/"test0"/seq:111`},
		{RaftLeaderLeaseKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftLeaderLease"},
		{RaftTombstoneKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftTombstone"},
		{RaftHardStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftHardState"},
		{RaftAppliedIndexKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftAppliedIndex"},
		{RaftLogKey(roachpb.RangeID(1000001), uint64(200001)), "/Local/RangeID/1000001/RaftLog/logIndex:200001"},
		{RaftTruncatedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftTruncatedState"},
		{RaftLastIndexKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RaftLastIndex"},
		{RangeGCMetadataKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RangeGCMetadata"},
		{RangeLastVerificationTimestampKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RangeLastVerificationTimestamp"},
		{RangeStatsKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/RangeStats"},

		{MakeRangeKeyPrefix(roachpb.RKey("ok")), `/Local/Range/"ok"`},
		{RangeDescriptorKey(roachpb.RKey("111")), `/Local/Range/RangeDescriptor/"111"`},
		{RangeTreeNodeKey(roachpb.RKey("111")), `/Local/Range/RangeTreeNode/"111"`},
		{TransactionKey(roachpb.Key("111"), []byte("22222")), `/Local/Range/Transaction/addrKey:/"111"/id:"22222"`},

		{LocalMax, "/Local/Max"},

		// system
		{roachpb.MakeKey(Meta2Prefix, roachpb.Key("foo")), `/System/Meta2/"foo"`},
		{roachpb.MakeKey(Meta1Prefix, roachpb.Key("foo")), `/System/Meta1/"foo"`},

		{StoreStatusKey(2222), "/System/StatusStore/2222"},
		{NodeStatusKey(1111), "/System/StatusNode/1111"},

		{SystemMax, "/System/Max"},

		// table
		{UserTableDataMin, "/Table/1000"},
		{MakeKey(TableDataPrefix, []byte("a")), `/Table/"a"`},
		{MakeTablePrefix(111), "/Table/111"},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey("foo")), `/Table/42/"foo"`},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey(encoding.EncodeFloat(nil, float64(233.221112)))), "/Table/42/233.221112"},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey(encoding.EncodeVarint(nil, 1222)), roachpb.RKey(encoding.EncodeString(nil, "handsome man"))), `/Table/42/1222/"handsome man"`},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey(encoding.EncodeBytes(nil, []byte{1, 2, 8, 255}))), `/Table/42/"\x01\x02\b\xff"`},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey(encoding.EncodeBytes(nil, []byte{1, 2, 8, 255})), roachpb.RKey("foo")), `/Table/42/"\x01\x02\b\xff"/"foo"`},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey(encoding.EncodeNull(nil))), "/Table/42/NULL"},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey(encoding.EncodeNotNull(nil))), "/Table/42/#"},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey(encoding.EncodeTime(nil, tm))), "/Table/42/Sat Mar  7 11:06:39 UTC 2015"},

		// others
		{MakeKey(TableDataPrefix, []byte("\xff")), "/Max"},
		{MakeKey([]byte("")), "/Min"},
		{MakeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0x31, 'a', 0x00, 0x02})), "/Table/42/<util/encoding/encoding.go:382: unknown escape>"},
	}
	for i, test := range testCases {
		keyInfo := PrettyPrint(test.key)
		if test.exp != keyInfo {
			t.Fatalf("%d: expected %s, got %s", i, test.exp, keyInfo)
		}

		if test.exp != test.key.String() {
			t.Fatalf("%d: expected %s, got %s", i, test.exp, keyInfo)
		}
	}
}
