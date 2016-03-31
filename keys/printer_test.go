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
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/uuid"
)

func TestPrettyPrint(t *testing.T) {

	tm, _ := time.Parse(time.RFC3339Nano, "2016-03-30T13:40:35.053725008Z")
	duration := duration.Duration{Months: 1, Days: 1, Nanos: 1 * time.Second.Nanoseconds()}
	durationAsc, _ := encoding.EncodeDurationAscending(nil, duration)
	durationDesc, _ := encoding.EncodeDurationDescending(nil, duration)
	txnID := uuid.NewV4()

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

		{AbortCacheKey(roachpb.RangeID(1000001), txnID), fmt.Sprintf(`/Local/RangeID/1000001/r/AbortCache/%q`, txnID)},
		{RaftTombstoneKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftTombstone"},
		{RaftAppliedIndexKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftAppliedIndex"},
		{RaftTruncatedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RaftTruncatedState"},
		{RangeLeaderLeaseKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeLeaderLease"},
		{RangeStatsKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeStats"},

		{RaftHardStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftHardState"},
		{RaftLastIndexKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftLastIndex"},
		{RaftLogKey(roachpb.RangeID(1000001), uint64(200001)), "/Local/RangeID/1000001/u/RaftLog/logIndex:200001"},
		{RangeLastReplicaGCTimestampKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeLastReplicaGCTimestamp"},
		{RangeLastVerificationTimestampKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeLastVerificationTimestamp"},

		{MakeRangeKeyPrefix(roachpb.RKey("ok")), `/Local/Range/"ok"`},
		{RangeDescriptorKey(roachpb.RKey("111")), `/Local/Range/"111"/RangeDescriptor`},
		{RangeTreeNodeKey(roachpb.RKey("111")), `/Local/Range/"111"/RangeTreeNode`},
		{TransactionKey(roachpb.Key("111"), txnID), fmt.Sprintf(`/Local/Range/"111"/Transaction/addrKey:/id:%q`, txnID)},

		{LocalMax, `/Meta1/""`}, // LocalMax == Meta1Prefix

		// system
		{makeKey(Meta2Prefix, roachpb.Key("foo")), `/Meta2/"foo"`},
		{makeKey(Meta1Prefix, roachpb.Key("foo")), `/Meta1/"foo"`},
		{RangeMetaKey(roachpb.RKey("f")), `/Meta2/"f"`},

		{NodeStatusKey(1111), "/System/StatusNode/1111"},

		{SystemMax, "/System/Max"},

		// key of key
		{RangeMetaKey(roachpb.RKey(MakeRangeKeyPrefix(roachpb.RKey("ok")))), `/Meta2/Local/Range/"ok"`},
		{RangeMetaKey(roachpb.RKey(makeKey(MakeTablePrefix(42), roachpb.RKey("foo")))), `/Meta2/Table/42/"foo"`},
		{RangeMetaKey(roachpb.RKey(makeKey(Meta2Prefix, roachpb.Key("foo")))), `/Meta1/"foo"`},

		// table
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
			roachpb.RKey(encoding.EncodeNotNullAscending(nil))), "/Table/42/#"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeAscending(nil, tm))),
			"/Table/42/2016-03-30T13:40:35.053725008Z"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeTimeDescending(nil, tm))),
			"/Table/42/1923-10-04T10:19:23.946274991Z"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalAscending(nil, inf.NewDec(1234, 2)))),
			"/Table/42/12.34"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(encoding.EncodeDecimalDescending(nil, inf.NewDec(1234, 2)))),
			"/Table/42/-12.34"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(durationAsc)),
			"/Table/42/1m1d1s"},
		{makeKey(MakeTablePrefix(42),
			roachpb.RKey(durationDesc)),
			"/Table/42/-2m-2d743h59m58.999999999s"},

		// others
		{makeKey([]byte("")), "/Min"},
		{Meta1KeyMax, "/Meta1/Max"},
		{Meta2KeyMax, "/Meta2/Max"},
		{makeKey(MakeTablePrefix(42), roachpb.RKey([]byte{0x12, 'a', 0x00, 0x02})), "/Table/42/<util/encoding/encoding.go:9999: unknown escape sequence: 0x0 0x2>"},
	}
	for i, test := range testCases {
		keyInfo := MassagePrettyPrintedSpanForTest(PrettyPrint(test.key), nil)
		exp := MassagePrettyPrintedSpanForTest(test.exp, nil)
		if exp != keyInfo {
			t.Errorf("%d: expected %s, got %s", i, exp, keyInfo)
		}

		if exp != MassagePrettyPrintedSpanForTest(test.key.String(), nil) {
			t.Errorf("%d: expected %s, got %s", i, exp, test.key.String())
		}

		parsed, err := UglyPrint(keyInfo)
		if err != nil {
			if _, ok := err.(*errUglifyUnsupported); !ok {
				t.Errorf("%d: %s: %s", i, keyInfo, err)
			} else {
				t.Logf("%d: skipping parsing of %s; key is unsupported: %v", i, keyInfo, err)
			}
		} else if exp, act := test.key, parsed; !bytes.Equal(exp, act) {
			t.Errorf("%d: expected %q, got %q", i, exp, act)
		}
		if t.Failed() {
			return
		}
	}
}
