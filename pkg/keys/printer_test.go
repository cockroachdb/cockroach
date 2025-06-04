// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lockTableKey(key roachpb.Key) roachpb.Key {
	k, _ := keys.LockTableSingleKey(key, nil)
	return k
}

func TestSafeFormatKey_SystemTenant(t *testing.T) {
	tenSysCodec := keys.SystemSQLCodec
	testCases := []struct {
		name string
		key  roachpb.Key
		exp  string
	}{
		{
			"table with string index key",
			roachpb.Key(makeKey(tenSysCodec.TablePrefix(42),
				encoding.EncodeVarintAscending(nil, 1222),
				encoding.EncodeStringAscending(nil, "handsome man"))),
			`/Table/42/1222/‹"handsome man"›`,
		},
		{
			"multi-column value",
			roachpb.Key(makeKey(tenSysCodec.TablePrefix(42),
				encoding.EncodeStringAscending(nil, "California"),
				encoding.EncodeStringAscending(nil, "Los Angeles"))),
			`/Table/42/‹"California"›/‹"Los Angeles"›`,
		},
		{
			"table with decimal index key",
			roachpb.Key(makeKey(tenSysCodec.IndexPrefix(84, 2),
				encoding.EncodeDecimalAscending(nil, apd.New(1234, -2)))),
			`/Table/84/2/‹12.34›`,
		},
		{
			"namespace table handled as standard system tenant table",
			keys.NamespaceTableMin,
			"/NamespaceTable/30",
		},
		{
			"table index without index key",
			tenSysCodec.IndexPrefix(42, 5),
			"/Table/42/5",
		},
		{
			"handles infinity values",
			makeKey(tenSysCodec.TablePrefix(42),
				encoding.EncodeFloatAscending(nil, math.Inf(1))),
			"/Table/42/‹+Inf›",
		},
		{
			"handles null values",
			makeKey(tenSysCodec.TablePrefix(42),
				encoding.EncodeNullAscending(nil)),
			"/Table/42/‹NULL›",
		},
		{
			"handles PrefixEnd",
			roachpb.Key(makeKey(tenSysCodec.TablePrefix(42),
				encoding.EncodeBitArrayAscending(nil, bitarray.MakeZeroBitArray(64)),
			)).PrefixEnd(),
			"/Table/42/‹B0000000000000000000000000000000000000000000000000000000000000000›/‹PrefixEnd›",
		},
		{
			"handles /Table/Max",
			keys.TableDataMax,
			"/Table/Max",
		},
		{
			"handles unknowns",
			makeKey(tenSysCodec.TablePrefix(42), []byte{0x12, 'a', 0x00, 0x03}),
			`/Table/42/‹???›`,
		},
		{
			"marks safe reserved key prefix",
			makeKey(keys.Meta1Prefix,
				tenSysCodec.TablePrefix(42),
				encoding.EncodeStringAscending(nil, "California"),
				encoding.EncodeStringAscending(nil, "Los Angeles")),
			`/Meta1/Table/42/‹"California"›/‹"Los Angeles"›`,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, redact.RedactableString(test.exp), redact.Sprint(test.key))
		})
	}
}

func TestSafeFormatKey_Basic(t *testing.T) {
	testCases := []struct {
		name string
		key  roachpb.Key
		exp  string
	}{
		{
			"ensure constants get redacted",
			makeKey(keys.Meta2Prefix, roachpb.Key("foo")),
			`/Meta2/‹"foo"›`,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, redact.RedactableString(test.exp), redact.Sprint(test.key))
		})
	}
}

func TestSafeFormatKey_AppTenant(t *testing.T) {
	ten5Codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(5))
	testCases := []struct {
		name string
		key  roachpb.Key
		exp  string
	}{
		{
			"table with string index key",
			roachpb.Key(makeKey(ten5Codec.IndexPrefix(42, 122),
				encoding.EncodeStringAscending(nil, "handsome man"))),
			`/Tenant/5/Table/42/122/‹"handsome man"›`,
		},
		{
			"table with decimal index key",
			roachpb.Key(makeKey(ten5Codec.IndexPrefix(84, 2),
				encoding.EncodeDecimalAscending(nil, apd.New(1234, -2)))),
			`/Tenant/5/Table/84/2/‹12.34›`,
		},
		{
			"multi-column value",
			roachpb.Key(makeKey(ten5Codec.TablePrefix(42),
				encoding.EncodeStringAscending(nil, "California"),
				encoding.EncodeStringAscending(nil, "Los Angeles"))),
			`/Tenant/5/Table/42/‹"California"›/‹"Los Angeles"›`,
		},
		{
			"table index without index key",
			ten5Codec.IndexPrefix(42, 5),
			"/Tenant/5/Table/42/5",
		},
		{
			"handles infinity values",
			makeKey(ten5Codec.TablePrefix(42),
				encoding.EncodeFloatAscending(nil, math.Inf(1))),
			"/Tenant/5/Table/42/‹+Inf›",
		},
		{
			"handles null values",
			makeKey(ten5Codec.IndexPrefix(42, 2),
				encoding.EncodeNullAscending(nil)),
			"/Tenant/5/Table/42/2/‹NULL›",
		},
		{
			"handles PrefixEnd",
			roachpb.Key(makeKey(ten5Codec.TablePrefix(42),
				encoding.EncodeBitArrayAscending(nil, bitarray.MakeZeroBitArray(64)),
			)).PrefixEnd(),
			"/Tenant/5/Table/42/‹B0000000000000000000000000000000000000000000000000000000000000000›/‹PrefixEnd›",
		},
		{
			"handles unknowns",
			makeKey(ten5Codec.TablePrefix(42), []byte{0x12, 'a', 0x00, 0x03}),
			`/Tenant/5/Table/42/‹???›`,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, redact.RedactableString(test.exp), redact.Sprint(test.key))
		})
	}
}

func TestPrettyPrint(t *testing.T) {
	tenSysCodec := keys.SystemSQLCodec
	ten5Codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(5))
	tm, _ := time.Parse(time.RFC3339Nano, "2016-03-30T13:40:35.053725008Z")
	duration := duration.MakeDuration(1*time.Second.Nanoseconds(), 1, 1)
	durationAsc, _ := encoding.EncodeDurationAscending(nil, duration)
	durationDesc, _ := encoding.EncodeDurationDescending(nil, duration)
	bitArray := bitarray.MakeBitArrayFromInt64(8, 58, 7)
	txnID := uuid.MakeV4()
	loqRecoveryID := uuid.MakeV4()

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
		{keys.DeprecatedStoreClusterVersionKey(), "/Local/Store/clusterVersion", revertSupportUnknown},
		{keys.StoreNodeTombstoneKey(123), "/Local/Store/nodeTombstone/n123", revertSupportUnknown},
		{keys.StoreCachedSettingsKey(roachpb.Key("a")), `/Local/Store/cachedSettings/"a"`, revertSupportUnknown},
		{keys.StoreUnsafeReplicaRecoveryKey(loqRecoveryID), fmt.Sprintf(`/Local/Store/lossOfQuorumRecovery/applied/%s`, loqRecoveryID), revertSupportUnknown},
		{keys.StoreLossOfQuorumRecoveryStatusKey(), "/Local/Store/lossOfQuorumRecovery/status", revertSupportUnknown},
		{keys.StoreLossOfQuorumRecoveryCleanupActionsKey(), "/Local/Store/lossOfQuorumRecovery/cleanup", revertSupportUnknown},

		{keys.AbortSpanKey(roachpb.RangeID(1000001), txnID), fmt.Sprintf(`/Local/RangeID/1000001/r/AbortSpan/%q`, txnID), revertSupportUnknown},
		{keys.ReplicatedSharedLocksTransactionLatchingKey(roachpb.RangeID(1000001), txnID), fmt.Sprintf(`/Local/RangeID/1000001/r/ReplicatedSharedLocksTransactionLatch/%q`, txnID), revertSupportUnknown},
		{keys.RangeAppliedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeAppliedState", revertSupportUnknown},
		{keys.RaftTruncatedStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftTruncatedState", revertSupportUnknown},
		{keys.RangeLeaseKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeLease", revertSupportUnknown},
		{keys.RangePriorReadSummaryKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangePriorReadSummary", revertSupportUnknown},
		{keys.RangeGCThresholdKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeGCThreshold", revertSupportUnknown},
		{keys.RangeVersionKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeVersion", revertSupportUnknown},
		{keys.RangeGCHintKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/r/RangeGCHint", revertSupportUnknown},

		{keys.RaftHardStateKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RaftHardState", revertSupportUnknown},
		{keys.RangeTombstoneKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeTombstone", revertSupportUnknown},
		{keys.RaftLogKey(roachpb.RangeID(1000001), kvpb.RaftIndex(200001)), "/Local/RangeID/1000001/u/RaftLog/logIndex:200001", revertSupportUnknown},
		{keys.RangeLastReplicaGCTimestampKey(roachpb.RangeID(1000001)), "/Local/RangeID/1000001/u/RangeLastReplicaGCTimestamp", revertSupportUnknown},

		{keys.MakeRangeKeyPrefix(roachpb.RKey(tenSysCodec.TablePrefix(42))), `/Local/Range/Table/42`, revertSupportUnknown},
		{keys.RangeDescriptorKey(roachpb.RKey(tenSysCodec.TablePrefix(42))), `/Local/Range/Table/42/RangeDescriptor`, revertSupportUnknown},
		{keys.TransactionKey(tenSysCodec.TablePrefix(42), txnID), fmt.Sprintf(`/Local/Range/Table/42/Transaction/%q`, txnID), revertSupportUnknown},
		{keys.RangeProbeKey(roachpb.RKey(tenSysCodec.TablePrefix(42))), `/Local/Range/Table/42/RangeProbe`, revertSupportUnknown},
		{keys.QueueLastProcessedKey(roachpb.RKey(tenSysCodec.TablePrefix(42)), "foo"), `/Local/Range/Table/42/QueueLastProcessed/"foo"`, revertSupportUnknown},
		{lockTableKey(keys.RangeDescriptorKey(roachpb.RKey(tenSysCodec.TablePrefix(42)))), `/Local/Lock/Local/Range/Table/42/RangeDescriptor`, revertSupportUnknown},
		{lockTableKey(tenSysCodec.TablePrefix(111)), "/Local/Lock/Table/111", revertSupportUnknown},

		{keys.MakeRangeKeyPrefix(roachpb.RKey(ten5Codec.TenantPrefix())), `/Local/Range/Tenant/5`, revertSupportUnknown},
		{keys.MakeRangeKeyPrefix(roachpb.RKey(ten5Codec.TablePrefix(42))), `/Local/Range/Tenant/5/Table/42`, revertSupportUnknown},
		{keys.RangeDescriptorKey(roachpb.RKey(ten5Codec.TablePrefix(42))), `/Local/Range/Tenant/5/Table/42/RangeDescriptor`, revertSupportUnknown},
		{keys.TransactionKey(ten5Codec.TablePrefix(42), txnID), fmt.Sprintf(`/Local/Range/Tenant/5/Table/42/Transaction/%q`, txnID), revertSupportUnknown},
		{keys.QueueLastProcessedKey(roachpb.RKey(ten5Codec.TablePrefix(42)), "foo"), `/Local/Range/Tenant/5/Table/42/QueueLastProcessed/"foo"`, revertSupportUnknown},
		{lockTableKey(keys.RangeDescriptorKey(roachpb.RKey(ten5Codec.TablePrefix(42)))), `/Local/Lock/Local/Range/Tenant/5/Table/42/RangeDescriptor`, revertSupportUnknown},
		{lockTableKey(ten5Codec.TablePrefix(111)), "/Local/Lock/Tenant/5/Table/111", revertSupportUnknown},

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
			roachpb.RKey(encoding.EncodeNotNullDescending(nil))), "/Table/42/!NULL", revertSupportUnknown},
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
		{ten5Codec.TablePrefix(0), "/Tenant/5/Table/0", revertSupportUnknown},
		{ten5Codec.TablePrefix(50), "/Tenant/5/Table/50", revertMustSupport},
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
			roachpb.RKey(encoding.EncodeNotNullDescending(nil))), "/Tenant/5/Table/42/!NULL", revertSupportUnknown},
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

		// Special characters.
		{makeKey(tenSysCodec.TablePrefix(61),
			encoding.EncodeBytesAscending(nil, []byte("☃⚠"))),
			`/Table/61/"☃⚠"`, revertSupportUnknown,
		},
		// Invalid utf-8 sequence.
		{makeKey(tenSysCodec.TablePrefix(61),
			encoding.EncodeBytesAscending(nil, []byte{0xff, 0xff})),
			`/Table/61/"\xff\xff"`, revertSupportUnknown,
		},
	}
	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			keyPP := keys.PrettyPrint(nil /* valDirs */, test.key)
			keyInfo := massagePrettyPrintedSpanForTest(keyPP, nil)
			exp := massagePrettyPrintedSpanForTest(test.exp, nil)
			t.Logf(`---- test case #%d:
input:  %q
output: %s
exp:    %s
`, i+1, []byte(test.key), keyInfo, exp)
			if exp != keyInfo {
				t.Errorf("%d: expected:\n%+v\ngot:\n%+v", i, []byte(exp), []byte(keyInfo))
			}

			if exp != massagePrettyPrintedSpanForTest(test.key.String(), nil) {
				t.Errorf("%d: from string expected %s, got %s", i, exp, test.key.String())
			}

			scanner := keysutil.MakePrettyScanner(nil /* tableParser */, nil /* tenantParser */)
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
				t.Errorf("%d: ugly print expected '%q', got '%q'", i, exp, act)
			}
			if t.Failed() {
				return
			}
		})
	}
}

// massagePrettyPrintedSpanForTest does some transformations on pretty-printed spans and keys:
// - if dirs is not nil, replace all ints with their ones' complement for
// descendingly-encoded columns.
// - strips line numbers from error messages.
func massagePrettyPrintedSpanForTest(span string, dirs []encoding.Direction) string {
	var r strings.Builder
	colIdx := -1
	for i := 0; i < len(span); i++ {
		if dirs != nil {
			var d int
			if _, err := fmt.Sscanf(span[i:], "%d", &d); err == nil {
				// We've managed to consume an int.
				dir := dirs[colIdx]
				i += len(strconv.Itoa(d)) - 1
				x := d
				if dir == encoding.Descending {
					x = ^x
				}
				r.WriteString(strconv.Itoa(x))
				continue
			}
		}
		switch {
		case span[i] == '/':
			colIdx++
			r.WriteByte(span[i])
		case span[i] == '-' || span[i] == ' ':
			// We're switching from the start constraints to the end constraints,
			// or starting another span.
			colIdx = -1
			r.WriteByte(span[i])
		case span[i] < ' ':
			fmt.Fprintf(&r, "\\x%02x", span[i])
		case span[i] <= utf8.RuneSelf:
			r.WriteByte(span[i])
		default:
			c, width := utf8.DecodeRuneInString(span[i:])
			if c == utf8.RuneError {
				fmt.Fprintf(&r, "\\x%02x", span[i])
			} else {
				r.WriteRune(c)
			}
			i += width - 1
		}
	}
	return r.String()
}

func TestPrettyPrintRange(t *testing.T) {
	tenSysCodec := keys.SystemSQLCodec
	ten5Codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(5))
	key := makeKey([]byte("a"))
	key2 := makeKey([]byte("z"))
	tableKey := makeKey(tenSysCodec.TablePrefix(61), encoding.EncodeVarintAscending(nil, 4))
	tableKey2 := makeKey(tenSysCodec.TablePrefix(61), encoding.EncodeVarintAscending(nil, 500))
	tenTableKey := makeKey(ten5Codec.TablePrefix(61), encoding.EncodeVarintAscending(nil, 999))
	specialBytesKeyA := makeKey(tenSysCodec.TablePrefix(61), encoding.EncodeBytesAscending(nil, []byte("☃️")))
	specialBytesKeyB := makeKey(tenSysCodec.TablePrefix(61), encoding.EncodeBytesAscending(nil, []byte("☃️⚠")))
	specialBytesKeyC := makeKey(tenSysCodec.TablePrefix(61), encoding.EncodeBytesAscending(nil, []byte{0xff, 0x00}))
	specialBytesKeyD := makeKey(tenSysCodec.TablePrefix(61), encoding.EncodeBytesAscending(nil, []byte{0xff, 0xfe}))

	testCases := []struct {
		start, end roachpb.Key
		maxChars   int
		expected   redact.RedactableString
	}{
		{key, nil, 20, "‹a›"},
		{tableKey, nil, 10, "/Table/61…"},
		{tableKey, specialBytesKeyB, 20, `/Table/61/{4-‹"☃›…}`},
		{tableKey, specialBytesKeyB, 30, `/Table/61/{4-‹"☃️⚠"›}`},
		{tableKey, specialBytesKeyB, 50, `/Table/61/{4-‹"☃️⚠"›}`},
		{specialBytesKeyA, specialBytesKeyB, 20, `/Table/61/‹"☃️›…`},
		{specialBytesKeyA, specialBytesKeyB, 25, `/Table/61/‹"☃️›{‹"›-‹⚠"›}`},
		{specialBytesKeyA, specialBytesKeyB, 30, `/Table/61/‹"☃️›{‹"›-‹⚠"›}`},
		// Note: the PrettyPrintRange() algorithm operates on the result
		// of PrettyPrint(), which already turns special characters into
		// hex sequences. Therefore, it can merge and truncate the hex
		// codes. To improve this would require making PrettyPrint() take
		// a bool flag to return un-escaped bytes, and let
		// PrettyPrintRange() escape the output adequately.
		//
		// Since all of this is best-effort, we'll accept the status quo
		// for now.
		{specialBytesKeyC, specialBytesKeyD, 20, `/Table/61/‹"\xff\x›…`},
		{specialBytesKeyC, specialBytesKeyD, 30, `/Table/61/‹"\xff\x›{‹00"›-‹fe"›}`},
		{specialBytesKeyB, specialBytesKeyD, 20, `/Table/61/‹"›{‹☃›…-‹\›…}`},
		{specialBytesKeyB, specialBytesKeyD, 30, `/Table/61/‹"›{‹☃️⚠"›-‹\xff\x›…}`},
		{specialBytesKeyB, specialBytesKeyD, 50, `/Table/61/‹"›{‹☃️⚠"›-‹\xff\xfe"›}`},
		{tenTableKey, nil, 20, "/Tenant/5/Table/61/…"},
		{key, key2, 20, "{‹a›-‹z›}"},
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
			t.Errorf("%d: expected:\n%s\ngot:\n%s", i, tc.expected, str)
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

func TestCopyEscape(t *testing.T) {
	invalidUTF8 := []byte("🪳")[:2]
	expectEscaped := "\\xf0\\x9f"

	tt := []struct {
		input    string
		expected redact.RedactableString
		maxChars int
	}{
		{"abc", "abc", math.MaxInt32},
		{"abc", "abc", 3},
		{"abc", "a…", 2},

		// should handle redaction markers
		{"abc‹cde›", redact.Sprintf("abc%s", "cde"), math.MaxInt32},
		{"abc‹def›ghi", redact.Sprintf("abc%sghi", "def"), math.MaxInt32},
		{"abc‹cde›", redact.Sprint(redact.SafeString("abc…")), 4},

		// should handle other valid UTF-8 characters
		{"abc🪳‹def›ghi", redact.Sprintf("abc🪳%sghi", "def"), math.MaxInt32},
		{"abc🪳def", redact.Sprint(redact.SafeString("abc🪳def")), math.MaxInt32},
		{"abc‹de🪳f›ghi", redact.Sprintf("abc%sghi", "de🪳f"), math.MaxInt32},
		{"abc‹de🪳f›ghi", redact.Sprintf("abc%s…", "de"), 6},

		// should handle partial redaction markers
		{"abc‹cd", redact.Sprintf("abc%s", "cd"), math.MaxInt32},
		{"abc‹cd", redact.Sprintf("ab…"), 3},
		{"abc‹cd", redact.Sprintf("abc…"), 4},
		{"abc›cd", redact.Sprintf("%scd", "abc"), math.MaxInt32},
		{"abc›cd", redact.Sprintf("%s…", "a"), 2},
		{"abc›cd", redact.Sprintf("%s…", "abc"), 4},

		// should handle invalid UTF-8 characters
		{string(append([]byte("abc"), invalidUTF8...)), redact.Sprintf("abc%s", redact.SafeString(expectEscaped)), math.MaxInt32},
		{string(append(invalidUTF8, []byte("abc")...)), redact.Sprintf("%sabc", redact.SafeString(expectEscaped)), math.MaxInt32},
		{string(append([]byte("abc‹"), invalidUTF8...)), redact.Sprintf("abc%s", expectEscaped), math.MaxInt32},
		{string(append([]byte("abc‹"), append(invalidUTF8, []byte("›def")...)...)), redact.Sprintf("abc%sdef", expectEscaped), math.MaxInt32},
		{string(append(invalidUTF8, []byte("abc")...)), redact.Sprintf("%s…", redact.SafeString(expectEscaped)[:4]), 2}, // the 1st escaped char is 4 bytes

		// should handle control characters (code points < 0x20)
		{"abc\x01\x02def", redact.Sprintf("abc\\x01\\x02def"), math.MaxInt32},
		{"abc‹\x00\x1F›def", redact.Sprintf("abc%sdef", "\\x00\\x1f"), math.MaxInt32},
		{"\x00abc\x1f", redact.Sprintf("\\x00abc\\x1f"), math.MaxInt32},
		{"\x00abc\x1f", redact.Sprintf("\\x00a…"), 3},
	}

	for _, tc := range tt {
		t.Run("", func(t *testing.T) {
			var b redact.StringBuilder
			keys.CopyEscapeTrunc(&b, tc.input, tc.maxChars)
			assert.Equal(t, tc.expected, b.RedactableString())
		})
	}
}
