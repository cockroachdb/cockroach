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
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestStoreKeyEncodeDecode(t *testing.T) {
	testCases := []struct {
		key       roachpb.Key
		expSuffix roachpb.RKey
		expDetail roachpb.RKey
	}{
		{key: StoreIdentKey(), expSuffix: localStoreIdentSuffix, expDetail: nil},
		{key: StoreGossipKey(), expSuffix: localStoreGossipSuffix, expDetail: nil},
		{key: StoreClusterVersionKey(), expSuffix: localStoreClusterVersionSuffix, expDetail: nil},
		{key: StoreLastUpKey(), expSuffix: localStoreLastUpSuffix, expDetail: nil},
		{key: StoreHLCUpperBoundKey(), expSuffix: localHLCUpperBoundSuffix, expDetail: nil},
		{
			key:       StoreSuggestedCompactionKey(roachpb.Key("a"), roachpb.Key("z")),
			expSuffix: localStoreSuggestedCompactionSuffix,
			expDetail: encoding.EncodeBytesAscending(encoding.EncodeBytesAscending(nil, roachpb.Key("a")), roachpb.Key("z")),
		},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			if suffix, detail, err := DecodeStoreKey(test.key); err != nil {
				t.Error(err)
			} else if !suffix.Equal(test.expSuffix) {
				t.Errorf("expected %s; got %s", test.expSuffix, suffix)
			} else if !detail.Equal(test.expDetail) {
				t.Errorf("expected %s; got %s", test.expDetail, detail)
			}
		})
	}
}

func TestStoreSuggestedCompactionKeyDecode(t *testing.T) {
	origStart := roachpb.Key("a")
	origEnd := roachpb.Key("z")
	key := StoreSuggestedCompactionKey(origStart, origEnd)
	start, end, err := DecodeStoreSuggestedCompactionKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(origStart) {
		t.Errorf("expected %s == %s", start, origStart)
	}
	if !end.Equal(origEnd) {
		t.Errorf("expected %s == %s", end, origEnd)
	}
}

// TestLocalKeySorting is a sanity check to make sure that
// the non-replicated part of a store sorts before the meta.
func TestKeySorting(t *testing.T) {
	// Reminder: Increasing the last byte by one < adding a null byte.
	if !(roachpb.RKey("").Less(roachpb.RKey("\x00")) && roachpb.RKey("\x00").Less(roachpb.RKey("\x01")) &&
		roachpb.RKey("\x01").Less(roachpb.RKey("\x01\x00"))) {
		t.Fatalf("something is seriously wrong with this machine")
	}
	if bytes.Compare(localPrefix, Meta1Prefix) >= 0 {
		t.Fatalf("local key spilling into replicated ranges")
	}
	if !bytes.Equal(roachpb.Key(""), roachpb.Key(nil)) {
		t.Fatalf("equality between keys failed")
	}
}

func TestMakeKey(t *testing.T) {
	if !bytes.Equal(makeKey(roachpb.Key("A"), roachpb.Key("B")), roachpb.Key("AB")) ||
		!bytes.Equal(makeKey(roachpb.Key("A")), roachpb.Key("A")) ||
		!bytes.Equal(makeKey(roachpb.Key("A"), roachpb.Key("B"), roachpb.Key("C")), roachpb.Key("ABC")) {
		t.Fatalf("MakeKey is broken")
	}
}

func TestAbortSpanEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const rangeID = 123
	testTxnID, err := uuid.FromString("0ce61c17-5eb4-4587-8c36-dcf4062ada4c")
	if err != nil {
		t.Fatal(err)
	}
	key := AbortSpanKey(rangeID, testTxnID)
	txnID, err := DecodeAbortSpanKey(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if txnID != testTxnID {
		t.Fatalf("expected txnID %q, got %q", testTxnID, txnID)
	}
}

func TestKeyAddress(t *testing.T) {
	testCases := []struct {
		key        roachpb.Key
		expAddress roachpb.RKey
	}{
		{roachpb.Key{}, roachpb.RKeyMin},
		{roachpb.Key("123"), roachpb.RKey("123")},
		{RangeDescriptorKey(roachpb.RKey("foo")), roachpb.RKey("foo")},
		{TransactionKey(roachpb.Key("baz"), uuid.MakeV4()), roachpb.RKey("baz")},
		{TransactionKey(roachpb.KeyMax, uuid.MakeV4()), roachpb.RKeyMax},
		{RangeDescriptorKey(roachpb.RKey(TransactionKey(roachpb.Key("doubleBaz"), uuid.MakeV4()))), roachpb.RKey("doubleBaz")},
		{nil, nil},
	}
	for i, test := range testCases {
		if keyAddr, err := Addr(test.key); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if !keyAddr.Equal(test.expAddress) {
			t.Errorf("%d: expected address for key %q doesn't match %q", i, test.key, test.expAddress)
		}
	}
}

func TestKeyAddressError(t *testing.T) {
	testCases := map[string][]roachpb.Key{
		"store-local key .* is not addressable": {
			StoreIdentKey(),
			StoreGossipKey(),
		},
		"local range ID key .* is not addressable": {
			AbortSpanKey(0, uuid.MakeV4()),
			RaftTombstoneKey(0),
			RaftAppliedIndexLegacyKey(0),
			RaftTruncatedStateLegacyKey(0),
			RangeLeaseKey(0),
			RangeStatsLegacyKey(0),
			RaftHardStateKey(0),
			RaftLastIndexKey(0),
			RaftLogPrefix(0),
			RaftLogKey(0, 0),
			RangeLastReplicaGCTimestampKey(0),
			RangeLastVerificationTimestampKeyDeprecated(0),
			RangeDescriptorKey(roachpb.RKey(RangeLastVerificationTimestampKeyDeprecated(0))),
		},
		"local key .* malformed": {
			makeKey(localPrefix, roachpb.Key("z")),
		},
	}
	for regexp, keyList := range testCases {
		for _, key := range keyList {
			if addr, err := Addr(key); err == nil {
				t.Errorf("expected addressing key %q to throw error, but it returned address %q",
					key, addr)
			} else if !testutils.IsError(err, regexp) {
				t.Errorf("expected addressing key %q to throw error matching %s, but got error %v",
					key, regexp, err)
			}
		}
	}
}

func TestRangeMetaKey(t *testing.T) {
	testCases := []struct {
		key, expKey roachpb.RKey
	}{
		{
			key:    roachpb.RKey{},
			expKey: roachpb.RKeyMin,
		},
		{
			key:    roachpb.RKey("\x03\x04zonefoo"),
			expKey: roachpb.RKey("\x02\x04zonefoo"),
		},
		{
			key:    roachpb.RKey("\x02\x04zonefoo"),
			expKey: roachpb.RKeyMin,
		},
		{
			key:    roachpb.RKey("foo"),
			expKey: roachpb.RKey("\x03foo"),
		},
		{
			key:    roachpb.RKey("\x03foo"),
			expKey: roachpb.RKey("\x02foo"),
		},
		{
			key:    roachpb.RKey("\x02foo"),
			expKey: roachpb.RKeyMin,
		},
	}
	for i, test := range testCases {
		result := RangeMetaKey(test.key)
		if !bytes.Equal(result, test.expKey) {
			t.Errorf("%d: expected range meta for key %q doesn't match %q (%q)",
				i, test.key, test.expKey, result)
		}
	}
}

func TestUserKey(t *testing.T) {
	testCases := []struct {
		key, expKey roachpb.RKey
	}{
		{
			key:    roachpb.RKeyMin,
			expKey: roachpb.RKey(Meta1Prefix),
		},
		{
			key:    roachpb.RKey("\x02\x04zonefoo"),
			expKey: roachpb.RKey("\x03\x04zonefoo"),
		},
		{
			key:    roachpb.RKey("\x03foo"),
			expKey: roachpb.RKey("foo"),
		},
		{
			key:    roachpb.RKey("foo"),
			expKey: roachpb.RKey("foo"),
		},
	}
	for i, test := range testCases {
		result := UserKey(test.key)
		if !bytes.Equal(result, test.expKey) {
			t.Errorf("%d: expected range meta for key %q doesn't match %q (%q)",
				i, test.key, test.expKey, result)
		}
	}
}

func TestSequenceKey(t *testing.T) {
	actual := MakeSequenceKey(55)
	expected := []byte("\xbf\x89\x88\x88")
	if !bytes.Equal(actual, expected) {
		t.Errorf("expected %q (len %d), got %q (len %d)", expected, len(expected), actual, len(actual))
	}
}

// TestMetaPrefixLen asserts that both levels of meta keys have the same prefix length,
// as MetaScanBounds, MetaReverseScanBounds and validateRangeMetaKey depend on this fact.
func TestMetaPrefixLen(t *testing.T) {
	if len(Meta1Prefix) != len(Meta2Prefix) {
		t.Fatalf("Meta1Prefix %q and Meta2Prefix %q are not of equal length!", Meta1Prefix, Meta2Prefix)
	}
}

func TestMetaScanBounds(t *testing.T) {

	testCases := []struct {
		key, expStart, expEnd []byte
		expError              string
	}{
		{
			key:      roachpb.RKey{},
			expStart: Meta1Prefix,
			expEnd:   Meta1Prefix.PrefixEnd(),
			expError: "",
		},
		{
			key:      makeKey(Meta2Prefix, roachpb.Key("foo")),
			expStart: makeKey(Meta2Prefix, roachpb.Key("foo\x00")),
			expEnd:   Meta2Prefix.PrefixEnd(),
			expError: "",
		},
		{
			key:      makeKey(Meta1Prefix, roachpb.Key("foo")),
			expStart: makeKey(Meta1Prefix, roachpb.Key("foo\x00")),
			expEnd:   Meta1Prefix.PrefixEnd(),
			expError: "",
		},
		{
			key:      makeKey(Meta1Prefix, roachpb.RKeyMax),
			expStart: makeKey(Meta1Prefix, roachpb.RKeyMax),
			expEnd:   Meta1Prefix.PrefixEnd(),
			expError: "",
		},
		{
			key:      Meta2KeyMax,
			expStart: nil,
			expEnd:   nil,
			expError: "Meta2KeyMax can't be used as the key of scan",
		},
		{
			key:      Meta2KeyMax.Next(),
			expStart: nil,
			expEnd:   nil,
			expError: "body of meta key range lookup is",
		},
		{
			key:      Meta1KeyMax.Next(),
			expStart: nil,
			expEnd:   nil,
			expError: "body of meta key range lookup is",
		},
	}
	for i, test := range testCases {
		res, err := MetaScanBounds(test.key)

		if !testutils.IsError(err, test.expError) {
			t.Errorf("expected error: %s ; got %v", test.expError, err)
		}

		expected := roachpb.RSpan{Key: test.expStart, EndKey: test.expEnd}
		if !res.Equal(expected) {
			t.Errorf("%d: range bounds %s don't match expected bounds %s for key %s",
				i, res, expected, roachpb.Key(test.key))
		}
	}
}

func TestMetaReverseScanBounds(t *testing.T) {
	testCases := []struct {
		key              []byte
		expStart, expEnd []byte
		expError         string
	}{
		{
			key:      roachpb.RKey{},
			expStart: nil,
			expEnd:   nil,
			expError: "KeyMin and Meta1Prefix can't be used as the key of reverse scan",
		},
		{
			key:      Meta1Prefix,
			expStart: nil,
			expEnd:   nil,
			expError: "KeyMin and Meta1Prefix can't be used as the key of reverse scan",
		},
		{
			key:      Meta2KeyMax.Next(),
			expStart: nil,
			expEnd:   nil,
			expError: "body of meta key range lookup is",
		},
		{
			key:      Meta1KeyMax.Next(),
			expStart: nil,
			expEnd:   nil,
			expError: "body of meta key range lookup is",
		},
		{
			key:      makeKey(Meta2Prefix, roachpb.Key("foo")),
			expStart: Meta2Prefix,
			expEnd:   makeKey(Meta2Prefix, roachpb.Key("foo\x00")),
			expError: "",
		},
		{
			key:      makeKey(Meta1Prefix, roachpb.Key("foo")),
			expStart: Meta1Prefix,
			expEnd:   makeKey(Meta1Prefix, roachpb.Key("foo\x00")),
			expError: "",
		},
		{
			key:      MustAddr(Meta2Prefix),
			expStart: Meta1Prefix,
			expEnd:   Meta2Prefix.Next(),
			expError: "",
		},
		{
			key:      Meta2KeyMax,
			expStart: Meta2Prefix,
			expEnd:   Meta2KeyMax.Next(),
			expError: "",
		},
	}
	for i, test := range testCases {
		res, err := MetaReverseScanBounds(roachpb.RKey(test.key))

		if !testutils.IsError(err, test.expError) {
			t.Errorf("expected error %q ; got %v", test.expError, err)
		}

		expected := roachpb.RSpan{Key: test.expStart, EndKey: test.expEnd}
		if !res.Equal(expected) {
			t.Errorf("%d: range bounds %s don't match expected bounds %s for key %s",
				i, res, expected, roachpb.Key(test.key))
		}
	}
}

func TestValidateRangeMetaKey(t *testing.T) {
	testCases := []struct {
		key    []byte
		expErr bool
	}{
		{roachpb.RKeyMin, false},
		{roachpb.RKey("\x00"), true},
		{Meta1Prefix, false},
		{makeKey(Meta1Prefix, roachpb.RKeyMax), false},
		{makeKey(Meta2Prefix, roachpb.RKeyMax), false},
		{makeKey(Meta2Prefix, roachpb.RKeyMax.Next()), true},
	}
	for i, test := range testCases {
		err := validateRangeMetaKey(test.key)
		if err != nil != test.expErr {
			t.Errorf("%d: expected error? %t: %s", i, test.expErr, err)
		}
	}
}

func TestBatchRange(t *testing.T) {
	testCases := []struct {
		req [][2]string
		exp [2]string
	}{
		{
			// Boring single request.
			req: [][2]string{{"a", "b"}},
			exp: [2]string{"a", "b"},
		},
		{
			// Request with invalid range. It's important that this still
			// results in a valid range.
			req: [][2]string{{"b", "a"}},
			exp: [2]string{"b", "b\x00"},
		},
		{
			// Two overlapping ranges.
			req: [][2]string{{"a", "c"}, {"b", "d"}},
			exp: [2]string{"a", "d"},
		},
		{
			// Two disjoint ranges.
			req: [][2]string{{"a", "b"}, {"c", "d"}},
			exp: [2]string{"a", "d"},
		},
		{
			// Range and disjoint point request.
			req: [][2]string{{"a", "b"}, {"c", ""}},
			exp: [2]string{"a", "c\x00"},
		},
		{
			// Three disjoint point requests.
			req: [][2]string{{"a", ""}, {"b", ""}, {"c", ""}},
			exp: [2]string{"a", "c\x00"},
		},
		{
			// Disjoint range request and point request.
			req: [][2]string{{"a", "b"}, {"b", ""}},
			exp: [2]string{"a", "b\x00"},
		},
		{
			// Range-local point request.
			req: [][2]string{{string(RangeDescriptorKey(roachpb.RKeyMax)), ""}},
			exp: [2]string{"\xff\xff", "\xff\xff\x00"},
		},
		{
			// Range-local to global such that the key ordering flips.
			// Important that we get a valid range back.
			req: [][2]string{{string(RangeDescriptorKey(roachpb.RKeyMax)), "x"}},
			exp: [2]string{"\xff\xff", "\xff\xff\x00"},
		},
		{
			// Range-local to global without order messed up.
			req: [][2]string{{string(RangeDescriptorKey(roachpb.RKey("a"))), "x"}},
			exp: [2]string{"a", "x"},
		},
	}

	for i, c := range testCases {
		var ba roachpb.BatchRequest
		for _, pair := range c.req {
			ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{
				Key: roachpb.Key(pair[0]), EndKey: roachpb.Key(pair[1]),
			}})
		}
		if rs, err := Range(ba.Requests); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if actPair := [2]string{string(rs.Key), string(rs.EndKey)}; !reflect.DeepEqual(actPair, c.exp) {
			t.Errorf("%d: expected [%q,%q), got [%q,%q)", i, c.exp[0], c.exp[1], actPair[0], actPair[1])
		}
	}
}

// TestBatchError verifies that Range returns an error if a request has an invalid range.
func TestBatchError(t *testing.T) {
	testCases := []struct {
		req    [2]string
		errMsg string
	}{
		{
			req:    [2]string{"\xff\xff\xff\xff", "a"},
			errMsg: "must be less than KeyMax",
		},
		{
			req:    [2]string{"a", "\xff\xff\xff\xff"},
			errMsg: "must be less than or equal to KeyMax",
		},
	}

	for i, c := range testCases {
		var ba roachpb.BatchRequest
		ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{
			Key: roachpb.Key(c.req[0]), EndKey: roachpb.Key(c.req[1]),
		}})
		if _, err := Range(ba.Requests); !testutils.IsError(err, c.errMsg) {
			t.Errorf("%d: unexpected error %v", i, err)
		}
	}

	// Test a case where a non-range request has an end key.
	var ba roachpb.BatchRequest
	ba.Add(&roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{
		Key: roachpb.Key("a"), EndKey: roachpb.Key("b"),
	}})
	if _, err := Range(ba.Requests); !testutils.IsError(err, "end key specified for non-range operation") {
		t.Errorf("unexpected error %v", err)
	}
}

func TestMakeFamilyKey(t *testing.T) {
	const maxFamID = math.MaxUint32
	key := MakeFamilyKey(nil, maxFamID)
	if expected, n := 6, len(key); expected != n {
		t.Errorf("expected %d bytes, but got %d: [% x]", expected, n, key)
	}
}

func TestEnsureSafeSplitKey(t *testing.T) {
	e := func(vals ...uint64) roachpb.Key {
		var k roachpb.Key
		for _, v := range vals {
			k = encoding.EncodeUvarintAscending(k, v)
		}
		return k
	}

	goodData := []struct {
		in       roachpb.Key
		expected roachpb.Key
	}{
		{e(1, 2, 0), e(1, 2)},          // /Table/1/2/0 -> /Table/1/2
		{e(1, 2, 1), e(1)},             // /Table/1/2/1 -> /Table/1
		{e(1, 2, 2), e()},              // /Table/1/2/2 -> /Table
		{e(1, 2, 3, 0), e(1, 2, 3)},    // /Table/1/2/3/0 -> /Table/1/2/3
		{e(1, 2, 3, 1), e(1, 2)},       // /Table/1/2/3/1 -> /Table/1/2
		{e(1, 2, 200, 2), e(1, 2)},     // /Table/1/2/200/2 -> /Table/1/2
		{e(1, 2, 3, 4, 1), e(1, 2, 3)}, // /Table/1/2/3/4/1 -> /Table/1/2/3
	}
	for i, d := range goodData {
		out, err := EnsureSafeSplitKey(d.in)
		if err != nil {
			t.Fatalf("%d: %s: unexpected error: %v", i, d.in, err)
		}
		if !d.expected.Equal(out) {
			t.Fatalf("%d: %s: expected %s, but got %s", i, d.in, d.expected, out)
		}

		prefixLen, err := GetRowPrefixLength(d.in)
		if err != nil {
			t.Fatalf("%d: %s: unexpected error: %v", i, d.in, err)
		}
		suffix := d.in[prefixLen:]
		expectedSuffix := d.in[len(d.expected):]
		if !bytes.Equal(suffix, expectedSuffix) {
			t.Fatalf("%d: %s: expected %s, but got %s", i, d.in, expectedSuffix, suffix)
		}
	}

	errorData := []struct {
		in  roachpb.Key
		err string
	}{
		// Column ID suffix size is too large.
		{e(1), "malformed table key"},
		{e(1, 2), "malformed table key"},
		// The table ID is invalid.
		{e(200)[:1], "insufficient bytes to decode uvarint value"},
		// The index ID is invalid.
		{e(1, 200)[:2], "insufficient bytes to decode uvarint value"},
		// The column ID suffix is invalid.
		{e(1, 2, 200)[:3], "insufficient bytes to decode uvarint value"},
		// Exercises a former overflow bug. We decode a uint(18446744073709551610) which, if casted
		// to int carelessly, results in -6.
		{encoding.EncodeVarintAscending(MakeTablePrefix(999), 322434), "malformed table key"},
	}
	for i, d := range errorData {
		_, err := EnsureSafeSplitKey(d.in)
		if !testutils.IsError(err, d.err) {
			t.Fatalf("%d: %s: expected %q, but got %v", i, d.in, d.err, err)
		}
	}
}
