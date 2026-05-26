// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"bytes"
	"math"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeKeyRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name string
		key  roachpb.Key
		ts   hlc.Timestamp
	}{
		{name: "empty key, zero ts", key: roachpb.Key{}, ts: hlc.Timestamp{}},
		{name: "single byte", key: roachpb.Key{0x42}, ts: hlc.Timestamp{WallTime: 1}},
		{name: "key with sentinel byte", key: roachpb.Key{0x00, 0xff, 0x00}, ts: hlc.Timestamp{WallTime: 100, Logical: 1}},
		{name: "multi-byte key", key: roachpb.Key("hello/world"), ts: hlc.Timestamp{WallTime: 1714646400000000000, Logical: 5}},
		{name: "large walltime", key: roachpb.Key("k"), ts: hlc.Timestamp{WallTime: math.MaxInt64}},
		{name: "large logical", key: roachpb.Key("k"), ts: hlc.Timestamp{WallTime: 1, Logical: math.MaxInt32}},
		{name: "max both", key: roachpb.Key("k"), ts: hlc.Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc := EncodeKey(tc.key, tc.ts)
			gotKey, gotTS, err := DecodeKey(enc)
			require.NoError(t, err)
			require.Equal(t, tc.key, gotKey)
			require.Equal(t, tc.ts, gotTS)
		})
	}
}

func TestEncodeKeyPrefixIsStrictPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	keys := []roachpb.Key{
		{},
		roachpb.Key("a"),
		roachpb.Key("hello"),
		{0x00, 0xff},
	}
	timestamps := []hlc.Timestamp{
		{},
		{WallTime: 1},
		{WallTime: 1, Logical: 1},
		{WallTime: math.MaxInt64, Logical: math.MaxInt32},
	}
	for _, k := range keys {
		prefix := EncodeKeyPrefix(k)
		for _, ts := range timestamps {
			full := EncodeKey(k, ts)
			require.True(t, bytes.HasPrefix(full, prefix),
				"EncodeKey(%q, %s) should have EncodeKeyPrefix(%q) as a prefix",
				k, ts, k)
			require.Greater(t, len(full), len(prefix),
				"full encoding should be strictly longer than prefix")
		}
	}
}

func TestEncodeKeySortOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type kts struct {
		key roachpb.Key
		ts  hlc.Timestamp
	}
	// Pairs in expected ascending order: user_key asc, then ts asc.
	ordered := []kts{
		{roachpb.Key("a"), hlc.Timestamp{WallTime: 1}},
		{roachpb.Key("a"), hlc.Timestamp{WallTime: 2}},
		{roachpb.Key("a"), hlc.Timestamp{WallTime: 2, Logical: 1}},
		{roachpb.Key("b"), hlc.Timestamp{WallTime: 1}},
		{roachpb.Key("b"), hlc.Timestamp{WallTime: 1, Logical: 5}},
		{roachpb.Key("c"), hlc.Timestamp{WallTime: 0}},
	}
	encoded := make([][]byte, len(ordered))
	for i, o := range ordered {
		encoded[i] = EncodeKey(o.key, o.ts)
	}
	for i := 1; i < len(encoded); i++ {
		require.Negative(t, bytes.Compare(encoded[i-1], encoded[i]),
			"expected encoded[%d] < encoded[%d] for (%s,%s) < (%s,%s)",
			i-1, i, ordered[i-1].key, ordered[i-1].ts, ordered[i].key, ordered[i].ts)
	}
}

func TestDecodeKeyErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name    string
		input   []byte
		wantErr string
	}{
		{name: "empty", input: []byte{}, wantErr: "decoding revlog key user_key"},
		{name: "truncated", input: []byte{0x12, 0x01}, wantErr: "decoding revlog key"},
		{name: "trailing bytes", input: append(EncodeKey(roachpb.Key("k"), hlc.Timestamp{WallTime: 1}), 0xff), wantErr: "trailing bytes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := DecodeKey(tc.input)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestEncodeKeySortOrderShuffled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type kts struct {
		key roachpb.Key
		ts  hlc.Timestamp
	}
	pairs := []kts{
		{roachpb.Key("z"), hlc.Timestamp{WallTime: 100}},
		{roachpb.Key("a"), hlc.Timestamp{WallTime: 1}},
		{roachpb.Key("m"), hlc.Timestamp{WallTime: 50}},
		{roachpb.Key("a"), hlc.Timestamp{WallTime: 2}},
		{roachpb.Key("m"), hlc.Timestamp{WallTime: 10}},
	}
	encoded := make([][]byte, len(pairs))
	for i, p := range pairs {
		encoded[i] = EncodeKey(p.key, p.ts)
	}
	slices.SortFunc(encoded, bytes.Compare)

	// After sorting encoded forms, decoding should yield
	// (user_key asc, walltime asc) order.
	var prev kts
	for i, enc := range encoded {
		k, ts, err := DecodeKey(enc)
		require.NoError(t, err)
		if i > 0 {
			keyCompare := bytes.Compare(prev.key, k)
			if keyCompare == 0 {
				require.True(t, prev.ts.Less(ts),
					"same key %q: expected ts %s < %s", k, prev.ts, ts)
			} else {
				require.Negative(t, keyCompare,
					"expected key %q < %q", prev.key, k)
			}
		}
		prev = kts{k, ts}
	}
}
