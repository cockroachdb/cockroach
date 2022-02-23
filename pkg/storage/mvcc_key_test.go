// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/stretchr/testify/require"
)

// Verify the sort ordering of successive keys with metadata and
// versioned values. In particular, the following sequence of keys /
// versions:
//
// a
// a<t=max>
// a<t=1>
// a<t=0>
// a\x00
// a\x00<t=max>
// a\x00<t=1>
// a\x00<t=0>
func TestMVCCKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	aKey := roachpb.Key("a")
	a0Key := roachpb.Key("a\x00")
	keys := mvccKeys{
		mvccKey(aKey),
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: math.MaxInt64}),
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: 1}),
		mvccVersionKey(aKey, hlc.Timestamp{Logical: 1}),
		mvccKey(a0Key),
		mvccVersionKey(a0Key, hlc.Timestamp{WallTime: math.MaxInt64}),
		mvccVersionKey(a0Key, hlc.Timestamp{WallTime: 1}),
		mvccVersionKey(a0Key, hlc.Timestamp{Logical: 1}),
	}
	sortKeys := make(mvccKeys, len(keys))
	copy(sortKeys, keys)
	shuffle.Shuffle(sortKeys)
	sort.Sort(sortKeys)
	if !reflect.DeepEqual(sortKeys, keys) {
		t.Errorf("expected keys to sort in order %s, but got %s", keys, sortKeys)
	}
}

func TestMVCCKeyCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a1 := MVCCKey{roachpb.Key("a"), hlc.Timestamp{Logical: 1}}
	a2 := MVCCKey{roachpb.Key("a"), hlc.Timestamp{Logical: 2}}
	b0 := MVCCKey{roachpb.Key("b"), hlc.Timestamp{Logical: 0}}
	b1 := MVCCKey{roachpb.Key("b"), hlc.Timestamp{Logical: 1}}
	b2 := MVCCKey{roachpb.Key("b"), hlc.Timestamp{Logical: 2}}

	testcases := map[string]struct {
		a      MVCCKey
		b      MVCCKey
		expect int
	}{
		"equal":               {a1, a1, 0},
		"key lt":              {a1, b1, -1},
		"key gt":              {b1, a1, 1},
		"time lt":             {a2, a1, -1}, // MVCC timestamps sort in reverse order
		"time gt":             {a1, a2, 1},  // MVCC timestamps sort in reverse order
		"empty time lt set":   {b0, b1, -1}, // empty MVCC timestamps sort before non-empty
		"set time gt empty":   {b1, b0, 1},  // empty MVCC timestamps sort before non-empty
		"key time precedence": {a1, b2, -1}, // a before b, but 2 before 1; key takes precedence
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.a.Compare(tc.b))
		})
	}
}

func TestEncodeDecodeMVCCKeyAndTimestampWithLength(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		key     string
		ts      hlc.Timestamp
		encoded string // hexadecimal
	}{
		"empty":                  {"", hlc.Timestamp{}, "00"},
		"only key":               {"foo", hlc.Timestamp{}, "666f6f00"},
		"no key":                 {"", hlc.Timestamp{WallTime: 1643550788737652545}, "0016cf10bc0505574109"},
		"walltime":               {"foo", hlc.Timestamp{WallTime: 1643550788737652545}, "666f6f0016cf10bc0505574109"},
		"logical":                {"foo", hlc.Timestamp{Logical: 65535}, "666f6f0000000000000000000000ffff0d"},
		"synthetic":              {"foo", hlc.Timestamp{Synthetic: true}, "666f6f00000000000000000000000000010e"},
		"walltime and logical":   {"foo", hlc.Timestamp{WallTime: 1643550788737652545, Logical: 65535}, "666f6f0016cf10bc050557410000ffff0d"},
		"walltime and synthetic": {"foo", hlc.Timestamp{WallTime: 1643550788737652545, Synthetic: true}, "666f6f0016cf10bc0505574100000000010e"},
		"logical and synthetic":  {"foo", hlc.Timestamp{Logical: 65535, Synthetic: true}, "666f6f0000000000000000000000ffff010e"},
		"all":                    {"foo", hlc.Timestamp{WallTime: 1643550788737652545, Logical: 65535, Synthetic: true}, "666f6f0016cf10bc050557410000ffff010e"},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {

			// Test Encode/DecodeMVCCKey.
			expect, err := hex.DecodeString(tc.encoded)
			require.NoError(t, err)
			if len(expect) == 0 {
				expect = nil
			}

			mvccKey := MVCCKey{Key: []byte(tc.key), Timestamp: tc.ts}

			encoded := EncodeMVCCKey(mvccKey)
			require.Equal(t, expect, encoded)
			require.Equal(t, len(encoded), encodedMVCCKeyLength(mvccKey))
			require.Equal(t, len(encoded),
				encodedMVCCKeyPrefixLength(mvccKey.Key)+encodedMVCCTimestampSuffixLength(mvccKey.Timestamp))

			decoded, err := DecodeMVCCKey(encoded)
			require.NoError(t, err)
			require.Equal(t, mvccKey, decoded)

			// Test EncodeMVCCKeyPrefix.
			expectPrefix, err := hex.DecodeString(tc.encoded[:2*len(tc.key)+2])
			require.NoError(t, err)
			require.Equal(t, expectPrefix, EncodeMVCCKeyPrefix(roachpb.Key(tc.key)))
			require.Equal(t, len(expectPrefix), encodedMVCCKeyPrefixLength(roachpb.Key(tc.key)))

			// Test encode/decodeMVCCTimestampSuffix too, since we can trivially do so.
			expectTS, err := hex.DecodeString(tc.encoded[2*len(tc.key)+2:])
			require.NoError(t, err)
			if len(expectTS) == 0 {
				expectTS = nil
			}

			encodedTS := EncodeMVCCTimestampSuffix(tc.ts)
			require.Equal(t, expectTS, encodedTS)
			require.Equal(t, len(encodedTS), encodedMVCCTimestampSuffixLength(tc.ts))

			decodedTS, err := decodeMVCCTimestampSuffix(encodedTS)
			require.NoError(t, err)
			require.Equal(t, tc.ts, decodedTS)

			// Test encode/decodeMVCCTimestamp as well, for completeness.
			if len(expectTS) > 0 {
				expectTS = expectTS[:len(expectTS)-1]
			}

			encodedTS = encodeMVCCTimestamp(tc.ts)
			require.Equal(t, expectTS, encodedTS)
			require.Equal(t, len(encodedTS), encodedMVCCTimestampLength(tc.ts))

			decodedTS, err = decodeMVCCTimestamp(encodedTS)
			require.NoError(t, err)
			require.Equal(t, tc.ts, decodedTS)
		})
	}
}

func TestDecodeMVCCKeyErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		encoded   string // hex-encoded
		expectErr string
	}{
		"empty input":                     {"", "invalid encoded mvcc key: "},
		"lone length suffix":              {"01", "invalid encoded mvcc key: "},
		"invalid timestamp length":        {"ab00ffff03", "invalid encoded mvcc key: ab00ffff03 bad timestamp ffff"},
		"invalid timestamp length suffix": {"ab00ffffffffffffffff0f", "invalid encoded mvcc key: ab00ffffffffffffffff0f"},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			encoded, err := hex.DecodeString(tc.encoded)
			require.NoError(t, err)

			_, err = DecodeMVCCKey(encoded)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectErr)
		})
	}
}

func TestDecodeMVCCTimestampSuffixErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		encoded   string // hex-encoded
		expectErr string
	}{
		"invalid length":        {"ffff03", "bad timestamp ffff"},
		"invalid length suffix": {"ffffffffffffffff0f", "bad timestamp: found length suffix 15, actual length 9"},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			encoded, err := hex.DecodeString(tc.encoded)
			require.NoError(t, err)

			_, err = decodeMVCCTimestampSuffix(encoded)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectErr)
		})
	}
}

var benchmarkEncodeMVCCKeyResult []byte

func BenchmarkEncodeMVCCKey(b *testing.B) {
	keys := map[string][]byte{
		"empty": {},
		"short": []byte("foo"),
		"long":  bytes.Repeat([]byte{1}, 4096),
	}
	timestamps := map[string]hlc.Timestamp{
		"empty":            {},
		"walltime":         {WallTime: 1643550788737652545},
		"walltime+logical": {WallTime: 1643550788737652545, Logical: 4096},
		"all":              {WallTime: 1643550788737652545, Logical: 4096, Synthetic: true},
	}
	buf := make([]byte, 0, 65536)
	for keyDesc, key := range keys {
		for tsDesc, ts := range timestamps {
			mvccKey := MVCCKey{Key: key, Timestamp: ts}
			b.Run(fmt.Sprintf("key=%s/ts=%s", keyDesc, tsDesc), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					EncodeMVCCKeyToBuf(buf, mvccKey)
				}
			})
		}
	}
	benchmarkEncodeMVCCKeyResult = buf // avoid compiler optimizing away function call
}

var benchmarkDecodeMVCCKeyResult MVCCKey

func BenchmarkDecodeMVCCKey(b *testing.B) {
	keys := map[string][]byte{
		"empty": {},
		"short": []byte("foo"),
		"long":  bytes.Repeat([]byte{1}, 4096),
	}
	timestamps := map[string]hlc.Timestamp{
		"empty":            {},
		"walltime":         {WallTime: 1643550788737652545},
		"walltime+logical": {WallTime: 1643550788737652545, Logical: 4096},
		"all":              {WallTime: 1643550788737652545, Logical: 4096, Synthetic: true},
	}
	var mvccKey MVCCKey
	var err error
	for keyDesc, key := range keys {
		for tsDesc, ts := range timestamps {
			encoded := EncodeMVCCKey(MVCCKey{Key: key, Timestamp: ts})
			b.Run(fmt.Sprintf("key=%s/ts=%s", keyDesc, tsDesc), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					mvccKey, err = DecodeMVCCKey(encoded)
					if err != nil { // for performance
						require.NoError(b, err)
					}
				}
			})
		}
	}
	benchmarkDecodeMVCCKeyResult = mvccKey // avoid compiler optimizing away function call
}
