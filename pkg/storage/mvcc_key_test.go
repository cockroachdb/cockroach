// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
			require.Equal(t, tc.expect == 0, tc.a.Equal(tc.b))
			require.Equal(t, tc.expect < 0, tc.a.Less(tc.b))
			require.Equal(t, tc.expect > 0, tc.b.Less(tc.a))

			// Comparators on encoded keys should be identical.
			aEnc, bEnc := EncodeMVCCKey(tc.a), EncodeMVCCKey(tc.b)
			require.Equal(t, tc.expect, EngineComparer.Compare(aEnc, bEnc))
			require.Equal(t, tc.expect == 0, EngineComparer.Equal(aEnc, bEnc))
		})
	}
}

func TestMVCCKeyCompareRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := func(aGen, bGen randMVCCKey) bool {
		a, b := MVCCKey(aGen), MVCCKey(bGen)
		aEnc, bEnc := EncodeMVCCKey(a), EncodeMVCCKey(b)

		cmp := a.Compare(b)
		cmpEnc := EngineComparer.Compare(aEnc, bEnc)
		eq := a.Equal(b)
		eqEnc := EngineComparer.Equal(aEnc, bEnc)
		lessAB := a.Less(b)
		lessBA := b.Less(a)

		if cmp != cmpEnc {
			t.Logf("cmp (%v) != cmpEnc (%v)", cmp, cmpEnc)
			return false
		}
		if eq != eqEnc {
			t.Logf("eq (%v) != eqEnc (%v)", eq, eqEnc)
			return false
		}
		if (cmp == 0) != eq {
			t.Logf("(cmp == 0) (%v) != eq (%v)", cmp == 0, eq)
			return false
		}
		if (cmp < 0) != lessAB {
			t.Logf("(cmp < 0) (%v) != lessAB (%v)", cmp < 0, lessAB)
			return false
		}
		if (cmp > 0) != lessBA {
			t.Logf("(cmp > 0) (%v) != lessBA (%v)", cmp > 0, lessBA)
			return false
		}
		return true
	}
	require.NoError(t, quick.Check(f, nil))
}

// randMVCCKey is a quick.Generator for MVCCKey.
type randMVCCKey MVCCKey

func (k randMVCCKey) Generate(r *rand.Rand, size int) reflect.Value {
	k.Key = []byte([...]string{"a", "b", "c"}[r.Intn(3)])
	k.Timestamp.WallTime = r.Int63n(5)
	k.Timestamp.Logical = r.Int31n(5)
	return reflect.ValueOf(k)
}

func TestEncodeDecodeMVCCKeyAndTimestampWithLength(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		key     string
		ts      hlc.Timestamp
		encoded string // hexadecimal
	}{
		"empty":                {"", hlc.Timestamp{}, "00"},
		"only key":             {"foo", hlc.Timestamp{}, "666f6f00"},
		"no key":               {"", hlc.Timestamp{WallTime: 1643550788737652545}, "0016cf10bc0505574109"},
		"walltime":             {"foo", hlc.Timestamp{WallTime: 1643550788737652545}, "666f6f0016cf10bc0505574109"},
		"logical":              {"foo", hlc.Timestamp{Logical: 65535}, "666f6f0000000000000000000000ffff0d"},
		"walltime and logical": {"foo", hlc.Timestamp{WallTime: 1643550788737652545, Logical: 65535}, "666f6f0016cf10bc050557410000ffff0d"},
	}

	buf := []byte{}
	for name, tc := range testcases {
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
				EncodedMVCCKeyPrefixLength(mvccKey.Key)+EncodedMVCCTimestampSuffixLength(mvccKey.Timestamp))

			decoded, err := DecodeMVCCKey(encoded)
			require.NoError(t, err)
			require.Equal(t, mvccKey, decoded)

			// Test EncodeMVCCKeyPrefix.
			expectPrefix, err := hex.DecodeString(tc.encoded[:2*len(tc.key)+2])
			require.NoError(t, err)
			require.Equal(t, expectPrefix, EncodeMVCCKeyPrefix(roachpb.Key(tc.key)))
			require.Equal(t, len(expectPrefix), EncodedMVCCKeyPrefixLength(roachpb.Key(tc.key)))

			// Test Encode/DecodeMVCCTimestampSuffix too, since we can trivially do so.
			expectTS, err := hex.DecodeString(tc.encoded[2*len(tc.key)+2:])
			require.NoError(t, err)
			if len(expectTS) == 0 {
				expectTS = nil
			}

			encodedTS := EncodeMVCCTimestampSuffix(tc.ts)
			require.Equal(t, expectTS, encodedTS)
			require.Equal(t, len(encodedTS), EncodedMVCCTimestampSuffixLength(tc.ts))

			decodedTS, err := DecodeMVCCTimestampSuffix(encodedTS)
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

			buf = EncodeMVCCTimestampToBuf(buf, tc.ts)
			if expectTS == nil {
				require.Empty(t, buf)
			} else {
				require.Equal(t, expectTS, buf)
			}
		})
	}
}

func TestDecodeUnnormalizedMVCCKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		encoded       string // hex-encoded
		expected      MVCCKey
		equalToNormal bool
	}{
		"zero logical": {
			encoded:       "666f6f0016cf10bc05055741000000000d",
			expected:      MVCCKey{Key: []byte("foo"), Timestamp: hlc.Timestamp{WallTime: 1643550788737652545, Logical: 0}},
			equalToNormal: true,
		},
		"zero walltime and logical": {
			encoded:  "666f6f000000000000000000000000000d",
			expected: MVCCKey{Key: []byte("foo"), Timestamp: hlc.Timestamp{WallTime: 0, Logical: 0}},
			// We could normalize this form in EngineKeyEqual and EngineKeyCompare,
			// but doing so is not worth losing the fast-path byte comparison between
			// keys that only contain (at most) a walltime.
			equalToNormal: false,
		},
		"synthetic": {
			encoded: "666f6f00000000000000000000000000010e",
			// Synthetic bit set to true when decoded by older versions of the code.
			expected: MVCCKey{Key: []byte("foo"), Timestamp: hlc.Timestamp{WallTime: 0, Logical: 0}},
			// See comment above on "zero walltime and logical".
			equalToNormal: false,
		},
		"walltime and synthetic": {
			encoded: "666f6f0016cf10bc0505574100000000010e",
			// Synthetic bit set to true when decoded by older versions of the code.
			expected:      MVCCKey{Key: []byte("foo"), Timestamp: hlc.Timestamp{WallTime: 1643550788737652545, Logical: 0}},
			equalToNormal: true,
		},
		"logical and synthetic": {
			encoded: "666f6f0000000000000000000000ffff010e",
			// Synthetic bit set to true when decoded by older versions of the code.
			expected:      MVCCKey{Key: []byte("foo"), Timestamp: hlc.Timestamp{WallTime: 0, Logical: 65535}},
			equalToNormal: true,
		},
		"walltime, logical, and synthetic": {
			encoded: "666f6f0016cf10bc050557410000ffff010e",
			// Synthetic bit set to true when decoded by older versions of the code.
			expected:      MVCCKey{Key: []byte("foo"), Timestamp: hlc.Timestamp{WallTime: 1643550788737652545, Logical: 65535}},
			equalToNormal: true,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			encoded, err := hex.DecodeString(tc.encoded)
			require.NoError(t, err)

			decoded, err := DecodeMVCCKey(encoded)
			require.NoError(t, err)
			require.Equal(t, tc.expected, decoded)

			// Re-encode the key into its normal form.
			reencoded := EncodeMVCCKey(decoded)
			require.NotEqual(t, encoded, reencoded)
			require.Equal(t, tc.equalToNormal, EngineComparer.Equal(encoded, reencoded))
			require.Equal(t, tc.equalToNormal, EngineComparer.Compare(encoded, reencoded) == 0)
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
		t.Run(name, func(t *testing.T) {
			encoded, err := hex.DecodeString(tc.encoded)
			require.NoError(t, err)

			_, err = DecodeMVCCTimestampSuffix(encoded)
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
	}
	if testing.Short() {
		// Reduce the number of configurations under -short.
		delete(keys, "empty")
		delete(timestamps, "walltime")
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
	}
	if testing.Short() {
		// Reduce the number of configurations under -short.
		delete(keys, "empty")
		delete(timestamps, "walltime")
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

func TestMVCCRangeKeyClone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	orig := MVCCRangeKeyStack{
		Bounds: roachpb.Span{Key: roachpb.Key("abc"), EndKey: roachpb.Key("def")},
		Versions: MVCCRangeKeyVersions{
			{Timestamp: hlc.Timestamp{WallTime: 5, Logical: 1}, Value: nil,
				EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 5, Logical: 1})},
			{Timestamp: hlc.Timestamp{WallTime: 3, Logical: 4}, Value: []byte{1, 2, 3}},
			{Timestamp: hlc.Timestamp{WallTime: 1, Logical: 2}, Value: nil},
		},
	}

	clone := orig.Clone()
	require.Equal(t, orig, clone)

	// Assert that the slices are actual clones, by asserting the location of the
	// backing array at [0].
	require.NotSame(t, &orig.Bounds.Key[0], &clone.Bounds.Key[0])
	require.NotSame(t, &orig.Bounds.EndKey[0], &clone.Bounds.EndKey[0])
	for i := range orig.Versions {
		if len(orig.Versions[i].Value) > 0 {
			require.NotSame(t, &orig.Versions[i].Value[0], &clone.Versions[i].Value[0])
		}
		if len(orig.Versions[i].EncodedTimestampSuffix) > 0 {
			require.NotSame(t, &orig.Versions[i].EncodedTimestampSuffix[0],
				&clone.Versions[i].EncodedTimestampSuffix[0])
		}
	}
}

func TestMVCCRangeKeyCloneInto(t *testing.T) {
	defer leaktest.AfterTest(t)()

	orig := MVCCRangeKeyStack{
		Bounds: roachpb.Span{Key: roachpb.Key("abc"), EndKey: roachpb.Key("def")},
		Versions: MVCCRangeKeyVersions{
			{Timestamp: hlc.Timestamp{WallTime: 3, Logical: 4}, Value: []byte{1, 2, 3}},
			{Timestamp: hlc.Timestamp{WallTime: 1, Logical: 2}, Value: nil,
				EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 1, Logical: 2})},
		},
	}

	targetEmpty := MVCCRangeKeyStack{}
	targetSmall := MVCCRangeKeyStack{
		Bounds: roachpb.Span{Key: make(roachpb.Key, 1), EndKey: make(roachpb.Key, 1)},
		Versions: MVCCRangeKeyVersions{
			{Value: make([]byte, 1)},
		},
	}
	targetSame := MVCCRangeKeyStack{
		Bounds: roachpb.Span{
			Key:    make(roachpb.Key, len(orig.Bounds.Key)),
			EndKey: make(roachpb.Key, len(orig.Bounds.EndKey))},
		Versions: MVCCRangeKeyVersions{
			{Value: make([]byte, len(orig.Versions[0].Value))},
			{},
		},
	}
	targetLarge := MVCCRangeKeyStack{
		Bounds: roachpb.Span{
			Key:    make(roachpb.Key, len(orig.Bounds.Key)+1),
			EndKey: make(roachpb.Key, len(orig.Bounds.EndKey)+1)},
		Versions: MVCCRangeKeyVersions{
			{Value: make([]byte, len(orig.Versions[0].Value)+1)},
			{Value: make([]byte, len(orig.Versions[1].Value)+1)},
			{Value: make([]byte, 100), EncodedTimestampSuffix: make([]byte, 10)},
		},
	}

	testcases := map[string]struct {
		target       MVCCRangeKeyStack
		expectReused bool
	}{
		"empty": {targetEmpty, false},
		"small": {targetSmall, false},
		"same":  {targetSame, true},
		"large": {targetLarge, true},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "cleared", func(t *testing.T, cleared bool) {
				clone := tc.target
				if cleared {
					clone.Clear()
				}
				orig.CloneInto(&clone)

				// We don't discard empty byte slices when cloning a nil value, so we have
				// to normalize these back to nil for the purpose of comparison.
				for i := range clone.Versions {
					if orig.Versions[i].Value == nil && len(clone.Versions[i].Value) == 0 {
						clone.Versions[i].Value = nil
					}
				}
				require.Equal(t, orig, clone)

				requireSliceIdentity := func(t *testing.T, a, b []byte, expectSame bool) {
					t.Helper()
					a, b = a[:cap(a)], b[:cap(b)]
					if len(a) > 0 {
						if expectSame {
							require.Same(t, &a[0], &b[0])
						} else {
							require.NotSame(t, &a[0], &b[0])
						}
					}
				}

				// Assert that slices are actual clones, by asserting the address of the
				// backing array at [0].
				requireSliceIdentity(t, orig.Bounds.Key, clone.Bounds.Key, false)
				requireSliceIdentity(t, orig.Bounds.EndKey, clone.Bounds.EndKey, false)
				for i := range orig.Versions {
					requireSliceIdentity(t, orig.Versions[i].Value, clone.Versions[i].Value, false)
					requireSliceIdentity(t, orig.Versions[i].EncodedTimestampSuffix, clone.Versions[i].EncodedTimestampSuffix, false)
				}

				// Assert whether the clone is reusing byte slices from the target.
				requireSliceIdentity(t, tc.target.Bounds.Key, clone.Bounds.Key, tc.expectReused)
				requireSliceIdentity(t, tc.target.Bounds.EndKey, clone.Bounds.EndKey, tc.expectReused)
				for i := range tc.target.Versions {
					if i < len(clone.Versions) {
						requireSliceIdentity(t, tc.target.Versions[i].Value, clone.Versions[i].Value, tc.expectReused)
						requireSliceIdentity(t, tc.target.Versions[i].EncodedTimestampSuffix, clone.Versions[i].EncodedTimestampSuffix, tc.expectReused)
					}
				}
			})
		})
	}
}

func TestMVCCRangeKeyString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		rk     MVCCRangeKey
		expect string
	}{
		"empty":           {MVCCRangeKey{}, "/Min"},
		"only start":      {MVCCRangeKey{StartKey: roachpb.Key("foo")}, "foo"},
		"only end":        {MVCCRangeKey{EndKey: roachpb.Key("foo")}, "{/Min-foo}"},
		"only timestamp":  {MVCCRangeKey{Timestamp: hlc.Timestamp{Logical: 1}}, "/Min/0,1"},
		"only span":       {MVCCRangeKey{StartKey: roachpb.Key("a"), EndKey: roachpb.Key("z")}, "{a-z}"},
		"all":             {MVCCRangeKey{StartKey: roachpb.Key("a"), EndKey: roachpb.Key("z"), Timestamp: hlc.Timestamp{Logical: 1}}, "{a-z}/0,1"},
		"all overlapping": {MVCCRangeKey{StartKey: roachpb.Key("ab"), EndKey: roachpb.Key("af"), Timestamp: hlc.Timestamp{Logical: 1}}, "a{b-f}/0,1"},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.rk.String())
		})
	}
}

func TestMVCCRangeKeyCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ab1 := MVCCRangeKey{roachpb.Key("a"), roachpb.Key("b"), hlc.Timestamp{Logical: 1}, nil}
	ac1 := MVCCRangeKey{roachpb.Key("a"), roachpb.Key("c"), hlc.Timestamp{Logical: 1}, nil}
	ac2 := MVCCRangeKey{roachpb.Key("a"), roachpb.Key("c"), hlc.Timestamp{Logical: 2}, nil}
	bc0 := MVCCRangeKey{roachpb.Key("b"), roachpb.Key("c"), hlc.Timestamp{Logical: 0}, nil}
	bc1 := MVCCRangeKey{roachpb.Key("b"), roachpb.Key("c"), hlc.Timestamp{Logical: 1}, nil}
	bc3 := MVCCRangeKey{roachpb.Key("b"), roachpb.Key("c"), hlc.Timestamp{Logical: 3}, nil}
	bd4 := MVCCRangeKey{roachpb.Key("b"), roachpb.Key("d"), hlc.Timestamp{Logical: 4}, nil}
	bd5 := MVCCRangeKey{roachpb.Key("b"), roachpb.Key("d"), hlc.Timestamp{Logical: 4}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 14}}

	testcases := map[string]struct {
		a      MVCCRangeKey
		b      MVCCRangeKey
		expect int
	}{
		"equal":                 {ac1, ac1, 0},
		"start lt":              {ac1, bc1, -1},
		"start gt":              {bc1, ac1, 1},
		"end lt":                {ab1, ac1, -1},
		"end gt":                {ac1, ab1, 1},
		"time lt":               {ac2, ac1, -1}, // MVCC timestamps sort in reverse order
		"time gt":               {ac1, ac2, 1},  // MVCC timestamps sort in reverse order
		"empty time lt set":     {bc0, bc1, -1}, // empty MVCC timestamps sort before non-empty
		"set time gt empty":     {bc1, bc0, 1},  // empty MVCC timestamps sort before non-empty
		"start time precedence": {ac2, bc3, -1}, // a before b, but 3 before 2; key takes precedence
		"time end precedence":   {bd4, bc3, -1}, // c before d, but 4 before 3; time takes precedence
		"ignore encoded ts":     {bd4, bd5, 0},  // encoded timestamp is ignored
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.a.Compare(tc.b))
		})
	}
}

func TestMVCCRangeKeyEncodedSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		rk     MVCCRangeKey
		expect int
	}{
		"empty":         {MVCCRangeKey{}, 2}, // sentinel byte for start and end
		"only start":    {MVCCRangeKey{StartKey: roachpb.Key("foo")}, 5},
		"only end":      {MVCCRangeKey{EndKey: roachpb.Key("foo")}, 5},
		"only walltime": {MVCCRangeKey{Timestamp: hlc.Timestamp{WallTime: 1}}, 11},
		"only logical":  {MVCCRangeKey{Timestamp: hlc.Timestamp{Logical: 1}}, 15},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.rk.EncodedSize())
		})
	}
}

func TestMVCCRangeKeyValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a := roachpb.Key("a")
	b := roachpb.Key("b")
	blank := roachpb.Key("")
	ts1 := hlc.Timestamp{Logical: 1}

	testcases := map[string]struct {
		rangeKey  MVCCRangeKey
		expectErr string // empty if no error
	}{
		"valid":            {MVCCRangeKey{StartKey: a, EndKey: b, Timestamp: ts1}, ""},
		"empty":            {MVCCRangeKey{}, "/Min: no start key"},
		"no start":         {MVCCRangeKey{EndKey: b, Timestamp: ts1}, "{/Min-b}/0,1: no start key"},
		"no end":           {MVCCRangeKey{StartKey: a, Timestamp: ts1}, "a/0,1: no end key"},
		"no timestamp":     {MVCCRangeKey{StartKey: a, EndKey: b}, "{a-b}: no timestamp"},
		"blank start":      {MVCCRangeKey{StartKey: blank, EndKey: b, Timestamp: ts1}, "{/Min-b}/0,1: no start key"},
		"end at start":     {MVCCRangeKey{StartKey: a, EndKey: a, Timestamp: ts1}, `a{-}/0,1: start key "a" is at or after end key "a"`},
		"end before start": {MVCCRangeKey{StartKey: b, EndKey: a, Timestamp: ts1}, `{b-a}/0,1: start key "b" is at or after end key "a"`},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			err := tc.rangeKey.Validate()
			if tc.expectErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErr)
			}
		})
	}
}

func TestMVCCRangeKeyStackCanMergeRight(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		lhs, rhs MVCCRangeKeyStack
		expect   bool
	}{
		"empty stacks don't merge": {
			rangeKeyStack("", "", nil),
			rangeKeyStack("", "", nil),
			false},

		"empty lhs doesn't merge": {
			rangeKeyStack("a", "b", map[int]MVCCValue{}),
			rangeKeyStack("b", "c", map[int]MVCCValue{1: {}}),
			false},

		"empty rhs doesn't merge": {
			rangeKeyStack("a", "b", map[int]MVCCValue{1: {}}),
			rangeKeyStack("b", "c", map[int]MVCCValue{}),
			false},

		"stacks merge": {
			rangeKeyStack("a", "b", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			rangeKeyStack("b", "c", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			true},

		"missing lhs version end": {
			rangeKeyStack("a", "b", map[int]MVCCValue{5: {}, 3: {}}),
			rangeKeyStack("b", "c", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			false},

		"missing rhs version end": {
			rangeKeyStack("a", "b", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			rangeKeyStack("b", "c", map[int]MVCCValue{5: {}, 3: {}}),
			false},

		"different version timestamp": {
			rangeKeyStack("a", "b", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			rangeKeyStack("b", "c", map[int]MVCCValue{5: {}, 2: {}, 1: {}}),
			false},

		"different version value": {
			rangeKeyStack("a", "b", map[int]MVCCValue{5: {}, 3: tombstoneLocalTS(9), 1: {}}),
			rangeKeyStack("b", "c", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			false},

		"bounds not touching": {
			rangeKeyStack("a", "b", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			rangeKeyStack("c", "d", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			false},

		"overlapping range keys don't merge": {
			rangeKeyStack("a", "c", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			rangeKeyStack("b", "d", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			false},

		"same range keys don't merge": {
			rangeKeyStack("a", "c", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			rangeKeyStack("a", "c", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			false},

		"wrong order don't merge": {
			rangeKeyStack("b", "c", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			rangeKeyStack("a", "b", map[int]MVCCValue{5: {}, 3: {}, 1: {}}),
			false},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.lhs.CanMergeRight(tc.rhs))
		})
	}
}

func TestMVCCRangeKeyStackExcise(t *testing.T) {
	defer leaktest.AfterTest(t)()

	initialRangeKeys := rangeKeyStack("a", "f", map[int]MVCCValue{7: {}, 5: {}, 3: {}})

	testcases := []struct {
		from, to    int
		expect      bool
		expectStack []int
	}{
		{0, 10, true, []int{}},
		{10, 0, false, []int{7, 5, 3}}, // wrong order
		{0, 0, false, []int{7, 5, 3}},
		{0, 2, false, []int{7, 5, 3}},
		{8, 9, false, []int{7, 5, 3}},
		{3, 7, true, []int{}},
		{4, 7, true, []int{3}},
		{4, 6, true, []int{7, 3}},
		{5, 6, true, []int{7, 3}},
		{4, 5, true, []int{7, 3}},
		{5, 5, true, []int{7, 3}},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%d,%d", tc.from, tc.to), func(t *testing.T) {
			expect := rangeKeyStack("a", "f", nil)
			for _, ts := range tc.expectStack {
				expect.Versions = append(expect.Versions, rangeKeyVersion(ts, MVCCValue{}))
			}

			rangeKeys := initialRangeKeys.Clone()
			require.Equal(t, tc.expect, rangeKeys.Excise(wallTS(tc.from), wallTS(tc.to)))
			require.Equal(t, expect, rangeKeys)
		})
	}
}

func TestMVCCRangeKeyStackFirstAtOrAbove(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rangeKeys := rangeKeyStack("a", "f", map[int]MVCCValue{6: {}, 4: {}, 3: {}, 1: {}})

	testcases := []struct {
		ts     int
		expect int
	}{
		{0, 1},
		{1, 1},
		{2, 3},
		{3, 3},
		{4, 4},
		{5, 6},
		{6, 6},
		{7, 0},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tc.ts), func(t *testing.T) {
			v, ok := rangeKeys.FirstAtOrAbove(wallTS(tc.ts))
			if tc.expect == 0 {
				require.False(t, ok)
				require.Empty(t, v)
			} else {
				require.True(t, ok)
				require.Equal(t, rangeKeyVersion(tc.expect, MVCCValue{}), v)
			}
		})
	}
}

func TestMVCCRangeKeyStackFirstAtOrBelow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rangeKeys := rangeKeyStack("a", "f", map[int]MVCCValue{6: {}, 4: {}, 3: {}, 1: {}})

	testcases := []struct {
		ts     int
		expect int
	}{
		{0, 0},
		{1, 1},
		{2, 1},
		{3, 3},
		{4, 4},
		{5, 4},
		{6, 6},
		{7, 6},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tc.ts), func(t *testing.T) {
			v, ok := rangeKeys.FirstAtOrBelow(wallTS(tc.ts))
			if tc.expect == 0 {
				require.False(t, ok)
				require.Empty(t, v)
			} else {
				require.True(t, ok)
				require.Equal(t, rangeKeyVersion(tc.expect, MVCCValue{}), v)
			}
		})
	}
}

func TestMVCCRangeKeyStackHasBetween(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rangeKeys := rangeKeyStack("a", "f", map[int]MVCCValue{5: {}, 1: {}})

	testcases := []struct {
		lower, upper int
		expect       bool
	}{
		{0, 0, false},
		{1, 0, false}, // wrong order
		{0, 1, true},
		{1, 1, true},
		{2, 0, false}, // wrong order
		{6, 4, false}, // wrong order
		{4, 6, true},
		{5, 5, true},
		{4, 4, false},
		{6, 6, false},
		{2, 4, false},
		{9, 0, false}, // wrong order
		{0, 9, true},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%d,%d", tc.lower, tc.upper), func(t *testing.T) {
			require.Equal(t, tc.expect, rangeKeys.HasBetween(wallTS(tc.lower), wallTS(tc.upper)))
		})
	}
}

func TestMVCCRangeKeyStackRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()

	initialRangeKeys := rangeKeyStack("a", "f", map[int]MVCCValue{7: {}, 5: {}, 3: {}})

	testcases := []struct {
		ts          int
		expect      bool
		expectStack []int
	}{
		{0, false, []int{7, 5, 3}},
		{2, false, []int{7, 5, 3}},
		{3, true, []int{7, 5}},
		{4, false, []int{7, 5, 3}},
		{5, true, []int{7, 3}},
		{6, false, []int{7, 5, 3}},
		{7, true, []int{5, 3}},
		{8, false, []int{7, 5, 3}},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tc.ts), func(t *testing.T) {
			expect := rangeKeyStack("a", "f", nil)
			for _, ts := range tc.expectStack {
				expect.Versions = append(expect.Versions, rangeKeyVersion(ts, MVCCValue{}))
			}

			rangeKeys := initialRangeKeys.Clone()
			removed, ok := rangeKeys.Remove(wallTS(tc.ts))
			require.Equal(t, tc.expect, ok)
			if ok {
				require.Equal(t, wallTS(tc.ts), removed.Timestamp)
			} else {
				require.Empty(t, removed)
			}

			require.Equal(t, expect, rangeKeys)
		})
	}
}

func TestMVCCRangeKeyStackString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	DisableMetamorphicSimpleValueEncoding(t)

	require.Equal(t, "{a-f}[]",
		rangeKeyStack("a", "f", map[int]MVCCValue{}).String())

	require.Equal(t, "aa{a-f}[]",
		rangeKeyStack("aaa", "aaf", map[int]MVCCValue{}).String())

	require.Equal(t, "{a-f}[0.000000001,0=]",
		rangeKeyStack("a", "f", map[int]MVCCValue{1: {}}).String())

	require.Equal(t, "{a-f}[0.000000007,0= 0.000000005,0= 0.000000003,0=]",
		rangeKeyStack("a", "f", map[int]MVCCValue{7: {}, 5: {}, 3: {}}).String())

	require.Equal(t, "{a-f}[0.000000007,0= 0.000000005,0=00000004650a020809 0.000000003,0=]",
		rangeKeyStack("a", "f", map[int]MVCCValue{7: {}, 5: tombstoneLocalTS(9), 3: {}}).String())
}

func TestMVCCRangeKeyStackTrim(t *testing.T) {
	defer leaktest.AfterTest(t)()

	initialRangeKeys := rangeKeyStack("a", "f", map[int]MVCCValue{7: {}, 5: {}, 3: {}})

	testcases := []struct {
		from, to    int
		expect      bool
		expectStack []int
	}{
		{0, 10, false, []int{7, 5, 3}},
		{10, 0, true, []int{}}, // wrong order
		{0, 0, true, []int{}},
		{0, 2, true, []int{}},
		{8, 9, true, []int{}},
		{3, 7, false, []int{7, 5, 3}},
		{4, 7, true, []int{7, 5}},
		{4, 6, true, []int{5}},
		{5, 6, true, []int{5}},
		{4, 5, true, []int{5}},
		{5, 5, true, []int{5}},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%d,%d", tc.from, tc.to), func(t *testing.T) {
			expect := rangeKeyStack("a", "f", nil)
			for _, ts := range tc.expectStack {
				expect.Versions = append(expect.Versions, rangeKeyVersion(ts, MVCCValue{}))
			}

			rangeKeys := initialRangeKeys.Clone()
			require.Equal(t, tc.expect, rangeKeys.Trim(wallTS(tc.from), wallTS(tc.to)))
			require.Equal(t, expect, rangeKeys)
		})
	}
}

var mvccRangeKeyStackClone MVCCRangeKeyStack

func BenchmarkMVCCRangeKeyStack_Clone(b *testing.B) {
	makeStack := func(keySize, versions, withValues int) MVCCRangeKeyStack {
		const valueSize = 8
		r := randutil.NewTestRandWithSeed(4829418876581)

		var stack MVCCRangeKeyStack
		stack.Bounds.Key = randutil.RandBytes(r, keySize)
		for stack.Bounds.EndKey.Compare(stack.Bounds.Key) <= 0 {
			stack.Bounds.EndKey = randutil.RandBytes(r, keySize)
		}

		for i := 0; i < versions; i++ {
			version := MVCCRangeKeyVersion{Timestamp: hlc.Timestamp{WallTime: r.Int63()}}
			if i < withValues {
				version.Value = randutil.RandBytes(r, valueSize)
			}
			stack.Versions = append(stack.Versions, version)
		}
		sort.Slice(stack.Versions, func(i, j int) bool {
			return stack.Versions[i].Timestamp.Less(stack.Versions[j].Timestamp)
		})
		return stack
	}

	type testCase struct {
		keySize     int
		numVersions int
		withValues  int
	}
	var testCases []testCase

	for _, keySize := range []int{16} {
		for _, numVersions := range []int{1, 3, 10, 100} {
			for _, withValues := range []int{0, 1} {
				testCases = append(testCases, testCase{
					keySize:     keySize,
					numVersions: numVersions,
					withValues:  withValues,
				})
			}
		}
	}

	if testing.Short() {
		// Choose a few configurations for the short version.
		testCases = []testCase{
			{keySize: 16, numVersions: 1, withValues: 0},
			{keySize: 16, numVersions: 3, withValues: 1},
			{keySize: 16, numVersions: 100, withValues: 1},
		}
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"keySize=%d/numVersions=%d/withValues=%d",
			tc.keySize, tc.numVersions, tc.withValues,
		)
		b.Run(name, func(b *testing.B) {
			stack := makeStack(tc.keySize, tc.numVersions, tc.withValues)
			b.Run("Clone", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					mvccRangeKeyStackClone = stack.Clone()
				}
			})
			b.Run("CloneInto", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					stack.CloneInto(&mvccRangeKeyStackClone)
				}
			})
		})
	}
}

// TODO(erikgrinaker): The below should use the testutils/storageutils variants
// instead, but that requires test code to be in storage_test.
func pointKey(key string, ts int) MVCCKey {
	return MVCCKey{Key: roachpb.Key(key), Timestamp: wallTS(ts)}
}

func pointKV(key string, ts int, value string) MVCCKeyValue {
	return MVCCKeyValue{
		Key:   pointKey(key, ts),
		Value: stringValueRaw(value),
	}
}

func rangeKey(start, end string, ts int) MVCCRangeKey {
	return MVCCRangeKey{
		StartKey:               roachpb.Key(start),
		EndKey:                 roachpb.Key(end),
		Timestamp:              wallTS(ts),
		EncodedTimestampSuffix: EncodeMVCCTimestampSuffix(wallTS(ts)),
	}
}

func rangeKV(start, end string, ts int, v MVCCValue) MVCCRangeKeyValue {
	valueBytes, err := EncodeMVCCValue(v)
	if err != nil {
		panic(err)
	}
	if valueBytes == nil {
		valueBytes = []byte{}
	}
	return MVCCRangeKeyValue{
		RangeKey: rangeKey(start, end, ts),
		Value:    valueBytes,
	}
}

func rangeKeyStack(start, end string, versions map[int]MVCCValue) MVCCRangeKeyStack {
	return MVCCRangeKeyStack{
		Bounds:   roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)},
		Versions: rangeKeyVersions(versions),
	}
}

func rangeKeyVersions(v map[int]MVCCValue) MVCCRangeKeyVersions {
	versions := make([]MVCCRangeKeyVersion, len(v))
	var timestamps []int
	for i := range v {
		timestamps = append(timestamps, i)
	}
	sort.Ints(timestamps)
	for i, ts := range timestamps {
		versions[len(versions)-1-i] = rangeKeyVersion(ts, v[ts])
	}
	return versions
}

func rangeKeyVersion(ts int, v MVCCValue) MVCCRangeKeyVersion {
	valueRaw, err := EncodeMVCCValue(v)
	if err != nil {
		panic(err)
	}
	return MVCCRangeKeyVersion{
		Timestamp: wallTS(ts),
		Value:     valueRaw,
	}
}

func wallTS(ts int) hlc.Timestamp {
	return hlc.Timestamp{WallTime: int64(ts)}
}

func wallTSRaw(ts int) []byte {
	return EncodeMVCCTimestampSuffix(wallTS(ts))
}
