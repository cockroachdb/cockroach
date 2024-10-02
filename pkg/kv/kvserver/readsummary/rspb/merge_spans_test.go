// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rspb

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestReadSpanMethods(t *testing.T) {
	s := ReadSpan{Key: keyA, EndKey: keyB, Timestamp: ts10, TxnID: uuid1}

	// Test span[readSpanVal] getters.
	require.Equal(t, keyA, s.key())
	require.Equal(t, keyB, s.endKey())
	require.Equal(t, readSpanVal{ts: ts10, txnID: uuid1}, s.value())

	// Test spanPtr[readSpanVal, ReadSpan] setters.
	s.setKey(keyC)
	s.setEndKey(keyD)
	s.setValue(readSpanVal{ts: ts20, txnID: uuid2})
	require.Equal(t, keyC, s.key())
	require.Equal(t, keyD, s.endKey())
	require.Equal(t, readSpanVal{ts: ts20, txnID: uuid2}, s.value())
}

func TestReadSpanValPartialCompareAndRatchet(t *testing.T) {
	v10x := ReadSpan{Timestamp: ts10, TxnID: uuidNil}
	v20a := ReadSpan{Timestamp: ts20, TxnID: uuid1}
	v20b := ReadSpan{Timestamp: ts20, TxnID: uuid2}
	v20x := ReadSpan{Timestamp: ts20, TxnID: uuidNil}

	testCases := []struct {
		a, b       ReadSpan
		expCmp     int
		expCmpOk   bool
		expRatchet ReadSpan
	}{
		{v10x, v10x, 0, true, v10x},
		{v10x, v20a, -1, true, v20a},
		{v10x, v20b, -1, true, v20b},
		{v10x, v20x, -1, true, v20x},
		{v20a, v10x, +1, true, v20a},
		{v20a, v20a, 0, true, v20a},
		{v20a, v20b, 0, false, v20x},
		{v20a, v20x, -1, true, v20x},
		{v20b, v10x, +1, true, v20b},
		{v20b, v20a, 0, false, v20x},
		{v20b, v20b, 0, true, v20b},
		{v20b, v20x, -1, true, v20x},
		{v20x, v10x, +1, true, v20x},
		{v20x, v20a, +1, true, v20x},
		{v20x, v20b, +1, true, v20x},
		{v20x, v20x, 0, true, v20x},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			// Validate test case.
			if tc.expCmpOk {
				if tc.expCmp <= 0 {
					// a <= b.
					require.Equal(t, tc.b, tc.expRatchet)
				} else {
					// a > b.
					require.Equal(t, tc.a, tc.expRatchet)
				}
			}

			// Test partialCompare.
			cmp, cmpOk := tc.a.value().partialCompare(tc.b.value())
			require.Equal(t, tc.expCmpOk, cmpOk)
			require.Equal(t, tc.expCmp, cmp)

			// Test ratchet.
			var ratchet ReadSpan
			ratchet.setValue(tc.a.value().ratchet(tc.b.value()))
			require.Equal(t, tc.expRatchet, ratchet)
		})
	}
}

// mergeFns is a list of mergeSpans implementations to test and compare.
var mergeFns = []struct {
	name string
	fn   func(a, b []ratchetSpan) []ratchetSpan
	// Does the function normalize its output? See normalizeSpans.
	normExps bool
}{
	{
		name:     "complex",
		fn:       mergeSpans[ratchetVal, ratchetSpan, *ratchetSpan],
		normExps: false,
	},
	{
		name:     "simple",
		fn:       mergeSpansSimple,
		normExps: true,
	},
}

func TestMergeSpans(t *testing.T) {
	type testCase struct {
		a, b []ratchetSpan
		exp  []ratchetSpan
	}
	testCases := func() []testCase {
		span := func(key, endKey string, val int) ratchetSpan {
			var keyBytes, endKeyBytes []byte
			keyBytes = []byte(key)
			if len(endKey) != 0 {
				endKeyBytes = []byte(endKey)
			}
			return ratchetSpan{Key: keyBytes, EndKey: endKeyBytes, Value: ratchetVal(val)}
		}
		return []testCase{
			// Simple cases.
			{
				a:   []ratchetSpan{},
				b:   []ratchetSpan{},
				exp: []ratchetSpan{},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{},
				exp: []ratchetSpan{
					span("a", "d", 2),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{
					span("a", "d", 2),
				},
				exp: []ratchetSpan{
					span("a", "d", 2),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{
					span("a", "d", 3),
				},
				exp: []ratchetSpan{
					span("a", "d", 4),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{
					span("a", "c", 1),
				},
				exp: []ratchetSpan{
					span("a", "d", 2),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{
					span("a", "c", 2),
				},
				exp: []ratchetSpan{
					span("a", "d", 2),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{
					span("b", "c", 3),
				},
				exp: []ratchetSpan{
					span("a", "b", 2),
					span("b", "c", 4),
					span("c", "d", 2),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{
					span("b", "", 3),
				},
				exp: []ratchetSpan{
					span("a", "b", 2),
					span("b", "", 4),
					span("b\x00", "d", 2),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{
					span("b", "e", 1),
				},
				exp: []ratchetSpan{
					span("a", "d", 2),
					span("d", "e", 1),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "d", 2),
				},
				b: []ratchetSpan{
					span("b", "e", 2),
				},
				exp: []ratchetSpan{
					span("a", "d", 2),
					span("d", "e", 2),
				},
			},
			// Complex cases.
			{
				a: []ratchetSpan{
					span("a", "c", 2),
					span("c", "i\x00", 2),
				},
				b: []ratchetSpan{
					span("a\x00", "c", 1),
					span("d", "e", 1),
					span("f", "", 2),
					span("g", "h", 2),
					span("i", "", 3),
				},
				exp: []ratchetSpan{
					span("a", "c", 2),
					span("c", "i", 2),
					span("i", "", 4),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "", 4),
					span("c", "e", 1),
					span("f", "h", 2),
				},
				b: []ratchetSpan{
					span("a\x00", "", 4),
					span("b", "m", 3),
				},
				exp: []ratchetSpan{
					span("a", "", 4),
					span("a\x00", "", 4),
					span("b", "f", 3),
					span("f", "h", 4),
					span("h", "m", 3),
				},
			},
			{
				a: []ratchetSpan{
					span("a", "", 1),
					span("b", "d", 3),
					span("d", "", 4),
					span("e", "", 3),
					span("f", "h", 2),
					span("j", "k", 4),
				},
				b: []ratchetSpan{
					span("b", "e", 2),
					span("f", "", 4),
					span("g", "k", 3),
				},
				exp: []ratchetSpan{
					span("a", "", 1),
					span("b", "d", 4),
					span("d", "", 4),
					span("d\x00", "e", 2),
					span("e", "", 3),
					span("f", "", 4),
					span("f\x00", "g", 2),
					span("g", "h", 4),
					span("h", "j", 3),
					span("j", "k", 4),
				},
			},
		}
	}

	for _, fn := range mergeFns {
		t.Run(fn.name, func(t *testing.T) {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
					for _, tc := range testCases() {
						t.Run("", func(t *testing.T) {
							a, b := tc.a, tc.b
							if reverse {
								a, b = b, a
							}
							merged := fn.fn(a, b)
							expSpans := tc.exp
							if fn.normExps {
								expSpans = normalizeSpans(expSpans)
							}
							require.Equal(t, expSpans, merged, "%+v\n!=\n%+v", expSpans, merged)
						})
					}
				})
			}
		})
	}
}

// TestMergeSpansEquivalence is a quickcheck test that verifies that a
// mergeSpans is equivalent to a simpler but less efficient implementation of
// the same function.
func TestMergeSpansEquivalence(t *testing.T) {
	mergeSpansComplexFn := func(a, b []ratchetSpan) []ratchetSpan {
		a, b = prepareRandomSpans(a), prepareRandomSpans(b)
		t.Logf("a = %+v\nb = %+v\n", a, b)
		res := mergeSpans[ratchetVal, ratchetSpan, *ratchetSpan](a, b)
		// The result of mergeSpans is not normalized. Normalize it to match the
		// expected output of mergeSpansSimple.
		res = normalizeSpans(res)
		t.Logf("res(complex) = %+v\n", res)
		return res
	}
	mergeSpansSimpleFn := func(a, b []ratchetSpan) []ratchetSpan {
		a, b = prepareRandomSpans(a), prepareRandomSpans(b)
		res := mergeSpansSimple(a, b)
		t.Logf("res(simple) = %+v\n", res)
		return res
	}
	require.NoError(t, quick.CheckEqual(mergeSpansComplexFn, mergeSpansSimpleFn, nil))
}

// BenchmarkMergeSpans benchmarks the mergeSpans implementations.
//
// Results with go1.21.4 on a Mac with an Apple M1 Pro processor:
//
// name                          time/op
// MergeSpans/complex/n=10-10     540ns ± 3%
// MergeSpans/complex/n=100-10   3.13µs ± 8%
// MergeSpans/complex/n=1000-10  32.0µs ±19%
// MergeSpans/simple/n=10-10     1.82µs ± 3%
// MergeSpans/simple/n=100-10    13.3µs ± 2%
// MergeSpans/simple/n=1000-10    123µs ± 1%
//
// name                          alloc/op
// MergeSpans/complex/n=10-10      920B ± 0%
// MergeSpans/complex/n=100-10   7.20kB ± 0%
// MergeSpans/complex/n=1000-10  65.6kB ± 0%
// MergeSpans/simple/n=10-10     4.15kB ± 0%
// MergeSpans/simple/n=100-10    36.0kB ± 0%
// MergeSpans/simple/n=1000-10    305kB ± 0%
//
// name                          allocs/op
// MergeSpans/complex/n=10-10      1.00 ± 0%
// MergeSpans/complex/n=100-10     1.00 ± 0%
// MergeSpans/complex/n=1000-10    1.00 ± 0%
// MergeSpans/simple/n=10-10       17.0 ± 0%
// MergeSpans/simple/n=100-10      58.0 ± 0%
// MergeSpans/simple/n=1000-10      413 ± 0%
func BenchmarkMergeSpans(b *testing.B) {
	for _, fn := range mergeFns {
		b.Run(fn.name, func(b *testing.B) {
			for _, n := range []int{10, 100, 1000} {
				b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
					r := rand.New(rand.NewSource(0))
					makeSpans := func() []ratchetSpan {
						a := make([]ratchetSpan, n)
						for i := range a {
							a[i] = ratchetSpan{}.generate(r, 4 /* keySize */)
						}
						return prepareRandomSpans(a)
					}
					makeBenchSpans := func() [][]ratchetSpan {
						as := make([][]ratchetSpan, b.N)
						for i := range as {
							as[i] = makeSpans()
						}
						return as
					}
					as, bs := makeBenchSpans(), makeBenchSpans()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_ = fn.fn(as[i], bs[i])
					}
				})
			}
		})
	}
}

// Simple implementations of the mergeSpans type constraints, with random
// generator methods for quickcheck.

// readSpanValue is a simple implementation of ratchetingValue[ratchetVal] which
// takes a value in the range [1, 4], where 2 and 3 are incomparable with one
// another and all other comparisons are as expected.
type ratchetVal int

func (a ratchetVal) assertValid() {
	if a < 1 || a > 4 {
		panic("invalid ratchetVal")
	}
}

func (ratchetVal) generate(r *rand.Rand) ratchetVal {
	v := ratchetVal(r.Intn(4) + 1)
	v.assertValid()
	return v
}

// The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
func (a ratchetVal) partialCompare(b ratchetVal) (int, bool) {
	a.assertValid()
	b.assertValid()
	switch {
	case a == b:
		return 0, true
	case a == 1 || b == 4:
		// a < b.
		return -1, true
	case a == 4 || b == 1:
		// a > b.
		return 1, true
	default:
		// a and b are both in the range [2, 3] but a != b. Cannot compare.
		return 0, false
	}
}

func (a ratchetVal) ratchet(b ratchetVal) ratchetVal {
	a.assertValid()
	b.assertValid()
	switch {
	case a == b:
		return a
	case a == 1 || b == 4:
		// a < b.
		return b
	case a == 4 || b == 1:
		// a > b.
		return a
	default:
		// a and b are both in the range [2, 3] but a != b. Ratchet to 4.
		return 4
	}
}

var _ ratchetingValue[ratchetVal] = ratchetVal(0)

func TestRatchetValPartialCompareAndRatchet(t *testing.T) {
	testCases := []struct {
		a, b       ratchetVal
		expCmp     int
		expCmpOk   bool
		expRatchet ratchetVal
	}{
		{1, 1, 0, true, 1},
		{1, 2, -1, true, 2},
		{1, 3, -1, true, 3},
		{1, 4, -1, true, 4},
		{2, 1, +1, true, 2},
		{2, 2, 0, true, 2},
		{2, 3, 0, false, 4},
		{2, 4, -1, true, 4},
		{3, 1, +1, true, 3},
		{3, 2, 0, false, 4},
		{3, 3, 0, true, 3},
		{3, 4, -1, true, 4},
		{4, 1, +1, true, 4},
		{4, 2, +1, true, 4},
		{4, 3, +1, true, 4},
		{4, 4, 0, true, 4},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			// Validate test case.
			if tc.expCmpOk {
				if tc.expCmp <= 0 {
					// a <= b.
					require.Equal(t, tc.b, tc.expRatchet)
				} else {
					// a > b.
					require.Equal(t, tc.a, tc.expRatchet)
				}
			}

			// Test partialCompare.
			cmp, cmpOk := tc.a.partialCompare(tc.b)
			require.Equal(t, tc.expCmpOk, cmpOk)
			require.Equal(t, tc.expCmp, cmp)

			// Test ratchet.
			ratchet := tc.a.ratchet(tc.b)
			require.Equal(t, tc.expRatchet, ratchet)
		})
	}
}

// randKey is a random key.
type randKey []byte

func (randKey) generate(r *rand.Rand, size int) randKey {
	k := randutil.RandBytes(r, size)
	if r.Intn(2) != 0 {
		// Randomly generate keys that are the direct successor to other keys.
		k = append(k, 0) // Key.Next()
	}
	return k
}

// ratchetSpan is a simple implementation of span[ratchetVal]
type ratchetSpan struct {
	Key    randKey
	EndKey randKey
	Value  ratchetVal
}

func (ratchetSpan) generate(r *rand.Rand, keySize int) ratchetSpan {
	key := randKey{}.generate(r, keySize)
	var endKey randKey
	if r.Intn(2) != 0 {
		endKey = randKey{}.generate(r, keySize)
		cmp := bytes.Compare(key, endKey)
		if cmp == 0 {
			endKey = nil
		} else if cmp > 0 {
			key, endKey = endKey, key
		}
	}
	return ratchetSpan{
		Key:    key,
		EndKey: endKey,
		Value:  ratchetVal(0).generate(r),
	}
}

// Generate implements quick.Generator.
func (ratchetSpan) Generate(r *rand.Rand, size int) reflect.Value {
	return reflect.ValueOf(ratchetSpan{}.generate(r, 1 /* keySize */))
}

// ratchetSpan implements span[readSpanVal].
func (s ratchetSpan) key() []byte       { return []byte(s.Key) }
func (s ratchetSpan) endKey() []byte    { return []byte(s.EndKey) }
func (s ratchetSpan) value() ratchetVal { return s.Value }

var _ span[ratchetVal] = ratchetSpan{}

// *ratchetSpan implements spanPtr[ratchetVal, ratchetSpan].
func (s *ratchetSpan) setKey(k []byte)       { s.Key = k }
func (s *ratchetSpan) setEndKey(k []byte)    { s.EndKey = k }
func (s *ratchetSpan) setValue(v ratchetVal) { s.Value = v }

// Go won't let us perform this compile-time assertion.
// var _ spanPtr[ratchetVal, ratchetSpan] = &ratchetSpan{}

// mergeSpansSimple is a simple, inefficient implementation of mergeSpans, used
// for testing. Like mergeSpans, it merges two ordered slices of ratchetSpan and
// returns a new slice of ordered spans without overlaps.
func mergeSpansSimple(a, b []ratchetSpan) []ratchetSpan {
	// Algorithm:
	// (a) merge the two sorted slices together.
	// (c) re-sort on start key. This will create overlaps.
	// (b) normalize the end keys in each slice, which frees the algorithm from
	//     having to deal with empty end keys.
	// (d) iterate over the spans, merging overlapping portion of spans and
	//     splitting on non-overlapping portions.
	// (e) merge adjacent spans with same value.
	c := append(a, b...)
	c = sortByStartKey(c)
	c = normalizeEndKeys(c)
	var d []ratchetSpan
	for i := 0; i < len(c); i++ {
		next := c[i]
		if len(d) == 0 {
			d = append(d, next)
			continue
		}
		last := &d[len(d)-1]
		if bytes.Equal(last.Key, next.Key) {
			// Same start key, ratchet value over overlapping portions and preserve
			// non-overlapping portions.
			cmp := bytes.Compare(last.EndKey, next.EndKey)
			switch {
			case cmp < 0:
				// last.EndKey < next.EndKey.
				last.Value = last.Value.ratchet(next.Value)
				next.Key = last.EndKey
				d = append(d, next)
			case cmp == 0:
				// last.EndKey == next.EndKey.
				last.Value = last.Value.ratchet(next.Value)
			case cmp > 0:
				// last.EndKey > next.EndKey.
				rest := *last
				rest.Key = next.EndKey
				last.EndKey = next.EndKey
				last.Value = last.Value.ratchet(next.Value)
				d = append(d, rest)
			}
		} else if bytes.Compare(last.EndKey, next.Key) > 0 {
			// Overlapping range with different start key, split last at next.Key and
			// retry. This guarantees we hit the bytes.Equal case above next time.
			splitLast := *last
			last.EndKey = next.Key
			splitLast.Key = next.Key
			d = append(d, splitLast)
			i-- // retry
		} else /* bytes.Compare(last.EndKey, next.Key) <= 0 */ {
			// Non-overlapping.
			d = append(d, next)
		}
	}
	e := mergeAdjacent(d)
	return e
}

// Test utilities to manipulate ratchetSpans.

// sortByStartKey sorts a by start key.
func sortByStartKey(a []ratchetSpan) []ratchetSpan {
	sort.Slice(a, func(i, j int) bool {
		return bytes.Compare(a[i].Key, a[j].Key) < 0
	})
	return a
}

// filterOverlaps removes overlapping spans from a.
// a must be sorted by start key.
func filterOverlaps(a []ratchetSpan) []ratchetSpan {
	var b []ratchetSpan
	for i, next := range a {
		if len(b) == 0 {
			b = append(b, next)
			continue
		}
		last := &b[len(b)-1]
		if bytes.Equal(last.Key, next.Key) {
			continue // skip
		}
		if len(last.EndKey) != 0 && bytes.Compare(last.EndKey, next.Key) > 0 {
			// The previous span overlaps with the next span. Randomly choose to keep
			// one of the two. This has the effect of probabilistically preferring
			// shorter spans, because long spans will be more likely to overlap with
			// many other spans.
			if i%2 == 0 {
				*last = next
			}
			continue // skip
		}
		b = append(b, next)
	}
	return b
}

// prepareRandomSpans is a convenience function that accepts a slice of randomly
// generated spans and prepares them for use with mergeSpans by sorting them and
// removing overlapping segments.
// The function is deterministic and idempotent.
func prepareRandomSpans(a []ratchetSpan) []ratchetSpan {
	a = sortByStartKey(a)
	a = filterOverlaps(a)
	return a
}

// normalizeEndKeys ensures that all spans have a non-empty EndKey.
func normalizeEndKeys(a []ratchetSpan) []ratchetSpan {
	for i := range a {
		if len(a[i].EndKey) == 0 {
			a[i].EndKey = append(a[i].Key, 0) // Key.Next()
		}
	}
	return a
}

// mergeAdjacent merges adjacent spans with the same value.
// a must be sorted by start key, contain no overlapping spans, and have
// non-empty end keys.
func mergeAdjacent(a []ratchetSpan) []ratchetSpan {
	var b []ratchetSpan
	for _, next := range a {
		if len(next.EndKey) == 0 {
			panic("must call normalizeEndKeys first")
		}
		if len(b) == 0 {
			b = append(b, next)
			continue
		}
		last := &b[len(b)-1]
		if bytes.Equal(last.EndKey, next.Key) && last.Value == next.Value {
			last.EndKey = next.EndKey
		} else {
			b = append(b, next)
		}
	}
	return b
}

// normalizeSpans is a convenience function that normalizes a by calling
// normalizeEndKeys and mergeAdjacent. a must be sorted by start key and
// contain no overlapping spans.
func normalizeSpans(a []ratchetSpan) []ratchetSpan {
	a = normalizeEndKeys(a)
	a = mergeAdjacent(a)
	return a
}

// Avoid unused warnings, due to github.com/dominikh/go-tools/issues/1294.
// TODO(nvanbenschoten): remove when staticcheck is updated to support generics.
var (
	_ = readSpanVal.partialCompare
	_ = readSpanVal.ratchet
	_ = ReadSpan.key
	_ = ReadSpan.endKey
	_ = ReadSpan.value
	_ = (*ReadSpan).setKey
	_ = (*ReadSpan).setEndKey
	_ = (*ReadSpan).setValue

	_ = ratchetVal.partialCompare
	_ = ratchetVal.ratchet
	_ = ratchetSpan.key
	_ = ratchetSpan.endKey
	_ = ratchetSpan.value
	_ = (*ratchetSpan).setKey
	_ = (*ratchetSpan).setEndKey
	_ = (*ratchetSpan).setValue
)
