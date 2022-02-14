// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type bytesMethod int

const (
	set bytesMethod = iota
	window
	copySlice
	appendSlice
	appendVal
)

func (m bytesMethod) String() string {
	switch m {
	case set:
		return "Set"
	case window:
		return "Window"
	case copySlice:
		return "CopySlice"
	case appendSlice:
		return "AppendSlice"
	case appendVal:
		return "AppendVal"
	default:
		panic(fmt.Sprintf("unknown bytes method %d", m))
	}
}

var bytesMethods = []bytesMethod{set, window, copySlice, appendSlice, appendVal}

// applyMethodsAndVerify applies the given methods on b1 and a reference
// [][]byte implementation and checks if the results are equal. If
// selfReferencingSources is true, this is an indication by the caller that we
// are testing an edge case where the source for copies/appends refers to the
// destination. In cases where *Bytes updates itself under the hood, we also
// update the corresponding b2Source to mirror the behavior.
func applyMethodsAndVerify(
	rng *rand.Rand,
	b1, b1Source *Bytes,
	b2, b2Source [][]byte,
	methods []bytesMethod,
	selfReferencingSources bool,
) error {
	if err := verifyEqual(b1, b2); err != nil {
		return errors.Wrap(err, "arguments should start as equal")
	}
	if err := verifyEqual(b1Source, b2Source); err != nil {
		return errors.Wrap(err, "argument sources should start as equal")
	}
	debugString := fmt.Sprintf("\ninitial:\n%s\n", b1)
	for _, m := range methods {
		n := b1.Len()
		if n != len(b2) {
			return errors.Errorf("length mismatch between flat and reference: %d != %d", n, len(b2))
		}
		sourceN := b1Source.Len()
		if sourceN != len(b2Source) {
			return errors.Errorf("length mismatch between flat and reference sources: %d != %d", sourceN, len(b2Source))
		}
		debugString += m.String()
		switch m {
		case set:
			// Can only Set starting from maxSetLength - 1 (or zero if maxSetLength is
			// zero).
			i := b1.maxSetLength - 1 + rng.Intn(b1.Len()-(b1.maxSetLength-1))
			if i < 0 {
				i = 0
			}
			new := make([]byte, rng.Intn(16))
			rng.Read(new)
			debugString += fmt.Sprintf("(%d, %v)", i, new)
			b1.Set(i, new)
			b2[i] = new
		case window:
			start := rng.Intn(n)
			end := rng.Intn(n + 1)
			if start > end {
				end = start + 1
			}
			debugString += fmt.Sprintf("(%d, %d)", start, end)
			b1Window := b1.Window(start, end)
			b2Window := b2[start:end]
			// b1Window is not allowed to be modified, so we check explicitly whether
			// it equals the reference, and we do not update b1 and b2.
			b1Window.AssertOffsetsAreNonDecreasing(b1Window.Len())
			debugString += fmt.Sprintf("\n%s\n", b1Window)
			if err := verifyEqual(b1Window, b2Window); err != nil {
				return errors.Wrapf(err,
					"\ndebugString:\n%s\nflat:\n%s\nreference:\n%s",
					debugString, b1Window.String(), prettyByteSlice(b2Window))
			}
			continue
		case copySlice, appendSlice:
			if m == appendSlice && selfReferencingSources {
				// AppendSlice with selfReferencingSources is only supported
				// when srcStartIdx == srcEndIdx, so we skip this method to
				// avoid whittling down our destination slice.
				continue
			}
			// Generate a length-inclusive destIdx.
			destIdx := rng.Intn(n + 1)
			srcStartIdx := rng.Intn(sourceN)
			srcEndIdx := rng.Intn(sourceN)
			if srcStartIdx > srcEndIdx {
				srcEndIdx = srcStartIdx + 1
			} else if srcStartIdx == srcEndIdx {
				// Avoid whittling down our destination slice.
				srcStartIdx = 0
				srcEndIdx = sourceN
			}
			debugString += fmt.Sprintf("(%d, %d, %d)", destIdx, srcStartIdx, srcEndIdx)
			var numNewVals int
			if m == copySlice {
				b1.CopySlice(b1Source, destIdx, srcStartIdx, srcEndIdx)
				numNewVals = copy(b2[destIdx:], b2Source[srcStartIdx:srcEndIdx])
			} else {
				b1.AppendSlice(b1Source, destIdx, srcStartIdx, srcEndIdx)
				b2 = append(b2[:destIdx], b2Source[srcStartIdx:srcEndIdx]...)
				numNewVals = srcEndIdx - srcStartIdx
			}
			// Deep copy the copied/appended byte slices.
			b2Slice := b2[destIdx : destIdx+numNewVals]
			for i := range b2Slice {
				b2Slice[i] = append([]byte(nil), b2Slice[i]...)
			}
		case appendVal:
			v := make([]byte, 16)
			rng.Read(v)
			debugString += fmt.Sprintf("(%v)", v)
			b1.AppendVal(v)
			b2 = append(b2, v)
			if selfReferencingSources {
				b1Source = b1
				b2Source = b2
			}
		default:
			return errors.Errorf("unknown method name: %s", m)
		}
		b1.AssertOffsetsAreNonDecreasing(b1.Len())
		debugString += fmt.Sprintf("\n%s\n", b1)
		if err := verifyEqual(b1, b2); err != nil {
			return errors.Wrapf(err,
				"\ndebugString:\n%s\nflat (maxSetLength=%d):\n%s\nreference:\n%s",
				debugString, b1.maxSetLength, b1.String(), prettyByteSlice(b2))
		}
	}
	return nil
}

func verifyEqual(flat *Bytes, b [][]byte) error {
	if flat.Len() != len(b) {
		return errors.Errorf("mismatched lengths %d != %d", flat.Len(), len(b))
	}
	for i := range b {
		if !bytes.Equal(b[i], flat.Get(i)) {
			return errors.Errorf("mismatch at index %d", i)
		}
	}
	return nil
}

func prettyByteSlice(b [][]byte) string {
	var builder strings.Builder
	for i := range b {
		builder.WriteString(
			fmt.Sprintf("%d: %v\n", i, b[i]),
		)
	}
	return builder.String()
}

func TestBytesRefImpl(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()

	const (
		maxNumberOfCalls = 64
		maxLength        = 16
		nRuns            = 100
	)

	for nRun := 0; nRun < nRuns; nRun++ {
		n := 1 + rng.Intn(maxLength)

		flat := NewBytes(n)
		reference := make([][]byte, n)
		for i := 0; i < n; i++ {
			v := make([]byte, rng.Intn(16))
			rng.Read(v)
			flat.Set(i, append([]byte(nil), v...))
			reference[i] = append([]byte(nil), v...)
		}

		// Make a pair of sources to copy/append from. Use the destination variables
		// with a certain probability.
		flatSource := flat
		referenceSource := reference
		selfReferencingSources := true
		if rng.Float64() < 0.5 {
			selfReferencingSources = false
			sourceN := 1 + rng.Intn(maxLength)
			flatSource = NewBytes(sourceN)
			referenceSource = make([][]byte, sourceN)
			for i := 0; i < sourceN; i++ {
				v := make([]byte, rng.Intn(16))
				rng.Read(v)
				flatSource.Set(i, append([]byte(nil), v...))
				referenceSource[i] = append([]byte(nil), v...)
			}
		}

		if err := verifyEqual(flat, reference); err != nil {
			t.Fatalf("not equal: %v\nflat:\n%sreference:\n%s", err, flat, prettyByteSlice(reference))
		}

		numCalls := 1 + rng.Intn(maxNumberOfCalls)
		methods := make([]bytesMethod, 0, numCalls)
		for i := 0; i < numCalls; i++ {
			methods = append(methods, bytesMethods[rng.Intn(len(bytesMethods))])
		}
		if err := applyMethodsAndVerify(rng, flat, flatSource, reference, referenceSource, methods, selfReferencingSources); err != nil {
			t.Logf("nRun = %d\n", nRun)
			t.Fatal(err)
		}
	}
}

func TestBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("Simple", func(t *testing.T) {
		b1 := NewBytes(0)
		b1.AppendVal([]byte("hello"))
		require.Equal(t, "hello", string(b1.Get(0)))
		b1.AppendVal(nil)
		require.Equal(t, []byte{}, b1.Get(1))
		require.Equal(t, 2, b1.Len())
		// Verify that we cannot overwrite a value.
		require.Panics(
			t,
			func() { b1.Set(0, []byte("not allowed")) },
			"should be unable to overwrite value",
		)

		// However, it is legal to overwrite the last value.
		b1.Set(1, []byte("ok"))

		// If we Reset the Bytes, we can Set any index.
		b1.Reset()
		b1.Set(1, []byte("new usage"))
		// But not an index before that.
		require.Panics(
			t,
			func() { b1.Set(0, []byte("still not allowed")) },
			"should be unable to overwrite value",
		)

		// Same with Reset.
		b1.Reset()
		b1.Set(1, []byte("reset new usage"))
	})

	t.Run("Append", func(t *testing.T) {
		b1 := NewBytes(0)
		b2 := NewBytes(0)
		b2.AppendVal([]byte("source bytes value"))
		b1.AppendVal([]byte("one"))
		b1.AppendVal([]byte("two"))
		// Truncate b1.
		require.Equal(t, 2, b1.Len())
		b1.AppendSlice(b2, 0, 0, 0)
		require.Equal(t, 0, b1.Len())

		b1.AppendVal([]byte("hello again"))

		// Try appending b2 3 times. The first time will overwrite the current
		// present value in b1.
		for i := 0; i < 3; i++ {
			b1.AppendSlice(b2, i, 0, b2.Len())
			require.Equal(t, i+1, b1.Len())
			for j := 0; j <= i; j++ {
				require.Equal(t, "source bytes value", string(b1.Get(j)))
			}
		}

		b2 = NewBytes(0)
		b2.AppendVal([]byte("hello again"))
		b2.AppendVal([]byte("hello again"))
		b2.AppendVal([]byte("hello again"))
		// Try to append only a subset of the source keeping the first element of
		// b1 intact.
		b1.AppendSlice(b2, 1, 1, 2)
		require.Equal(t, 2, b1.Len())
		require.Equal(t, "source bytes value", string(b1.Get(0)))
		require.Equal(t, "hello again", string(b1.Get(1)))
	})

	t.Run("AppendZeroSlice", func(t *testing.T) {
		// This test makes sure that b.maxSetIndex is updated correctly when we
		// create a long flat bytes vector but not set all the values and then
		// truncate the vector. The expected behavior is that offsets must be
		// backfilled, and once a new value is appended, it should be
		// retrievable.
		b := NewBytes(5)
		b.Set(0, []byte("zero"))
		require.Equal(t, 5, b.Len())
		b.AppendSlice(b, 3, 0, 0)
		require.Equal(t, 3, b.Len())
		b.AppendVal([]byte("three"))
		require.Equal(t, 4, b.Len())
		require.Equal(t, "zero", string(b.Get(0)))
		require.Equal(t, "three", string(b.Get(3)))
		b.AssertOffsetsAreNonDecreasing(b.Len())
	})

	t.Run("Copy", func(t *testing.T) {
		b1 := NewBytes(0)
		b2 := NewBytes(0)
		b1.AppendVal([]byte("one"))
		b1.AppendVal([]byte("two"))
		b1.AppendVal([]byte("three"))

		b2.AppendVal([]byte("source one"))
		b2.AppendVal([]byte("source two"))

		// Copy "source two" into "two"'s position. This also tests that elements
		// following the copied element are correctly shifted.
		b1.CopySlice(b2, 1, 1, 2)
		require.Equal(t, 3, b1.Len())
		require.Equal(t, "one", string(b1.Get(0)))
		require.Equal(t, "source two", string(b1.Get(1)))
		require.Equal(t, "three", string(b1.Get(2)))

		// Copy will only copy as many elements as there is capacity for. In this
		// call, the copy starts at index 2, so there is only capacity for one
		// element.
		b1.CopySlice(b2, 2, 0, b2.Len())
		require.Equal(t, "one", string(b1.Get(0)))
		require.Equal(t, "source two", string(b1.Get(1)))
		require.Equal(t, "source one", string(b1.Get(2)))

		// Set the length to 1 and follow it with testing a full overwrite of
		// only one element.
		b1.offsets = b1.offsets[:2]
		b1.maxSetLength = 1
		require.Equal(t, 1, b1.Len())
		b1.CopySlice(b2, 0, 0, b2.Len())
		require.Equal(t, 1, b1.Len())
		require.Equal(t, "source one", string(b1.Get(0)))

		// Verify a full overwrite with a non-zero source start index.
		b1.CopySlice(b2, 0, 1, b2.Len())
		require.Equal(t, 1, b1.Len())
		require.Equal(t, "source two", string(b1.Get(0)))
	})

	t.Run("Window", func(t *testing.T) {
		b1 := NewBytes(0)
		b1.AppendVal([]byte("one"))
		b1.AppendVal([]byte("two"))
		b1.AppendVal([]byte("three"))

		w := b1.Window(0, 3)
		require.NotEqual(t, unsafe.Pointer(b1), unsafe.Pointer(w), "Bytes.Window should create a new object")
		b2 := b1.Window(1, 2)
		require.Equal(t, "one", string(b1.Get(0)))
		require.Equal(t, "two", string(b1.Get(1)))
		require.Equal(t, "two", string(b2.Get(0)))

		require.Panics(t, func() { b2.AppendVal([]byte("four")) }, "appending to the window into b1 should have panicked")
	})

	t.Run("String", func(t *testing.T) {
		b1 := NewBytes(0)
		vals := [][]byte{
			[]byte("one"),
			[]byte("two"),
			[]byte("three"),
		}
		for i := range vals {
			b1.AppendVal(vals[i])
		}

		// The values should be printed using the String function.
		b1String := b1.String()
		require.True(
			t,
			strings.Contains(b1String, fmt.Sprint(vals[0])) &&
				strings.Contains(b1String, fmt.Sprint(vals[1])) &&
				strings.Contains(b1String, fmt.Sprint(vals[2])),
		)

		// A window on the bytes should only print the values included in the
		// window.
		b2String := b1.Window(1, 3).String()
		require.True(
			t,
			!strings.Contains(b2String, fmt.Sprint(vals[0])) &&
				strings.Contains(b2String, fmt.Sprint(vals[1])) &&
				strings.Contains(b2String, fmt.Sprint(vals[2])),
		)
	})

	t.Run("InvariantSimple", func(t *testing.T) {
		b1 := NewBytes(8)
		b1.Set(0, []byte("zero"))
		other := b1.Window(0, 2)
		other.AssertOffsetsAreNonDecreasing(2)

		b2 := NewBytes(8)
		b2.Set(0, []byte("zero"))
		b2.Set(2, []byte("two"))
		other = b2.Window(0, 4)
		other.AssertOffsetsAreNonDecreasing(4)
	})

	t.Run("Truncate", func(t *testing.T) {
		b := NewBytes(8)
		b.Set(4, []byte("foo"))
		require.Equal(t, 5, b.maxSetLength)
		require.Panics(t, func() { b.Set(1, []byte("bar")) })
		b.Truncate(1)
		require.Equal(t, 1, b.maxSetLength)
		require.NotPanics(t, func() { b.Set(1, []byte("baz")) })
		require.Equal(t, 2, b.maxSetLength)
		b.Truncate(0)
		require.Equal(t, 0, b.maxSetLength)
		require.NotPanics(t, func() { b.Set(4, []byte("deadbeef")) })
	})

	t.Run("Abbreviated", func(t *testing.T) {
		rng, _ := randutil.NewTestRand()

		// Create a vector with random bytes values.
		b := NewBytes(250)
		for i := 0; i < b.Len(); i++ {
			size := rng.Intn(32)
			b.Set(i, randutil.RandBytes(rng, size))
		}

		// Ensure that for every i and j:
		//
		//  - abbr[i] < abbr[j] iff b.Get(i) < b.Get(j)
		//  - abbr[i] > abbr[j] iff b.Get(i) > b.Get(j)
		//
		abbr := b.Abbreviated()
		for i := 0; i < b.Len(); i++ {
			for j := 0; j < b.Len(); j++ {
				cmp := bytes.Compare(b.Get(i), b.Get(j))
				if abbr[i] < abbr[j] && cmp >= 0 {
					t.Errorf("abbr value of %v should not be less than %v", b.Get(i), b.Get(j))
				}
				if abbr[i] > abbr[j] && cmp <= 0 {
					t.Errorf("abbr value of %v should not be greater than %v", b.Get(i), b.Get(j))
				}
			}
		}
	})
}

func TestProportionalSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a large value so that the bytes vector needs to expand.
	value := make([]byte, 3*BytesInitialAllocationFactor)

	rng, _ := randutil.NewTestRand()
	// We need a number divisible by 4.
	fullCapacity := (1 + rng.Intn(100)) * 4
	b := NewBytes(fullCapacity)
	for i := 0; i < fullCapacity; i++ {
		b.Set(i, value)
	}

	fullSize := b.ProportionalSize(int64(fullCapacity))

	// Check that if we ask the size for a half of the capacity, we get the
	// half of full size (modulo a fixed overhead value).
	halfSize := b.ProportionalSize(int64(fullCapacity / 2))
	require.Equal(t, int((fullSize-FlatBytesOverhead)/2), int(halfSize-FlatBytesOverhead))

	// Check that if we create a window for a quarter of the capacity, we get
	// the quarter of full size (modulo a fixed overhead value).
	// Notably we don't start the window from the beginning.
	quarterSize := b.Window(fullCapacity/4, fullCapacity/2).ProportionalSize(int64(fullCapacity / 4))
	require.Equal(t, int(fullSize-FlatBytesOverhead)/4, int(quarterSize-FlatBytesOverhead))
}

const letters = "abcdefghijklmnopqrstuvwxyz"

func TestToArrowSerializationFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()
	nullChance := 0.2
	maxStringLength := 10
	numElements := 1 + rng.Intn(BatchSize())

	b := NewBytes(numElements)
	for i := 0; i < numElements; i++ {
		if rng.Float64() < nullChance {
			continue
		}
		element := []byte(randgen.RandString(rng, 1+rng.Intn(maxStringLength), letters))
		b.Set(i, element)
	}
	// We have to call this in case there are trailing nulls.
	b.UpdateOffsetsToBeNonDecreasing(numElements)

	startIdx := rng.Intn(numElements)
	endIdx := startIdx + rng.Intn(numElements-startIdx)
	if endIdx == startIdx {
		endIdx++
	}
	wind := b.Window(startIdx, endIdx)

	data, offsets := wind.ToArrowSerializationFormat(wind.Len())

	require.Equal(t, wind.Len(), len(offsets)-1)
	require.Equal(t, int32(0), offsets[0])
	require.Equal(t, len(data), int(offsets[len(offsets)-1]))

	// Verify that the offsets maintain the non-decreasing invariant.
	for i := 1; i < len(offsets); i++ {
		require.GreaterOrEqualf(t, offsets[i], offsets[i-1], "unexpectedly found decreasing offsets: %v", offsets)
	}

	// Verify that the data contains the correct values.
	for i := 0; i < len(offsets)-1; i++ {
		element := data[offsets[i]:offsets[i+1]]
		require.Equal(t, wind.Get(i), element)
	}
}

func TestForRegressions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("Regression test for #42054", func(t *testing.T) {
		b := NewBytes(4)
		b.Set(0, []byte("zero"))
		b.Set(1, []byte("one"))
		b.Set(2, []byte("two"))

		// Emulate copying when the first two values are null and the last is
		// non-null.
		b.Reset()
		b.Set(2, []byte("three"))
		b.AssertOffsetsAreNonDecreasing(2)

		b.Reset()
		b.Set(0, []byte("zero"))
		b.Set(1, []byte("one"))
		b.Set(2, []byte("two"))
		b.Set(3, []byte("three"))

		// Emulate copying when the first and last values are non-null, and the
		// middle two are null.
		b.Reset()
		b.Set(0, []byte("zero"))
		b.Set(3, []byte("four"))
		b.AssertOffsetsAreNonDecreasing(3)
	})
}
