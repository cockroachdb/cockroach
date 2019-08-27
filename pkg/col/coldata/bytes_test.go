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

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type bytesMethod int

const (
	set bytesMethod = iota
	slice
	copySlice
	appendSlice
	appendVal
)

func (m bytesMethod) String() string {
	switch m {
	case set:
		return "Set"
	case slice:
		return "Slice"
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

var bytesMethods = []bytesMethod{set, slice, copySlice, appendSlice, appendVal}

// applyMethodsAndVerify applies the given methods on b1 and a reference
// [][]byte implementation and checks if the results are equal. If
// selfReferencingSources is true, this is an indication by the caller that we
// are testing an edge case where the source for copies/appends refers to the
// destination. In cases where *flatBytes updates itself under the hood, we also
// update the corresponding b2Source to mirror the behavior.
func applyMethodsAndVerify(
	rng *rand.Rand,
	b1, b1Source *flatBytes,
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
	var debugString string
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
			// Can only Set the last index.
			i := b1.Len() - 1
			new := make([]byte, rng.Intn(16))
			rng.Read(new)
			debugString += fmt.Sprintf("(%d, %v)", i, new)
			b1.Set(i, new)
			b2[i] = new
		case slice:
			start := rng.Intn(n)
			end := rng.Intn(n + 1)
			if start > end {
				end = start + 1
			} else if start == end {
				// If start == end, do a noop Slice, otherwise the rest of the methods
				// won't do much (and rng.Intn will panic with an n of 0).
				start = 0
				end = n
			}
			debugString += fmt.Sprintf("(%d, %d)", start, end)
			b1 = b1.Slice(start, end)
			b2 = b2[start:end]
			if selfReferencingSources {
				b2Source = b2
			}
		case copySlice, appendSlice:
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
				if selfReferencingSources {
					b2Source = b2
				}
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
				b2Source = b2
			}
		default:
			return errors.Errorf("unknown method name: %s", m)
		}
		debugString += "\n"
		if err := verifyEqual(b1, b2); err != nil {
			return errors.Wrap(err, fmt.Sprintf("\ndebugString:\n%sflat (maxSetIdx=%d):\n%sreference:\n%s", debugString, b1.maxSetIndex, b1.String(), prettyByteSlice(b2)))
		}
	}
	return nil
}

func verifyEqual(flat *flatBytes, b [][]byte) error {
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

func TestFlatBytesRefImpl(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()

	const (
		maxNumberOfCalls = 64
		maxLength        = 16
	)

	n := 1 + rng.Intn(maxLength)

	flat := newFlatBytes(n)
	reference := make([][]byte, n)
	for i := 0; i < n; i++ {
		v := make([]byte, rng.Intn(16))
		rng.Read(v)
		flat.Set(i, append([]byte(nil), v...))
		reference[i] = append([]byte(nil), v...)
	}

	// Make a pair of sources to copy/append from. Use the destination variables
	// with a certain probability.
	sourceN := n
	flatSource := flat
	referenceSource := reference
	selfReferencingSources := true
	if rng.Float64() < 0.5 {
		selfReferencingSources = false
		sourceN = 1 + rng.Intn(maxLength)
		flatSource = newFlatBytes(sourceN)
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

	if err := verifyEqual(flatSource, referenceSource); err != nil {
		t.Fatalf("sources not equal: %v\nflat:\n%sreference:\n%s", err, flat, prettyByteSlice(reference))
	}

	numCalls := 1 + rng.Intn(maxNumberOfCalls)
	methods := make([]bytesMethod, 0, numCalls)
	for i := 0; i < numCalls; i++ {
		methods = append(methods, bytesMethods[rng.Intn(len(bytesMethods))])
	}
	if err := applyMethodsAndVerify(rng, flat, flatSource, reference, referenceSource, methods, selfReferencingSources); err != nil {
		t.Fatal(err)
	}
}

func TestFlatBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("Simple", func(t *testing.T) {
		b1 := newFlatBytes(0)
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

		// If we Zero the flatBytes, we can Set any index.
		b1.Zero()
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
		b1 := newFlatBytes(0)
		b2 := newFlatBytes(0)
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

		b2 = newFlatBytes(0)
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

	t.Run("Copy", func(t *testing.T) {
		b1 := newFlatBytes(0)
		b2 := newFlatBytes(0)
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

		// Slice b1 to test slicing logic and follow it with testing a full
		// overwrite of only one element.
		b1 = b1.Slice(0, 1)
		require.Equal(t, 1, b1.Len())
		b1.CopySlice(b2, 0, 0, b2.Len())
		require.Equal(t, 1, b1.Len())
		require.Equal(t, "source one", string(b1.Get(0)))

		// Verify a full overwrite with a non-zero source start index.
		b1.CopySlice(b2, 0, 1, b2.Len())
		require.Equal(t, 1, b1.Len())
		require.Equal(t, "source two", string(b1.Get(0)))
	})
}
