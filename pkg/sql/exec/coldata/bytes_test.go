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
	swap
	slice
	copySlice
	appendSlice
	appendVal
)

func (m bytesMethod) String() string {
	switch m {
	case set:
		return "Set"
	case swap:
		return "Swap"
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

var bytesMethods = []bytesMethod{set, swap, slice, copySlice, appendSlice, appendVal}

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
			i := rng.Intn(n)
			new := make([]byte, rng.Intn(16))
			rng.Read(new)
			debugString += fmt.Sprintf("(%d, %v)", i, new)
			b1.Set(i, new)
			b2[i] = new
		case swap:
			i := rng.Intn(n)
			j := rng.Intn(n)
			debugString += fmt.Sprintf("(%d, %d)", i, j)
			b1.Swap(i, j)
			b2[i], b2[j] = b2[j], b2[i]
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
			return errors.Wrap(err, fmt.Sprintf("\ndebugString:\n%sflat (ooo=%t, maxSetIdx=%d):\n%sreference:\n%s", debugString, b1.outOfOrder, b1.maxSetIndex, b1.String(), prettyByteSlice(b2)))
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

	rng, _ := randutil.NewPseudoRand()

	const (
		maxNumberOfCalls = 64
		maxLength        = 16
	)

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
	sourceN := n
	flatSource := flat
	referenceSource := reference
	selfReferencingSources := true
	if rng.Float64() < 0 {
		selfReferencingSources = false
		sourceN = 1 + rng.Intn(maxLength)
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

	if err := verifyEqual(flatSource, referenceSource); err != nil {
		t.Fatalf("sources not equal: %v\nflat:\n%sreference:\n%s", err, flat, prettyByteSlice(reference))
	}

	require.False(t, flat.outOfOrder, "flat was set in order")

	numCalls := 1 + rng.Intn(maxNumberOfCalls)
	methods := make([]bytesMethod, 0, numCalls)
	for i := 0; i < numCalls; i++ {
		methods = append(methods, bytesMethods[rng.Intn(len(bytesMethods))])
	}
	if err := applyMethodsAndVerify(rng, flat, flatSource, reference, referenceSource, methods, selfReferencingSources); err != nil {
		t.Fatal(err)
	}
}
