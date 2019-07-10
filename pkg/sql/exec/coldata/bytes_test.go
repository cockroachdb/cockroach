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

func applyMethodsAndVerify(
	rng *rand.Rand, b1, b1source *Bytes, b2 [][]byte, b2source [][]byte, methods []bytesMethod,
) error {
	if err := verifyEqual(b1, b2); err != nil {
		return errors.Wrap(err, "arguments should start as equal")
	}
	if err := verifyEqual(b1source, b2source); err != nil {
		return errors.Wrap(err, "argument sources should start as equal")
	}
	var debugString string
	for _, m := range methods {
		debugString += m.String()
		n := b1.Len()
		sourceN := b1source.Len()
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
			if m == bytesMethods[4] {
				b1.CopySlice(b1source, destIdx, srcStartIdx, srcEndIdx)
				numNewVals = copy(b2[destIdx:], b2source[srcStartIdx:srcEndIdx])
			} else {
				b1.AppendSlice(b1source, destIdx, srcStartIdx, srcEndIdx)
				b2 = append(b2[:destIdx], b2source[srcStartIdx:srcEndIdx]...)
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

	// Make a pair of sources to copy/append from.
	sourceN := 1 + rng.Intn(maxLength)
	flatSource := NewBytes(sourceN)
	referenceSource := make([][]byte, sourceN)
	for i := 0; i < sourceN; i++ {
		v := make([]byte, rng.Intn(16))
		rng.Read(v)
		flatSource.Set(i, append([]byte(nil), v...))
		referenceSource[i] = append([]byte(nil), v...)
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
	if err := applyMethodsAndVerify(
		rng, flat, flatSource, reference, referenceSource, methods,
	); err != nil {
		t.Fatal(err)
	}
}
