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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// noNulls is a nulls vector with no value set to null.
var noNulls Nulls

// nulls3 is a nulls vector with every third value set to null.
var nulls3 Nulls

// nulls5 is a nulls vector with every fifth value set to null.
var nulls5 Nulls

// nulls10 is a double-length nulls vector with every tenth value set to null.
var nulls10 Nulls

// pos is a collection of interesting boundary indices to use in tests.
var pos []int

func init() {
	pos = []int{0, 1, BatchSize() - 1, BatchSize()}
	for _, possiblePos := range []int{63, 64, 65} {
		if BatchSize() >= possiblePos {
			pos = append(pos, possiblePos)
		}
	}
}

func init() {
	noNulls = NewNulls(BatchSize())
	nulls3 = NewNulls(BatchSize())
	nulls5 = NewNulls(BatchSize())
	nulls10 = NewNulls(BatchSize() * 2)
	for i := 0; i < BatchSize(); i++ {
		if i%3 == 0 {
			nulls3.SetNull(i)
		}
		if i%5 == 0 {
			nulls5.SetNull(i)
		}
	}
	for i := 0; i < BatchSize()*2; i++ {
		if i%10 == 0 {
			nulls10.SetNull(i)
		}
	}
}

func TestNullAt(t *testing.T) {
	for i := 0; i < BatchSize(); i++ {
		if i%3 == 0 {
			require.True(t, nulls3.NullAt(i))
		} else {
			require.False(t, nulls3.NullAt(i))
		}
	}
}

func TestSetNullRange(t *testing.T) {
	for _, start := range pos {
		for _, end := range pos {
			n := NewNulls(BatchSize())
			n.SetNullRange(start, end)
			for i := 0; i < BatchSize(); i++ {
				expected := i >= start && i < end
				require.Equal(t, expected, n.NullAt(i),
					"NullAt(%d) should be %t after SetNullRange(%d, %d)", i, expected, start, end)
			}
		}
	}
}

func TestUnsetNullRange(t *testing.T) {
	for _, start := range pos {
		for _, end := range pos {
			n := NewNulls(BatchSize())
			n.SetNulls()
			n.UnsetNullRange(start, end)
			for i := 0; i < BatchSize(); i++ {
				notExpected := i >= start && i < end
				require.NotEqual(t, notExpected, n.NullAt(i),
					"NullAt(%d) saw %t, expected %t, after SetNullRange(%d, %d)", i, n.NullAt(i), !notExpected, start, end)
			}
		}
	}
}

func TestNullsTruncate(t *testing.T) {
	for _, size := range pos {
		n := NewNulls(BatchSize())
		n.Truncate(size)
		for i := 0; i < BatchSize(); i++ {
			expected := i >= size
			require.Equal(t, expected, n.NullAt(i),
				"NullAt(%d) should be %t after Truncate(%d)", i, expected, size)
		}
	}
}

func TestUnsetNullsAfter(t *testing.T) {
	for _, size := range pos {
		n := NewNulls(BatchSize())
		n.SetNulls()
		n.UnsetNullsAfter(size)
		for i := 0; i < BatchSize(); i++ {
			expected := i < size
			require.Equal(t, expected, n.NullAt(i),
				"NullAt(%d) should be %t after UnsetNullsAfter(%d)", i, expected, size)
		}
	}
}

func TestSetAndUnsetNulls(t *testing.T) {
	n := NewNulls(BatchSize())
	for i := 0; i < BatchSize(); i++ {
		require.False(t, n.NullAt(i))
	}
	n.SetNulls()
	for i := 0; i < BatchSize(); i++ {
		require.True(t, n.NullAt(i))
	}

	for i := 0; i < BatchSize(); i += 3 {
		n.UnsetNull(i)
	}
	for i := 0; i < BatchSize(); i++ {
		if i%3 == 0 {
			require.False(t, n.NullAt(i))
		} else {
			require.True(t, n.NullAt(i))
		}
	}

	n.UnsetNulls()
	for i := 0; i < BatchSize(); i++ {
		require.False(t, n.NullAt(i))
	}
}

func TestNullsSet(t *testing.T) {
	args := SliceArgs{
		// Neither type nor the length here matter.
		Src: NewMemColumn(types.Bool, 0, StandardColumnFactory),
	}
	for _, withSel := range []bool{false, true} {
		t.Run(fmt.Sprintf("WithSel=%t", withSel), func(t *testing.T) {
			var srcNulls *Nulls
			if withSel {
				args.Sel = make([]int, BatchSize())
				// Make a selection vector with every even index. (This turns nulls10 into
				// nulls5.)
				for i := range args.Sel {
					args.Sel[i] = i * 2
				}
				srcNulls = &nulls10
			} else {
				args.Sel = nil
				srcNulls = &nulls5
			}
			for _, destStartIdx := range pos {
				for _, srcStartIdx := range pos {
					for _, srcEndIdx := range pos {
						if destStartIdx <= srcStartIdx && srcStartIdx <= srcEndIdx {
							toAppend := srcEndIdx - srcStartIdx
							name := fmt.Sprintf("destStartIdx=%d,srcStartIdx=%d,toAppend=%d", destStartIdx,
								srcStartIdx, toAppend)
							t.Run(name, func(t *testing.T) {
								n := nulls3.makeCopy()
								args.Src.SetNulls(srcNulls)
								args.DestIdx = destStartIdx
								args.SrcStartIdx = srcStartIdx
								args.SrcEndIdx = srcEndIdx
								// Set some garbage values in the destination nulls that should
								// be overwritten.
								n.SetNullRange(destStartIdx, destStartIdx+toAppend)
								n.set(args)
								for i := 0; i < destStartIdx; i++ {
									require.Equal(t, nulls3.NullAt(i), n.NullAt(i))
								}
								for i := 0; i < toAppend; i++ {
									require.Equal(t, nulls5.NullAt(srcStartIdx+i), n.NullAt(destStartIdx+i))
								}
								for i := destStartIdx + toAppend; i < BatchSize(); i++ {
									require.Equal(t, nulls3.NullAt(i), n.NullAt(i))
								}
							})
						}
					}
				}
			}
		})
	}
}

func TestSlice(t *testing.T) {
	for _, start := range pos {
		for _, end := range pos {
			n := nulls3.Slice(start, end)
			for i := 0; i < 8*len(n.nulls); i++ {
				expected := start+i < end && nulls3.NullAt(start+i)
				require.Equal(t, expected, n.NullAt(i),
					"expected nulls3.Slice(%d, %d).NullAt(%d) to be %b", start, end, i, expected)
			}
		}
	}
	// Ensure we haven't modified the receiver.
	for i := 0; i < BatchSize(); i++ {
		expected := i%3 == 0
		require.Equal(t, expected, nulls3.NullAt(i))
	}
}

func TestNullsOr(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	randomNulls := NewNulls(BatchSize())
	for i := 0; i < BatchSize(); i++ {
		if rng.Float64() < 0.5 {
			randomNulls.SetNull(i)
		}
	}
	nullsToChooseFrom := []Nulls{noNulls, nulls3, nulls5, nulls10, randomNulls}

	length1, length2 := 1+rng.Intn(BatchSize()), 1+rng.Intn(BatchSize())
	n1Choice, n2Choice := rng.Intn(len(nullsToChooseFrom)), rng.Intn(len(nullsToChooseFrom))
	n1 := nullsToChooseFrom[n1Choice].Slice(0, length1)
	n2 := nullsToChooseFrom[n2Choice].Slice(0, length2)
	or := n1.Or(&n2)
	require.Equal(t, or.maybeHasNulls, n1.maybeHasNulls || n2.maybeHasNulls)
	maxLength := length1
	if length2 > length1 {
		maxLength = length2
	}
	for i := 0; i < maxLength; i++ {
		if i < length1 && n1.NullAt(i) || i < length2 && n2.NullAt(i) {
			require.True(t, or.NullAt(i), "or.NullAt(%d) should be true\nn1 %v\nn2 %v\n or %v", i, n1.nulls, n2.nulls, or.nulls)
		} else {
			require.False(t, or.NullAt(i), "or.NullAt(%d) should be false\nn1 %v\nn2 %v\n or %v", i, n1.nulls, n2.nulls, or.nulls)
		}
	}
}
