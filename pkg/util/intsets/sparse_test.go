// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package intsets

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const (
	// The number of operations to perform for each test configuration.
	numOps = 10000
	// The max size of sets returned by randomSets().
	maxRandSetSize = 20
)

func TestSparse(t *testing.T) {
	for _, minVal := range []int{-100_000, -smallCutoff * 10, -smallCutoff, 0, smallCutoff, smallCutoff * 10} {
		for _, maxVal := range []int{1, smallCutoff, smallCutoff * 10, 100_000} {
			if maxVal <= minVal {
				continue
			}
			t.Run(fmt.Sprintf("%d_%d", minVal, maxVal), func(t *testing.T) {
				t.Parallel() // SAFE FOR TESTING (this comment is for the linter)
				rng, _ := randutil.NewTestRand()
				o := new(oracle)
				s := new(Sparse)

				for i := 0; i < numOps; i++ {
					v := minVal + rng.Intn(maxVal-minVal)
					switch rng.Intn(4) {
					case 0:
						// Add operation.
						o.Add(v)
						s.Add(v)
					case 1:
						// Remove operation.
						o.Remove(v)
						s.Remove(v)
					case 2:
						// Copy operation.
						oCopy := new(oracle)
						oCopy.Copy(o)
						o = oCopy
						sCopy := new(Sparse)
						sCopy.Copy(s)
						s = sCopy
					case 3:
						// Clear operation infrequently.
						if rng.Intn(20) == 0 {
							o.Clear()
							s.Clear()
						}
					}
					validateSparseSet(t, o, s)
				}
			})
		}
	}
}

func TestSparseSetOps(t *testing.T) {
	for _, minVal := range []int{-100_000, -smallCutoff * 10, -smallCutoff, 0, smallCutoff, smallCutoff * 10} {
		for _, maxVal := range []int{1, smallCutoff, smallCutoff * 10, 100_000} {
			if maxVal <= minVal {
				continue
			}
			t.Run(fmt.Sprintf("%d_%d", minVal, maxVal), func(t *testing.T) {
				t.Parallel() // SAFE FOR TESTING (this comment is for the linter)
				rng, _ := randutil.NewTestRand()
				o, s := randomSets(rng, minVal, maxVal)

				for i := 0; i < numOps; i++ {
					oo, so := randomSets(rng, minVal, maxVal)
					// Test boolean methods.
					if o.Intersects(oo) != s.Intersects(so) {
						t.Fatal("expected sparse sets to intersect")
					}
					if o.Equals(oo) != s.Equals(so) {
						t.Fatal("expected sparse sets to intersect")
					}
					if o.SubsetOf(oo) != s.SubsetOf(so) {
						t.Fatal("expected sparse sets to intersect")
					}

					// Perform a set operation.
					switch rng.Intn(4) {
					case 0:
						// Copy operation.
						o.Copy(oo)
						s.Copy(so)
					case 1:
						// UnionWith operation.
						o.UnionWith(oo)
						s.UnionWith(so)
					case 2:
						// IntersectionWith operation.
						o.IntersectionWith(oo)
						s.IntersectionWith(so)
					case 3:
						// DifferenceWith operation.
						o.DifferenceWith(oo)
						s.DifferenceWith(so)
					}
					validateSparseSet(t, o, s)
				}
			})
		}
	}
}

func validateSparseSet(t *testing.T, o *oracle, s *Sparse) {
	if !s.Equals(s) {
		t.Fatal("expected sparse set to equal itself")
	}
	if !s.Empty() && !s.Intersects(s) {
		t.Fatal("expected non-empty sparse set to intersect with itself")
	}
	if !s.SubsetOf(s) {
		t.Fatal("expected sparse set to be subset of itself")
	}
	if o.Len() != s.Len() {
		t.Fatalf("expected sparse set to have len %d, found %d", o.Len(), s.Len())
	}
	if o.Empty() != s.Empty() {
		neg := ""
		if !o.Empty() {
			neg = "not "
		}
		t.Fatalf("expected sparse set to %sbe empty", neg)
	}
	if o.Min() != s.Min() {
		t.Fatalf("expected sparse set to have minimum of %d, found %d", o.Min(), s.Min())
	}
	for i := o.LowerBound(MinInt); i < MaxInt; i = o.LowerBound(i + 1) {
		if !s.Contains(i) {
			t.Fatalf("expected sparse set to contain %d", i)
		}
	}
	for i := s.LowerBound(MinInt); i < MaxInt; i = s.LowerBound(i + 1) {
		if !o.Contains(i) {
			t.Fatalf("expected sparse set to not contain %d", i)
		}
	}
}

func randomSets(rng *rand.Rand, minVal, maxVal int) (*oracle, *Sparse) {
	o := new(oracle)
	s := new(Sparse)
	for i, n := 0, rng.Intn(maxRandSetSize); i < n; i++ {
		v := minVal + rng.Intn(maxVal-minVal)
		o.Add(v)
		s.Add(v)
	}
	return o, s
}
