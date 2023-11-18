// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2013 Blake Mizerany
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-perks.txt.
// This code originated at github.com/beorn7/perks,
// at tag v1.0.1, SHA 37c8de3658fcb183f997c4e13e8337516ab753e6.

package quantile

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

var (
	Targets = map[float64]float64{
		0.01: 0.001,
		0.10: 0.01,
		0.50: 0.05,
		0.90: 0.01,
		0.99: 0.001,
	}
	TargetsSmallEpsilon = map[float64]float64{
		0.01: 0.0001,
		0.10: 0.001,
		0.50: 0.005,
		0.90: 0.001,
		0.99: 0.0001,
	}
	LowQuantiles  = []float64{0.01, 0.1, 0.5}
	HighQuantiles = []float64{0.99, 0.9, 0.5}
)

const RelativeEpsilon = 0.01

func verifyPercsWithAbsoluteEpsilon(t *testing.T, a []float64, s *Stream) {
	sort.Float64s(a)
	for quantile, epsilon := range Targets {
		n := float64(len(a))
		k := int(quantile * n)
		if k < 1 {
			k = 1
		}
		lower := int((quantile - epsilon) * n)
		if lower < 1 {
			lower = 1
		}
		upper := int(math.Ceil((quantile + epsilon) * n))
		if upper > len(a) {
			upper = len(a)
		}
		w, min, max := a[k-1], a[lower-1], a[upper-1]
		if g := s.Query(quantile, true); g < min || g > max {
			t.Errorf("q=%f: want %v [%f,%f], got %v", quantile, w, min, max, g)
		}
	}
}

func verifyLowPercsWithRelativeEpsilon(t *testing.T, a []float64, s *Stream) {
	sort.Float64s(a)
	for _, qu := range LowQuantiles {
		n := float64(len(a))
		k := int(qu * n)

		lowerRank := int((1 - RelativeEpsilon) * qu * n)
		upperRank := int(math.Ceil((1 + RelativeEpsilon) * qu * n))
		w, min, max := a[k-1], a[lowerRank-1], a[upperRank-1]
		if g := s.Query(qu, true); g < min || g > max {
			t.Errorf("q=%f: want %v [%f,%f], got %v", qu, w, min, max, g)
		}
	}
}

func verifyHighPercsWithRelativeEpsilon(t *testing.T, a []float64, s *Stream) {
	sort.Float64s(a)
	for _, qu := range HighQuantiles {
		n := float64(len(a))
		k := int(qu * n)

		lowerRank := int((1 - (1+RelativeEpsilon)*(1-qu)) * n)
		upperRank := int(math.Ceil((1 - (1-RelativeEpsilon)*(1-qu)) * n))
		w, min, max := a[k-1], a[lowerRank-1], a[upperRank-1]
		if g := s.Query(qu, true); g < min || g > max {
			t.Errorf("q=%f: want %v [%f,%f], got %v", qu, w, min, max, g)
		}
	}
}

func populateStream(s *Stream, randSource *rand.Rand) []float64 {
	a := make([]float64, 0, 1e5+100)
	for i := 0; i < cap(a); i++ {
		v := randSource.NormFloat64()
		// Add 5% asymmetric outliers.
		if i%20 == 0 {
			v = v*v + 1
		}
		s.Insert(v)
		a = append(a, v)
	}
	return a
}

func TestTargetedQuery(t *testing.T) {
	randSource := rand.New(rand.NewSource(42))
	s := NewTargeted(Targets)
	a := populateStream(s, randSource)
	verifyPercsWithAbsoluteEpsilon(t, a, s)
}

func TestTargetedQuerySmallSampleSize(t *testing.T) {
	s := NewTargeted(TargetsSmallEpsilon)
	a := []float64{1, 2, 3, 4, 5}
	for _, v := range a {
		s.Insert(v)
	}
	verifyPercsWithAbsoluteEpsilon(t, a, s)
	// If not yet flushed, results should be precise:
	if !s.flushed() {
		for φ, want := range map[float64]float64{
			0.01: 1,
			0.10: 1,
			0.50: 3,
			0.90: 5,
			0.99: 5,
		} {
			if got := s.Query(φ, true); got != want {
				t.Errorf("want %f for φ=%f, got %f", want, φ, got)
			}
		}
	}
}

func TestLowBiasedQuery(t *testing.T) {
	randSource := rand.New(rand.NewSource(42))
	s := NewLowBiased(RelativeEpsilon)
	a := populateStream(s, randSource)
	verifyLowPercsWithRelativeEpsilon(t, a, s)
}

func TestHighBiasedQuery(t *testing.T) {
	randSource := rand.New(rand.NewSource(42))
	s := NewHighBiased(RelativeEpsilon)
	a := populateStream(s, randSource)
	verifyHighPercsWithRelativeEpsilon(t, a, s)
}

// BrokenTestTargetedMerge is broken, see Merge doc comment.
func BrokenTestTargetedMerge(t *testing.T) {
	randSource := rand.New(rand.NewSource(42))
	s1 := NewTargeted(Targets)
	s2 := NewTargeted(Targets)
	a := populateStream(s1, randSource)
	a = append(a, populateStream(s2, randSource)...)
	s1.Merge(s2.Samples())
	verifyPercsWithAbsoluteEpsilon(t, a, s1)
}

// BrokenTestLowBiasedMerge is broken, see Merge doc comment.
func BrokenTestLowBiasedMerge(t *testing.T) {
	randSource := rand.New(rand.NewSource(42))
	s1 := NewLowBiased(RelativeEpsilon)
	s2 := NewLowBiased(RelativeEpsilon)
	a := populateStream(s1, randSource)
	a = append(a, populateStream(s2, randSource)...)
	s1.Merge(s2.Samples())
	verifyLowPercsWithRelativeEpsilon(t, a, s2)
}

// BrokenTestHighBiasedMerge is broken, see Merge doc comment.
func BrokenTestHighBiasedMerge(t *testing.T) {
	randSource := rand.New(rand.NewSource(42))
	s1 := NewHighBiased(RelativeEpsilon)
	s2 := NewHighBiased(RelativeEpsilon)
	a := populateStream(s1, randSource)
	a = append(a, populateStream(s2, randSource)...)
	s1.Merge(s2.Samples())
	verifyHighPercsWithRelativeEpsilon(t, a, s2)
}

func TestUncompressed(t *testing.T) {
	q := NewTargeted(Targets)
	for i := 100; i > 0; i-- {
		q.Insert(float64(i))
	}
	if g := q.Count(); g != 100 {
		t.Errorf("want count 100, got %d", g)
	}
	// Before compression, Query should have 100% accuracy.
	for quantile := range Targets {
		w := quantile * 100
		if g := q.Query(quantile, true); g != w {
			t.Errorf("want %f, got %f", w, g)
		}
	}
}

func TestUncompressedSamples(t *testing.T) {
	q := NewTargeted(map[float64]float64{0.99: 0.001})
	for i := 1; i <= 100; i++ {
		q.Insert(float64(i))
	}
	if g := q.Samples().Len(); g != 100 {
		t.Errorf("want count 100, got %d", g)
	}
}

func TestUncompressedOne(t *testing.T) {
	q := NewTargeted(map[float64]float64{0.99: 0.01})
	q.Insert(3.14)
	if g := q.Query(0.90, true); g != 3.14 {
		t.Error("want PI, got", g)
	}
}

func TestDefaults(t *testing.T) {
	if g := NewTargeted(map[float64]float64{0.99: 0.001}).Query(0.99, true); g != 0 {
		t.Errorf("want 0, got %f", g)
	}
}

func TestQueryFlush(t *testing.T) {
	q := NewTargeted(map[float64]float64{0.99: 0.001})
	for i := 1; i <= 100; i++ {
		q.Insert(float64(i))
	}
	// A flush after all inserts should make all following `Query`
	// give the same result with shouldFlush true or false.
	q.flush()
	if p := q.Query(0.90, true); p != 91 {
		t.Error("want 91, got", p)
	}
	if p := q.Query(0.90, false); p != 91 {
		t.Error("want 91, got", p)
	}

	// Do an insert without forcing a flush. The Query with
	// shouldFlush false will ignore the new value and return
	// the same result as before.
	q.Insert(float64(101))
	if p := q.Query(0.90, false); p != 91 {
		t.Error("want 91, got", p)
	}
	// The Query with flush will update the value.
	if p := q.Query(0.90, true); p != 92 {
		t.Error("want 92, got", p)
	}
}

func TestByteSize(t *testing.T) {
	// Empty size is nonzero.
	q := NewTargeted(Targets)
	s0 := q.ByteSize()
	if s0 <= 0 {
		t.Errorf("want > 0, got %d", s0)
	}

	// Uncompressed size is greater than empty size.
	for i := cap(q.b); i > 1; i-- {
		q.Insert(float64(i))
	}
	s1 := q.ByteSize()
	if s1 <= s0 {
		t.Errorf("want > %d, got %d", s0, s1)
	}

	// Compressed size is less than uncompressed size.
	q.Insert(float64(42))
	if s := q.ByteSize(); s <= s0 || s1 <= s {
		t.Errorf("want between (%d, %d), got %d", s0, s1, s)
	}
}
