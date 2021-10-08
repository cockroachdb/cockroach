// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddNumericStats(t *testing.T) {
	var a, b, ab NumericStat
	var countA, countB, countAB int64
	var sumA, sumB, sumAB float64

	aData := []float64{1.1, 3.3, 2.2}
	bData := []float64{2.0, 3.0, 5.5, 1.2}

	// Feed some data to A.
	for _, v := range aData {
		countA++
		sumA += v
		a.Record(countA, v)
	}

	// Feed some data to B.
	for _, v := range bData {
		countB++
		sumB += v
		b.Record(countB, v)
	}

	// Feed the A and B data to AB.
	for _, v := range append(bData, aData...) {
		countAB++
		sumAB += v
		ab.Record(countAB, v)
	}

	const epsilon = 0.0000001

	// Sanity check that we have non-trivial stats to combine.
	if mean := 2.2; math.Abs(mean-a.Mean) > epsilon {
		t.Fatalf("Expected Mean %f got %f", mean, a.Mean)
	}
	if mean := sumA / float64(countA); math.Abs(mean-a.Mean) > epsilon {
		t.Fatalf("Expected Mean %f got %f", mean, a.Mean)
	}
	if mean := sumB / float64(countB); math.Abs(mean-b.Mean) > epsilon {
		t.Fatalf("Expected Mean %f got %f", mean, b.Mean)
	}
	if mean := sumAB / float64(countAB); math.Abs(mean-ab.Mean) > epsilon {
		t.Fatalf("Expected Mean %f got %f", mean, ab.Mean)
	}

	// Verify that A+B = AB -- that the stat we get from combining the two is the
	// same as the one that saw the union of values the two saw.
	combined := AddNumericStats(a, b, countA, countB)
	if e := math.Abs(combined.Mean - ab.Mean); e > epsilon {
		t.Fatalf("Mean of combined %f does not match ab %f (%f)", combined.Mean, ab.Mean, e)
	}
	if e := combined.SquaredDiffs - ab.SquaredDiffs; e > epsilon {
		t.Fatalf("SquaredDiffs of combined %f does not match ab %f (%f)", combined.SquaredDiffs, ab.SquaredDiffs, e)
	}

	reversed := AddNumericStats(b, a, countB, countA)
	if combined != reversed {
		t.Fatalf("a+b != b+a: %v vs %v", combined, reversed)
	}

	// Check the in-place side-effect version matches the standalone helper.
	a.Add(b, countA, countB)
	if a != combined {
		t.Fatalf("a.Add(b) should match add(a, b): %+v vs %+v", a, combined)
	}
}

func TestAddExecStats(t *testing.T) {
	numericStatA := NumericStat{Mean: 354.123, SquaredDiffs: 34.34123}
	numericStatB := NumericStat{Mean: 9.34354, SquaredDiffs: 75.321}
	a := ExecStats{Count: 3, NetworkBytes: numericStatA}
	b := ExecStats{Count: 1, NetworkBytes: numericStatB}
	expectedNumericStat := AddNumericStats(a.NetworkBytes, b.NetworkBytes, a.Count, b.Count)
	a.Add(b)
	require.Equal(t, int64(4), a.Count)
	epsilon := 0.00000001
	require.True(t, expectedNumericStat.AlmostEqual(a.NetworkBytes, epsilon), "expected %+v, but found %+v", expectedNumericStat, a.NetworkMessages)
}
