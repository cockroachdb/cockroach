// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/stat"
)

func TestRandomOrthoTransformer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		name  string
		vec   vector.T
		other vector.T
	}

	testCases := []testCase{
		{
			name:  "simple 1d",
			vec:   vector.T{1},
			other: vector.T{2},
		},
		{
			name:  "simple 2d",
			vec:   vector.T{1, 2},
			other: vector.T{2, 1},
		},
		{
			name:  "negatives 2d",
			vec:   vector.T{-1, 2},
			other: vector.T{2, -1},
		},
		{
			name:  "unit vectors",
			vec:   vector.T{1, 0},
			other: vector.T{0, 1},
		},
		{
			name:  "simple 8d",
			vec:   vector.T{1, 2, 3, 4, 5, 6, 7, 8},
			other: vector.T{8, 7, 6, 5, 4, 3, 2, 1},
		},
	}

	algos := []struct {
		algo RotAlgorithm
		name string
	}{
		{RotNone, "rotNone"},
		{RotMatrix, "rotMatrix"},
		{RotGivens, "rotGivens"},
	}

	const seed = 42

	for _, tc := range testCases {
		for _, a := range algos {
			t.Run(tc.name+"_"+a.name, func(t *testing.T) {
				dims := len(tc.vec)
				var rot randomOrthoTransformer
				rot.Init(a.algo, dims, seed)

				// Transform the vectors.
				randomized := make(vector.T, dims)
				rot.RandomizeVector(tc.vec, randomized)
				randomizedOther := make(vector.T, dims)
				rot.RandomizeVector(tc.other, randomizedOther)

				// Norms should be preserved.
				origNorm := num32.Norm(tc.vec)
				randomizedNorm := num32.Norm(randomized)
				require.InDelta(t, origNorm, randomizedNorm, 1e-4, "norm not preserved")

				// Pairwise distance should be preserved.
				origDist := num32.L2SquaredDistance(tc.vec, tc.other)
				randomizedDist := num32.L2SquaredDistance(randomized, randomizedOther)
				require.InDelta(t, origDist, randomizedDist, 1e-4, "distance not preserved")

				// rotNone should be a no-op.
				if a.algo == RotNone {
					require.Equal(t, tc.vec, randomized, "rotNone should not change the vector")
				}

				// UnRandomizeVector should recover the original vector.
				orig := make(vector.T, dims)
				rot.UnRandomizeVector(randomized, orig)
				require.InDeltaSlice(t, tc.vec, orig, 1e-4, "inverse did not recover original")
			})
		}
	}
}

func TestRandomOrthoTransformer_SkewedVectors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const dims = 100
	const seed = 42
	const numVecs = 1000

	// Use a seeded RNG for reproducibility.
	rng := rand.New(rand.NewSource(seed))

	// Generate heavily skewed vectors: all dims random in [-1,1], but first dim
	// is scaled by 1000.
	vectors := make([]vector.T, numVecs)
	for i := range vectors {
		vec := make(vector.T, dims)
		for j := range dims {
			vec[j] = 2*rng.Float32() - 1 // Uniform in [-1, 1]
		}
		vec[0] *= 1000
		vectors[i] = vec
	}

	// Calculate the coefficient of variation (CV) of the variance of each
	// dimension in the data vectors. This is a good measure of how well the
	// random orthogonal transformation spreads the input skew across all
	// dimensions.
	calculateCV := func(algo RotAlgorithm) float64 {
		var rot randomOrthoTransformer
		rot.Init(algo, dims, seed)

		// Transform all vectors.
		transformed := make([]vector.T, numVecs)
		for i, original := range vectors {
			randomized := make(vector.T, dims)
			rot.RandomizeVector(original, randomized)
			transformed[i] = randomized
		}

		// Compute variance across each dimension.
		variances := make([]float64, dims)
		scratch := make([]float64, numVecs)
		for i := range variances {
			for j := range transformed {
				scratch[j] = float64(transformed[j][i])
			}
			variances[i] = stat.Variance(scratch, nil /* weights */)
		}

		// Compute the CV, which is stddev divided by mean.
		meanVar := stat.Mean(variances, nil)
		stddevVar := stat.StdDev(variances, nil)

		return stddevVar / meanVar
	}

	// With no rotation, almost all variance is in the first dimension, so CV is
	// very high.
	require.InDelta(t, float32(9.99895), calculateCV(RotNone), 0.0001)

	// With a full random orthogonal matrix, the variance is spread much more
	// evenly.
	require.InDelta(t, float32(1.36733), calculateCV(RotMatrix), 0.0001)

	// With Givens rotations, the variance is spread fairly well, but not as
	// uniformly as with a full matrix. This is a "good enough" reduction at a
	// much lower computational cost (NlogN rather than N^2).
	require.InDelta(t, float32(2.31794), calculateCV(RotGivens), 0.0001)
}
