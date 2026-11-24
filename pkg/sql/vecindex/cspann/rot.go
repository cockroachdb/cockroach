// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// givensRotation represents a 2D Givens rotation to be applied to a pair of
// vector elements. The rotation mixes the elements at offset1 and offset2 using
// the provided cosine and sine values, corresponding to a rotation by some
// angle θ in the (offset1, offset2) plane.
//
// A Givens rotation is an orthogonal transformation, so applying this rotation
// (or more generally, a sequence of rotations) to two vectors preserves the
// angles, inner product, and Euclidean distances between them.
type givensRotation struct {
	// offset1 is the index of the first element to rotate.
	offset1 int
	// offset2 is the index of the second element to rotate.
	offset2 int
	// cos is the cosine of the rotation angle.
	cos float32
	// sin is the sine of the rotation angle.
	sin float32
}

// RandomOrthoTransformer applies a random orthogonal transformation (ROT) to
// data and query vectors to mitigate the effects of skewed input data
// distributions:
//
//  1. Set-level skew: some dimensions may have much higher variance than others
//     across the dataset (e.g., one dimension is nearly constant while another
//     varies widely).
//  2. Vector-level skew: individual vectors may have a few disproportionaly
//     large coordinates that dominate the variance.
//
// Applying a ROT redistributes both forms of skew more evenly across all
// dimensions. This leads to more uniform quantization error, as no single
// coordinate dominates information loss. Crucially, ROTs preserve Euclidean
// distances, dot products, and angles — so distance-based comparisons remain
// valid.
//
// Ultimately, performing a random orthogonal transformation means that the
// index will work more consistently across a diversity of input data sets, even
// those with skewed data distributions. In addition, the RaBitQ algorithm
// depends on the statistical properties that are granted by the ROT.
//
// An example is the fashion-mnist-784-euclidean dataset. It consists of 28x28
// greyscale images (flattened to 784 dimensions) of clothing items, with pixel
// values ranging from 0 to 255. Some pixels, especially those near the image
// borders, are zero across almost all images and therefore contain very little
// information. A minority of pixels near the center of the image often contain
// most of the information. A ROT helps to "spread out" the "energy" of the
// vectors across available dimensions, resulting in less overall quantization
// loss. For a case like this, where the original values are only positive, the
// ROT will produce a diverse pattern of positive and negative values, which is
// important for RaBitQ quantization to work well.
//
// Here's an ASCII-art representation of a portion of a fashion image. On the
// left are the original pixels, with many having value zero, but some having
// high values closer to 255. On the right are the transformed pixels, showing
// how the intensity of a few central values is diluted across all available
// pixels.
//
// ╭───────────────╮    ╭───────────────╮
// │           +.  │    │=--.-+-+:-=.==.│
// │         .#@*  │    │.+#-:*:#.:+=-=:│
// │        :%%#   │ => │+--==-#==*==+=*│
// │       .%%%:   │    │=-++++:==+:=*-=│
// │   .:-==###    │    │-:=-::.+=--=+==│
// │.***#*%*###    │    │*:*::*=-=-:==:.│
// ╰───────────────╯    ╰───────────────╯
type RandomOrthoTransformer struct {
	// algo is the algorithm used for the orthogonal transformation.
	algo vecpb.RotAlgorithm
	// dims is the dimensionality of vectors that will be transformed.
	dims int
	// seed is used for pseudo-random number generation, ensuring reproducibility.
	seed int64
	// mat is a square dims x dims orthogonal matrix used to transform input
	// vectors. Used when algo = rotMatrix.
	mat num32.Matrix
	// rotations is the sequence of Givens rotations to apply when
	// algo = rotGivens. Each rotation mixes a pair of coordinates.
	rotations []givensRotation
}

// Init intializes the transformer for the specified algorithm, operating on
// vectors with the given number of dimensions. The same seed must always be
// used for a given vector index, in order to generate the same transforms.
func (t *RandomOrthoTransformer) Init(algo vecpb.RotAlgorithm, dims int, seed int64) {
	*t = RandomOrthoTransformer{
		algo: algo,
		dims: dims,
		seed: seed,
	}

	if algo == vecpb.RotNone {
		// Nothing to prepare if no rotations will be aplied.
		return
	}

	rng := rand.New(rand.NewSource(seed))

	switch algo {
	case vecpb.RotMatrix:
		// Generate a square dims x dims random orthogonal matrix. This will be
		// used to randomize vectors via matrix multiplication.
		t.mat = num32.MakeRandomOrthoMatrix(rng, t.dims)

	case vecpb.RotGivens:
		// Prepare NlogN Givens rotations, where each rotation multiplies a random
		// pair of vector coordinates (x and y) by a 2x2 matrix containing sines
		// and cosines of a random angle θ:
		//
		//  |  cosθ  sinθ |   | x |
		//  | -sinθ  cosθ | * | y |
		//
		// Precompute the random angle and sin/cosine values for each of the
		// NlogN Givens rotations that need to be applied to vectors.
		numRotations := int(math.Ceil(float64(dims) * math.Log2(float64(dims))))
		t.rotations = make([]givensRotation, numRotations)
		for rot := range numRotations {
			offset1 := rng.Intn(dims)
			offset2 := rng.Intn(dims - 1)
			if offset2 >= offset1 {
				offset2++
			}
			theta := rng.Float32() * 2 * math.Pi
			cos, sin := float32(math.Cos(float64(theta))), float32(math.Sin(float64(theta)))
			t.rotations[rot] = givensRotation{
				offset1: offset1, offset2: offset2, cos: cos, sin: sin}
		}
	}
}

// RandomizeVector performs the random orthogonal transformation (ROT) on the
// "original" vector and writes it to the "randomized" vector. The caller is
// responsible for allocating the randomized vector with length equal to the
// original vector.
func (t *RandomOrthoTransformer) RandomizeVector(original vector.T, randomized vector.T) vector.T {
	switch t.algo {
	case vecpb.RotNone:
		// Just copy the original, unchanged vector.
		copy(randomized, original)

	case vecpb.RotMatrix:
		// Multiply the vector by a random orthogonal matrix.
		num32.MulMatrixByVector(&t.mat, original, randomized, num32.NoTranspose)

	case vecpb.RotGivens:
		// Apply NlogN precomputed Givens rotations to the vector.
		copy(randomized, original)
		for i := range t.rotations {
			rot := &t.rotations[i]
			leftVal := randomized[rot.offset1]
			rightVal := randomized[rot.offset2]
			randomized[rot.offset1] = rot.cos*leftVal + rot.sin*rightVal
			randomized[rot.offset2] = -rot.sin*leftVal + rot.cos*rightVal
		}
	}

	return randomized
}

// UnRandomizeVector inverts the random orthogonal transformation performed by
// RandomizeVector, in order to recover the original vector from its randomized
// form. The caller is responsible for allocating the original vector with
// length equal to the randomized vector.
func (t *RandomOrthoTransformer) UnRandomizeVector(
	randomized vector.T, original vector.T,
) vector.T {
	switch t.algo {
	case vecpb.RotNone:
		// The randomized vector is the original vector, so simply copy it.
		copy(original, randomized)

	case vecpb.RotMatrix:
		// Multiply the vector by a random orthogonal matrix.
		num32.MulMatrixByVector(&t.mat, randomized, original, num32.Transpose)

	case vecpb.RotGivens:
		// Reverse previously applied Givens rotations by flipping the sign of
		// the sinθ and applying the rotations in reverse order.
		//
		// Forward rotation:
		//  |  cosθ  sinθ |
		//  | -sinθ  cosθ |
		//
		// Reverse rotation:
		//  | cosθ  -sinθ |
		//  | sinθ   cosθ |
		copy(original, randomized)
		for i := len(t.rotations) - 1; i >= 0; i-- {
			rot := &t.rotations[i]
			leftVal := original[rot.offset1]
			rightVal := original[rot.offset2]
			original[rot.offset1] = rot.cos*leftVal - rot.sin*rightVal
			original[rot.offset2] = rot.sin*leftVal + rot.cos*rightVal
		}
	}

	return original
}
