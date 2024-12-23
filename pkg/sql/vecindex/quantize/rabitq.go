// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"context"
	"math"
	"math/bits"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// RaBitQuantizer quantizes vectors according to the algorithm described in this
// paper:
//
//	"RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound
//	for Approximate Nearest Neighbor Search" by Jianyang Gao & Cheng Long.
//	URL: https://arxiv.org/pdf/2405.12497
//
// The RaBitQ quantization method provides good accuracy, produces compact
// codes, provides practical error bounds, is easy to implement, and can be
// accelerated with fast SIMD instructions. RaBitQ quantization codes use only
// 1 bit per dimension in the original vector.
//
// All methods in RaBitQuantizer are thread-safe. It is intended to be cached
// on a per-process basis and reused across all threads that query the same
// vector index. This is important, because the ROT matrix is expensive to
// generate and can use quite a bit of memory.
type RaBitQuantizer struct {
	// dims is the dimensionality of vectors that can be quantized.
	dims int
	// sqrtDims is the precomputed square root of the "dims" field.
	sqrtDims float32
	// sqrtDimsInv precomputes "1 / sqrtDims".
	sqrtDimsInv float32
	// rot is a square dims x dims matrix that performs random orthogonal
	// transformations on input vectors, in order to distribute skew more evenly.
	rot num32.Matrix
	// unbias is a precomputed slice of "dims" random values in the [0, 1)
	// interval that's used to remove bias when quantizing query vectors.
	unbias []float32
}

var _ Quantizer = (*RaBitQuantizer)(nil)

// NewRaBitQuantizer returns a new RaBitQ quantizer that quantizes vectors with
// the given number of dimensions. The provided seed is used to generate the
// pseudo-random values used by the algorithm. It's important that the quantizer
// is created with the same seed that was previously used to create any
// quantized sets that need to be searched or updated.
func NewRaBitQuantizer(dims int, seed int64) Quantizer {
	if dims <= 0 {
		panic(errors.AssertionFailedf("dimensions are not positive: %d", dims))
	}

	rng := rand.New(rand.NewSource(seed))

	// Generate dims x dims random orthogonal matrix to mitigate the impact of
	// skewed input data distributions:
	//
	//   1. Set skew: some dimensions can have higher variance than others. For
	//      example, perhaps all vectors in a set have similar values for one
	//      dimension but widely differing values in another dimension.
	//   2. Vector skew: Individual vectors can have internal skew, such that
	//      values higher than the mean are more spread out than values lower
	//      than the mean.
	//
	// Multiplying vectors by this matrix helps with both forms of skew. While
	// total skew does not change, the skew is more evenly distributed across
	// the dimensions. Now quantizing the vector will have more uniform
	// information loss across dimensions. Critically, none of this impacts
	// distance calculations, as orthogonal transformations do not change
	// distances or angles between vectors.
	//
	// Ultimately, performing a random orthogonal transformation (ROT) means that
	// the index will work more consistently across a diversity of input data
	// sets, even those with skewed data distributions. In addition, the RaBitQ
	// algorithm depends on the statistical properties that are granted by the
	// ROT.
	rot := num32.MakeRandomOrthoMatrix(rng, dims)

	// Create random offsets in range [0, 1) to remove bias when quantizing
	// query vectors.
	unbias := make([]float32, dims)
	for i := 0; i < len(unbias); i++ {
		unbias[i] = rng.Float32()
	}

	sqrtDims := num32.Sqrt(float32(dims))
	return &RaBitQuantizer{
		dims:        dims,
		sqrtDims:    sqrtDims,
		sqrtDimsInv: 1.0 / sqrtDims,
		rot:         rot,
		unbias:      unbias,
	}
}

// GetOriginalDims implements the Quantizer interface.
func (q *RaBitQuantizer) GetOriginalDims() int {
	return q.dims
}

// GetRandomDims implements the Quantizer interface.
func (q *RaBitQuantizer) GetRandomDims() int {
	return q.dims
}

// RandomizeVector implements the Quantizer interface.
func (q *RaBitQuantizer) RandomizeVector(
	ctx context.Context, input vector.T, output vector.T, invert bool,
) {
	if !invert {
		num32.MulMatrixByVector(&q.rot, input, output, num32.NoTranspose)
	} else {
		num32.MulMatrixByVector(&q.rot, input, output, num32.Transpose)
	}
}

// Quantize implements the Quantizer interface.
func (q *RaBitQuantizer) Quantize(ctx context.Context, vectors *vector.Set) QuantizedVectorSet {
	// Allocate slice for the centroid.
	quantizedSet := &RaBitQuantizedVectorSet{
		Centroid: vectors.Centroid(make(vector.T, vectors.Dims)),
		Codes:    MakeRaBitQCodeSet(vectors.Dims),
	}
	q.quantizeHelper(ctx, quantizedSet, vectors)
	return quantizedSet
}

// QuantizeInSet implements the Quantizer interface.
func (q *RaBitQuantizer) QuantizeInSet(
	ctx context.Context, quantizedSet QuantizedVectorSet, vectors *vector.Set,
) {
	q.quantizeHelper(ctx, quantizedSet.(*RaBitQuantizedVectorSet), vectors)
}

// NewQuantizedVectorSet implements the Quantizer interface
func (q *RaBitQuantizer) NewQuantizedVectorSet(capacity int, centroid vector.T) QuantizedVectorSet {
	dataBuffer := make([]uint64, 0, capacity*RaBitQCodeSetWidth(q.GetRandomDims()))
	raBitQuantizedVectorSet := &RaBitQuantizedVectorSet{
		Centroid:          centroid,
		Codes:             MakeRaBitQCodeSetFromRawData(dataBuffer, q.GetRandomDims()),
		CodeCounts:        make([]uint32, 0, capacity),
		CentroidDistances: make([]float32, 0, capacity),
		DotProducts:       make([]float32, 0, capacity),
	}
	return raBitQuantizedVectorSet
}

// EstimateSquaredDistances implements the Quantizer interface.
func (q *RaBitQuantizer) EstimateSquaredDistances(
	ctx context.Context,
	quantizedSet QuantizedVectorSet,
	queryVector vector.T,
	squaredDistances []float32,
	errorBounds []float32,
) {
	raBitSet := quantizedSet.(*RaBitQuantizedVectorSet)

	// Allocate temp space for calculations.
	workspace := internal.WorkspaceFromContext(ctx)
	tempCodes := allocCodes(workspace, 4, raBitSet.Codes.Width)
	defer freeCodes(workspace, tempCodes)
	tempVectors := workspace.AllocVectorSet(1, q.dims)
	defer workspace.FreeVectorSet(tempVectors)

	// Normalize the query vector to a unit vector.
	// Paper: q = (q_raw - c) / ||q_raw - c||
	tempQueryDiff := tempVectors.At(0)
	num32.SubTo(tempQueryDiff, queryVector, quantizedSet.GetCentroid())
	queryCentroidDistance := num32.Norm(tempQueryDiff)

	if queryCentroidDistance == 0 {
		// The query vector is the centroid. This means the squared distances from
		// the query to the quantized vectors are just the centroid distances that
		// have already been calculated, but just need to be squared.
		centroidDistances := quantizedSet.GetCentroidDistances()
		num32.MulTo(squaredDistances, centroidDistances, centroidDistances)
		num32.Zero(errorBounds)
		return
	}

	tempQueryUnitVector := tempQueryDiff
	num32.Scale(1.0/queryCentroidDistance, tempQueryUnitVector)

	// Find min and max values within the vector.
	// Paper: v_left and v_right
	minVal := num32.Min(tempQueryUnitVector)
	maxVal := num32.Max(tempQueryUnitVector)

	// Quantize query vector using small unsigned ints in the range [0,15].
	// Paper: Δ = (v_right - v_left) / (2^B_q - 1)
	//        q¯u[i] = floor((q'[i] - v_left) / Δ + u[i])
	const quantizedRange = 15
	delta := (maxVal - minVal) / quantizedRange

	// The full quantized query code is separated into 4 sub-codes. The first
	// sub-code includes bit 1 of the full code, the second sub-code includes
	// bit 2, the third bit 3, and the fourth bit 4. This separation enables more
	// efficient computation of the dot product between the quantized query vector
	// and the quantized data vectors.
	var quantized1, quantized2, quantized3, quantized4 uint64
	var quantizedSum uint64
	tempQueryQuantized1 := tempCodes.At(0)
	tempQueryQuantized2 := tempCodes.At(1)
	tempQueryQuantized3 := tempCodes.At(2)
	tempQueryQuantized4 := tempCodes.At(3)
	for i := 0; i < len(tempQueryUnitVector); {
		// If delta == 0, then quantized sub-codes will be set to zero. This
		// only happens when every dimension in the query has the same value.
		if delta != 0 {
			quantized := uint64(math.Floor(float64((tempQueryUnitVector[i]-minVal)/delta + q.unbias[i])))
			quantizedSum += quantized
			quantized1 = (quantized1 << 1) | (quantized & 1)
			quantized2 = (quantized2 << 1) | ((quantized & 2) >> 1)
			quantized3 = (quantized3 << 1) | ((quantized & 4) >> 2)
			quantized4 = (quantized4 << 1) | ((quantized & 8) >> 3)
		}

		i++
		if (i % 64) == 0 {
			offset := (i - 1) / 64
			tempQueryQuantized1[offset] = quantized1
			tempQueryQuantized2[offset] = quantized2
			tempQueryQuantized3[offset] = quantized3
			tempQueryQuantized4[offset] = quantized4
		}
	}

	// Set any leftover bits.
	if (len(tempQueryUnitVector) % 64) != 0 {
		offset := len(tempQueryUnitVector) / 64
		shift := 64 - (len(tempQueryUnitVector) % 64)
		tempQueryQuantized1[offset] = quantized1 << shift
		tempQueryQuantized2[offset] = quantized2 << shift
		tempQueryQuantized3[offset] = quantized3 << shift
		tempQueryQuantized4[offset] = quantized4 << shift
	}

	count := raBitSet.GetCount()
	for i := 0; i < count; i++ {
		code := raBitSet.Codes.At(i)

		var bitProduct int
		for j := 0; j < len(code); j++ {
			// Paper: <x¯bits,q¯u> = ∑ j in [0,B_q-1] (2^j * <x¯bits,q¯u¯j>)
			bitProduct += 1 * bits.OnesCount64(code[j]&tempQueryQuantized1[j])
			bitProduct += 2 * bits.OnesCount64(code[j]&tempQueryQuantized2[j])
			bitProduct += 4 * bits.OnesCount64(code[j]&tempQueryQuantized3[j])
			bitProduct += 8 * bits.OnesCount64(code[j]&tempQueryQuantized4[j])
		}

		// Compute the estimator efficiently.
		// Paper: term1 = 2Δ / √D * <x¯bits,q¯u>
		//        term2 = 2 * v_left / √D * count_bits(x¯bits)
		//        term3 = Δ / √D * sum(q¯u)
		//        term4 = √D * v_left
		//        <x¯,q¯> = term1 + term2 - term3 - term4
		//        <o¯,q> = <x¯,q'> ~ <x¯,q¯>
		//        <o,q> ~ <o¯,q> / <o¯,o>
		//
		// Note one tweak to the paper, where <o¯,o> (i.e. vector_products) is
		// stored as an inverted value so that it can be multiplied rather than
		// divided, in order to avoid divide-by-zero.
		term1 := 2 * delta * q.sqrtDimsInv * float32(bitProduct)
		term2 := 2 * minVal * q.sqrtDimsInv * float32(raBitSet.CodeCounts[i])
		term3 := delta * q.sqrtDimsInv * float32(quantizedSum)
		term4 := q.sqrtDims * minVal
		estimator := (term1 + term2 - term3 - term4) * raBitSet.DotProducts[i]

		// Compute estimated distances between the query and the quantized data
		// vectors.
		// Paper: ||o_raw - q_raw||^2 = ||o_raw - c||^2 +
		//        ||q_raw - c||^2 - 2 * ||o_raw - c|| * ||q_raw - c|| * <q,o>
		dataCentroidDistance := raBitSet.CentroidDistances[i]
		squaredDistance := dataCentroidDistance * dataCentroidDistance
		squaredDistance += queryCentroidDistance * queryCentroidDistance
		multiplier := 2 * dataCentroidDistance * queryCentroidDistance
		squaredDistance -= multiplier * estimator
		if squaredDistance < 0 {
			squaredDistance = 0
		}
		squaredDistances[i] = squaredDistance

		// Error bounds for the estimator are +- 1/√dims. For the entire distance,
		// that must be scaled by the distance terms.
		errorBounds[i] = multiplier / q.sqrtDims
	}
}

// quantizeHelper quantizes the given set of vectors and adds the quantization
// information to the provided quantized vector set.
func (q *RaBitQuantizer) quantizeHelper(
	ctx context.Context, qs *RaBitQuantizedVectorSet, vectors *vector.Set,
) {
	// Extend any existing slices in the vector set.
	count := vectors.Count
	oldCount := qs.GetCount()
	qs.AddUndefined(count)

	// Allocate temp space for vector calculations.
	workspace := internal.WorkspaceFromContext(ctx)
	tempVectors := workspace.AllocVectorSet(qs.GetCount(), q.dims)
	defer workspace.FreeVectorSet(tempVectors)

	// Calculate the difference between input vector(s) and the centroid.
	// Paper: o_raw - c
	tempDiffs := tempVectors
	for i := 0; i < count; i++ {
		num32.SubTo(tempDiffs.At(i), vectors.At(i), qs.Centroid)
	}

	// Calculate distance from each input vector to the centroid.
	// Paper: ||o_raw - c||
	centroidDistances := qs.CentroidDistances[oldCount:]
	for i := 0; i < len(centroidDistances); i++ {
		centroidDistances[i] = num32.Norm(tempDiffs.At(i))
	}

	// Normalize the input vectors into unit vectors relative to the centroid.
	// Paper (equation 1): o = (o_raw - c) / ||o_raw - c||
	tempUnitVectors := tempDiffs
	for i := 0; i < len(centroidDistances); i++ {
		// If distance to the centroid is zero, then the diff is zero. The unit
		// vector should be zero as well, so no need to do anything in that case.
		centroidDistance := centroidDistances[i]
		if centroidDistance != 0 {
			num32.ScaleTo(tempUnitVectors.At(i), 1.0/centroidDistance, tempUnitVectors.At(i))
		}
	}

	// Calculate:
	//   1. Dot products between the quantized vectors and unit vectors.
	//   2. Quantization code for each vector.
	//   3. Count of "1" bits in the quantization code.
	//
	// Note a difference from the paper: we assume that the caller applies the
	// random orthogonal transformation, so no need to do it here. This
	// simplifies any formulas from the paper which include P.
	dotProducts := qs.DotProducts[oldCount:]
	codeCounts := qs.CodeCounts[oldCount:]
	alignedDims := q.dims / 8 * 8
	for i := 0; i < count; i++ {
		// Define two functions that will be used to unroll the loop over the
		// dimensions of the unit vector. Doing this gives ~20% boost on Intel
		// and ARM.

		// getSignBit returns the floating point value's sign bit, which will be 1
		// if the value is negative (including -0), or 0 otherwise (including +0).
		getSignBit := func(value float32) uint64 {
			return uint64(math.Float32bits(value) >> 31)
		}

		// computeProduct multiplies a unit vector element by the quantized form
		// of that element. The quantized form is equal to 1/√D if the element
		// is positive and -1/√D otherwise.
		computeProduct := func(element, sqrtDimsInv float32) float32 {
			sign := float32(1 - 2*int32(getSignBit(element)))
			return element * sign * sqrtDimsInv
		}

		var dotProduct float32
		var codeBits, codeCount uint64
		tempUnitVector := tempUnitVectors.At(i)
		code := qs.Codes.At(oldCount + i)
		for dim := 0; dim < alignedDims; dim += 8 {
			// Unroll the loop 8x.

			// Compute the dot product of the unit vector and the quantized vector.
			// Paper: x¯bits ∈ {0, 1}^D | 0 if o[i] <= 0, 1 if o[i] > 0
			//        x¯ = (2 * x¯bits − 1_bits)/√D
			//        o¯ = Px¯
			//        <o¯,o>
			elements := tempUnitVector[dim : dim+8]
			dotProduct += computeProduct(elements[0], q.sqrtDimsInv)
			dotProduct += computeProduct(elements[1], q.sqrtDimsInv)
			dotProduct += computeProduct(elements[2], q.sqrtDimsInv)
			dotProduct += computeProduct(elements[3], q.sqrtDimsInv)
			dotProduct += computeProduct(elements[4], q.sqrtDimsInv)
			dotProduct += computeProduct(elements[5], q.sqrtDimsInv)
			dotProduct += computeProduct(elements[6], q.sqrtDimsInv)
			dotProduct += computeProduct(elements[7], q.sqrtDimsInv)

			// Compute the quantization code as a packed bit string.
			// Paper: x¯bits ∈ {0, 1}^D | 0 if o[i] <= 0, 1 if o[i] > 0
			codeBits <<= 8
			codeBits |= getSignBit(elements[0]) << 7
			codeBits |= getSignBit(elements[1]) << 6
			codeBits |= getSignBit(elements[2]) << 5
			codeBits |= getSignBit(elements[3]) << 4
			codeBits |= getSignBit(elements[4]) << 3
			codeBits |= getSignBit(elements[5]) << 2
			codeBits |= getSignBit(elements[6]) << 1
			codeBits |= getSignBit(elements[7])

			if (dim+8)%64 == 0 {
				// Invert the sign bits, since a "1" sign bit indicates a
				// negative float.
				codeBits = ^codeBits
				code[0] = codeBits
				code = code[1:]

				// Count the number of "1" bits in the code.
				codeCount += uint64(bits.OnesCount64(codeBits))
			}
		}

		// Handle any remaining unaligned elements in the unit vector.
		if q.dims%64 != 0 {
			for dim := alignedDims; dim < q.dims; dim++ {
				dotProduct += computeProduct(tempUnitVector[dim], q.sqrtDimsInv)
				codeBits <<= 1
				if getSignBit(tempUnitVector[dim]) == 1 {
					codeBits |= 1
				}
			}

			// Invert the sign bits and shift remaining code bits to most
			// significant bit positions.
			codeBits = ^codeBits << (64 - q.dims%64)
			code[0] = codeBits

			// Count the number of "1" bits in the code.
			codeCount += uint64(bits.OnesCount64(codeBits))
		}

		// Store the total number of "1" bits in the quantization code.
		codeCounts[i] = uint32(codeCount)

		// Store the inverted dot product, which will be used to make distance
		// estimates. If the dot product is zero, then the vector must be equal
		// to the centroid. By mapping the inverted dot product to zero here, the
		// <o,q> estimator will also map to zero, and the distance estimate will
		// collapse to the squared distance between the query vector and the
		// centroid, which is what we want.
		if dotProduct != 0 {
			dotProducts[i] = 1.0 / dotProduct
		}
	}
}

func allocCodes(w *internal.Workspace, count, width int) RaBitQCodeSet {
	tempUints := w.AllocUint64s(count * width)
	return MakeRaBitQCodeSetFromRawData(tempUints, width)
}

func freeCodes(w *internal.Workspace, codeSet RaBitQCodeSet) {
	w.FreeUint64s(codeSet.Data)
}
