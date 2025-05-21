// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"math"
	"math/bits"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
	// unbias is a precomputed slice of "dims" random values in the [0, 1)
	// interval that's used to remove bias when quantizing query vectors.
	unbias []float32
	// distanceMetric determines which distance function to use.
	distanceMetric vecdist.Metric
}

// raBitQuantizedVector adds extra storage space for the special case where the
// vector set has at most one vector. In that case, the vector set slices point
// to the statically-allocated arrays in this struct.
type raBitQuantizedVector struct {
	RaBitQuantizedVectorSet
	codeCountStorage        [1]uint32
	centroidDistanceStorage [1]float32
	dotProductStorage       [1]float32
}

var _ Quantizer = (*RaBitQuantizer)(nil)

// NewRaBitQuantizer returns a new RaBitQ quantizer that quantizes vectors with
// the given number of dimensions. The provided seed is used to generate the
// pseudo-random values used by the algorithm. It's important that the quantizer
// is created with the same seed that was previously used to create any
// quantized sets that need to be searched or updated.
func NewRaBitQuantizer(dims int, seed int64, distanceMetric vecdist.Metric) Quantizer {
	if dims <= 0 {
		panic(errors.AssertionFailedf("dimensions are not positive: %d", dims))
	}

	rng := rand.New(rand.NewSource(seed))

	// Create random offsets in range [0, 1) to remove bias when quantizing
	// query vectors.
	unbias := make([]float32, dims)
	for i := range len(unbias) {
		unbias[i] = rng.Float32()
	}

	sqrtDims := num32.Sqrt(float32(dims))
	return &RaBitQuantizer{
		dims:           dims,
		sqrtDims:       sqrtDims,
		sqrtDimsInv:    1.0 / sqrtDims,
		unbias:         unbias,
		distanceMetric: distanceMetric,
	}
}

// GetDims implements the Quantizer interface.
func (q *RaBitQuantizer) GetDims() int {
	return q.dims
}

// GetDistanceMetric implements the Quantizer interface.
func (q *RaBitQuantizer) GetDistanceMetric() vecdist.Metric {
	return q.distanceMetric
}

// Quantize implements the Quantizer interface.
func (q *RaBitQuantizer) Quantize(w *workspace.T, vectors vector.Set) QuantizedVectorSet {
	var centroid vector.T
	if vectors.Count == 1 {
		// If quantizing a single vector, it is the centroid of the set.
		centroid = vectors.At(0)
	} else {
		// Compute the centroid.
		centroid = vectors.Centroid(make(vector.T, vectors.Dims))
		if q.distanceMetric == vecdist.InnerProduct || q.distanceMetric == vecdist.Cosine {
			// Use spherical centroid for inner product and cosine distances,
			// which is the mean centroid, but normalized.
			num32.Normalize(centroid)
		}
	}

	quantizedSet := q.NewQuantizedVectorSet(vectors.Count, centroid)
	q.quantizeHelper(w, quantizedSet.(*RaBitQuantizedVectorSet), vectors)
	return quantizedSet
}

// QuantizeInSet implements the Quantizer interface.
func (q *RaBitQuantizer) QuantizeInSet(
	w *workspace.T, quantizedSet QuantizedVectorSet, vectors vector.Set,
) {
	q.quantizeHelper(w, quantizedSet.(*RaBitQuantizedVectorSet), vectors)
}

// NewQuantizedVectorSet implements the Quantizer interface
func (q *RaBitQuantizer) NewQuantizedVectorSet(capacity int, centroid vector.T) QuantizedVectorSet {
	if buildutil.CrdbTestBuild && q.distanceMetric == vecdist.Cosine {
		validateUnitVector(centroid)
	}

	codeWidth := RaBitQCodeSetWidth(q.GetDims())
	dataBuffer := make([]uint64, 0, capacity*codeWidth)
	if capacity <= 1 {
		// Special case capacity of zero or one by using in-line storage.
		var quantized raBitQuantizedVector
		quantized.Centroid = centroid
		quantized.Codes = MakeRaBitQCodeSetFromRawData(dataBuffer, codeWidth)
		quantized.CodeCounts = quantized.codeCountStorage[:0]
		quantized.CentroidDistances = quantized.centroidDistanceStorage[:0]
		quantized.QuantizedDotProducts = quantized.dotProductStorage[:0]
		return &quantized.RaBitQuantizedVectorSet
	}

	vs := &RaBitQuantizedVectorSet{
		Centroid:             centroid,
		Codes:                MakeRaBitQCodeSetFromRawData(dataBuffer, codeWidth),
		CodeCounts:           make([]uint32, 0, capacity),
		CentroidDistances:    make([]float32, 0, capacity),
		QuantizedDotProducts: make([]float32, 0, capacity),
	}
	// L2Squared doesn't use this, so don't make extra allocation.
	if q.distanceMetric != vecdist.L2Squared {
		vs.CentroidDotProducts = make([]float32, 0, capacity)
	}
	return vs
}

// EstimateDistances implements the Quantizer interface.
func (q *RaBitQuantizer) EstimateDistances(
	w *workspace.T,
	quantizedSet QuantizedVectorSet,
	queryVector vector.T,
	distances []float32,
	errorBounds []float32,
) {
	if buildutil.CrdbTestBuild && q.distanceMetric == vecdist.Cosine {
		validateUnitVector(queryVector)
	}

	raBitSet := quantizedSet.(*RaBitQuantizedVectorSet)

	// Allocate temp space for calculations.
	tempCodes := allocCodes(w, 4, raBitSet.Codes.Width)
	defer freeCodes(w, tempCodes)
	tempVectors := w.AllocVectorSet(1, q.dims)
	defer w.FreeVectorSet(tempVectors)

	// Normalize the query vector to a unit vector, with respect to the centroid.
	// Paper: q = (q_raw - c) / ||q_raw - c||
	tempQueryDiff := tempVectors.At(0)
	num32.SubTo(tempQueryDiff, queryVector, raBitSet.Centroid)
	queryCentroidDistance := num32.Norm(tempQueryDiff)

	if queryCentroidDistance == 0 {
		// The query vector is the centroid.
		switch q.distanceMetric {
		case vecdist.L2Squared:
			// The distance from the query to the data vectors are just the centroid
			// distances that have already been calculated, but just need to be
			// squared.
			num32.MulTo(distances, raBitSet.CentroidDistances, raBitSet.CentroidDistances)

		case vecdist.InnerProduct:
			// The dot products between the centroid and the data vectors have
			// already been computed, just need to negate them.
			num32.ScaleTo(distances, -1, raBitSet.CentroidDotProducts)

		case vecdist.Cosine:
			// All vectors have been normalized, so cosine distance = 1 - dot product.
			num32.ScaleTo(distances, -1, raBitSet.CentroidDotProducts)
			num32.AddConst(1, distances)
		}

		num32.Zero(errorBounds)
		return
	}

	// L2Squared doesn't use these values, so don't compute them in its case.
	var squaredCentroidNorm, queryCentroidDotProduct float32
	if q.distanceMetric != vecdist.L2Squared {
		queryCentroidDotProduct = num32.Dot(queryVector, raBitSet.Centroid)
		squaredCentroidNorm = num32.SquaredNorm(raBitSet.Centroid)
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
	for i := range len(tempQueryUnitVector) {
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
	for i := range count {
		code := raBitSet.Codes.At(i)

		var bitProduct int
		for j := range len(code) {
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
		// Note one tweak to the paper, where <o¯,o> (i.e. DotProducts) is
		// stored as an inverted value so that it can be multiplied rather than
		// divided, in order to avoid divide-by-zero.
		term1 := 2 * delta * q.sqrtDimsInv * float32(bitProduct)
		term2 := 2 * minVal * q.sqrtDimsInv * float32(raBitSet.CodeCounts[i])
		term3 := delta * q.sqrtDimsInv * float32(quantizedSum)
		term4 := q.sqrtDims * minVal
		estimator := (term1 + term2 - term3 - term4) * raBitSet.QuantizedDotProducts[i]
		dataCentroidDistance := raBitSet.CentroidDistances[i]

		// Compute estimated distances between the query and the quantized data
		// vector.
		switch q.distanceMetric {
		case vecdist.L2Squared:
			// Paper: ||o_raw - q_raw||^2 = ||o_raw - c||^2 +
			//        ||q_raw - c||^2 - 2 * ||o_raw - c|| * ||q_raw - c|| * <q,o>
			// The formula comes from equation 2 in the paper.
			distance := dataCentroidDistance * dataCentroidDistance
			distance += queryCentroidDistance * queryCentroidDistance
			multiplier := 2 * dataCentroidDistance * queryCentroidDistance
			distance -= multiplier * estimator

			// Error bounds for the estimator are +- 1/√dims. For the entire distance,
			// that must be scaled by the amount the estimator is scaled by. Ensure
			// the distance is >= 0, adjusting the error bound accordingly.
			errorBound := multiplier / q.sqrtDims
			if distance < 0 {
				errorBound = max(errorBound+distance, 0)
				distance = 0
			}

			distances[i] = distance
			errorBounds[i] = errorBound

		case vecdist.InnerProduct, vecdist.Cosine:
			// Note that the cosine similarity of two vectors is equal to their
			// inner product when they are unit vectors (which the caller must
			// guarantee).
			//
			// Paper: <o_raw, q_raw> = ||o_raw - c|| * ||q_raw - c|| * <q,o> +
			//        <o_raw,c> + <q_raw,c> - ||c||^2
			// The formula comes from footnote 8 in the paper.
			multiplier := dataCentroidDistance * queryCentroidDistance
			innerProduct := multiplier*estimator +
				raBitSet.CentroidDotProducts[i] + queryCentroidDotProduct - squaredCentroidNorm

			// Error bounds for the estimator are +- 1/√dims. For the entire distance,
			// that must be scaled by the amount the estimator is scaled by.
			errorBound := multiplier / q.sqrtDims

			var distance float32
			if q.distanceMetric == vecdist.InnerProduct {
				// Negate the inner product so that the more similar the vectors,
				// the lower the distance.
				distance = -innerProduct
			} else {
				// Cosine distance is 1 - cosine similarity (which is the inner
				// product for unit vectors). Cap the distance between 0 and 2,
				// adjusting the error bound accordingly.
				distance = 1 - innerProduct
				if distance < 0 {
					errorBound = max(errorBound+distance, 0)
					distance = 0
				} else if distance > 2 {
					errorBound = max(min(errorBound-(distance-2), 2), 0)
					distance = 2
				}
			}

			distances[i] = distance
			errorBounds[i] = errorBound

		default:
			panic(errors.AssertionFailedf("unknown distance function %d", q.distanceMetric))
		}
	}
}

// quantizeHelper quantizes the given set of vectors and adds the quantization
// information to the provided quantized vector set.
func (q *RaBitQuantizer) quantizeHelper(
	w *workspace.T, qs *RaBitQuantizedVectorSet, vectors vector.Set,
) {
	if buildutil.CrdbTestBuild && q.distanceMetric == vecdist.Cosine {
		validateUnitVectors(vectors)
	}

	// Extend any existing slices in the vector set.
	count := vectors.Count
	oldCount := qs.GetCount()
	qs.AddUndefined(count, q.distanceMetric)

	// L2Squared doesn't use this, so don't store it.
	if q.distanceMetric != vecdist.L2Squared {
		centroidDotProducts := qs.CentroidDotProducts[oldCount:]
		for i := range count {
			centroidDotProducts[i] = num32.Dot(vectors.At(i), qs.Centroid)
		}
	}

	// Allocate temp space for vector calculations.
	tempVectors := w.AllocVectorSet(qs.GetCount(), q.dims)
	defer w.FreeVectorSet(tempVectors)

	// Calculate the difference between input vector(s) and the centroid.
	// Paper: o_raw - c
	tempDiffs := tempVectors
	for i := range count {
		num32.SubTo(tempDiffs.At(i), vectors.At(i), qs.Centroid)
	}

	// Calculate Euclidean distance from each input vector to the centroid.
	// Paper: ||o_raw - c||
	centroidDistances := qs.CentroidDistances[oldCount:]
	for i := range len(centroidDistances) {
		centroidDistances[i] = num32.Norm(tempDiffs.At(i))
	}

	// Normalize the input vectors into unit vectors relative to the centroid.
	// Paper (equation 1): o = (o_raw - c) / ||o_raw - c||
	tempUnitVectors := tempDiffs
	for i := range len(centroidDistances) {
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
	dotProducts := qs.QuantizedDotProducts[oldCount:]
	codeCounts := qs.CodeCounts[oldCount:]
	alignedDims := q.dims / 8 * 8
	for i := range count {
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
		// estimates. The dot product is only zero in the case where the data vector
		// is equal to the centroid vector. That case is handled separately in
		// EstimatedDistances.
		if dotProduct != 0 {
			dotProducts[i] = 1.0 / dotProduct
		}
	}
}

func allocCodes(w *workspace.T, count, width int) RaBitQCodeSet {
	tempUints := w.AllocUint64s(count * width)
	return MakeRaBitQCodeSetFromRawData(tempUints, width)
}

func freeCodes(w *workspace.T, codeSet RaBitQCodeSet) {
	w.FreeUint64s(codeSet.Data)
}
