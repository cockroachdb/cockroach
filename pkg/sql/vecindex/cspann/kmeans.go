// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"cmp"
	"math"
	"math/rand"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// allowImbalance constrains how vectors will be assigned to partitions by the
// balanced K-means algorithm. If there are 100 vectors, then at least this
// number of vectors will be assigned to a side.
const allowImbalance = 33

// BalancedKmeans implements a balanced K-Means algorithm that separates
// d-dimensional vectors into a left and right partition. Vectors in each of the
// resulting partition are more similar to their own partition than they are to
// the other partition (i.e. closer to the centroid of each partition). The size
// of each partition is guaranteed to have no less than 1/3rd of the vectors to
// be partitioned.
//
// The more general K-means algorithm can partition a set of N items into k
// clusters, where k <= N. However, splitting a K-means tree partition only
// requires k = 2, and since the code is simpler and faster for this case,
// that's all this class handles. In the future, if k > 2 becomes important, we
// can extend the algorithm using this paper as a guide:
//
// "Fast Partitioning with Flexible Balance Constraints" by Hongfu Liu, Ziming
// Huang, et. al.
// URL: https://ieeexplore.ieee.org/document/8621917
//
// We should also look at the FAISS library's implementation of K-means, which
// has provisions for dealing with empty clusters.
type BalancedKmeans struct {
	// MaxIterations specifies the maximum number of retries that the K-means
	// algorithm will attempt as part of finding locally optimal partitions.
	MaxIterations int
	// Workspace is used to allocate temporary memory using stack allocators.
	Workspace *workspace.T
	// Rand is used to generate random numbers. If this is nil, then the global
	// random number generator is used instead. Setting this to non-nil is useful
	// for generating deterministic random numbers during testing.
	Rand *rand.Rand
	// DistanceMetric specifies which distance function to use when clustering
	// vectors. Lower distances indicate greater similarity.
	DistanceMetric vecdist.Metric
}

// ComputeCentroids separates the given set of input vectors into a left and
// right partition using the K-means algorithm. It sets the leftCentroid and
// rightCentroid inputs to the centroids of those partitions, respectively. If
// pinLeftCentroid is true, then keep the input value of leftCentroid and only
// compute the value of rightCentroid.
//
// NOTE: The caller is responsible for allocating the input centroids with
// dimensions equal to the dimensions of the input vector set.
func (km *BalancedKmeans) ComputeCentroids(
	vectors vector.Set, leftCentroid, rightCentroid vector.T, pinLeftCentroid bool,
) {
	if vectors.Count < 2 {
		panic(errors.AssertionFailedf("k-means requires at least 2 vectors"))
	}

	tempOffsets := km.Workspace.AllocUint64s(vectors.Count)
	defer km.Workspace.FreeUint64s(tempOffsets)

	tolerance := km.calculateTolerance(vectors)

	// Pick 2 centroids to start, using the K-means++ algorithm.
	// TOOD(andyk): We should consider adding an outer loop here to generate new
	// random centroids in the case where more than 2/3 the vectors are assigned
	// to one of the partitions. If we're that unbalanced, it might just be
	// because we picked bad starting centroids and retrying could correct that.
	tempLeftCentroid := km.Workspace.AllocVector(vectors.Dims)
	defer km.Workspace.FreeVector(tempLeftCentroid)
	newLeftCentroid := tempLeftCentroid

	tempRightCentroid := km.Workspace.AllocVector(vectors.Dims)
	defer km.Workspace.FreeVector(tempRightCentroid)
	newRightCentroid := tempRightCentroid

	if !pinLeftCentroid {
		km.selectInitialLeftCentroid(vectors, leftCentroid)
	} else {
		newLeftCentroid = leftCentroid
	}
	km.selectInitialRightCentroid(vectors, leftCentroid, rightCentroid)

	maxIterations := km.MaxIterations
	if maxIterations == 0 {
		maxIterations = 16
	}

	for range maxIterations {
		// Assign vectors to one of the partitions.
		leftOffsets, rightOffsets := km.AssignPartitions(
			vectors, leftCentroid, rightCentroid, tempOffsets)

		// Calculate new centroids.
		if !pinLeftCentroid {
			calcPartitionCentroid(km.DistanceMetric, vectors, leftOffsets, newLeftCentroid)
		}
		calcPartitionCentroid(km.DistanceMetric, vectors, rightOffsets, newRightCentroid)

		// Check for convergence using the scikit-learn algorithm.
		// NOTE: This uses Euclidean distance, even when using spherical centroids
		// with Cosine or InnerProduct distances. This approach mirrors the
		// spherecluster library. Since spherical centroids are always normalized
		// (unit vectors), the squared Euclidean distance is 2x the Cosine or
		// InnerProduct distance, so it's a reasonable convergence check.
		leftCentroidShift := num32.L2SquaredDistance(leftCentroid, newLeftCentroid)
		rightCentroidShift := num32.L2SquaredDistance(rightCentroid, newRightCentroid)
		if leftCentroidShift+rightCentroidShift <= tolerance {
			break
		}

		// Swap old and new centroids.
		newLeftCentroid, leftCentroid = leftCentroid, newLeftCentroid
		newRightCentroid, rightCentroid = rightCentroid, newRightCentroid
	}
}

// AssignPartitions assigns the input vectors into either the left or right
// partition, based on which partition's centroid they're closer to. It also
// enforces a constraint that one partition will never be more than 2x as large
// as the other.
func (km *BalancedKmeans) AssignPartitions(
	vectors vector.Set, leftCentroid, rightCentroid vector.T, offsets []uint64,
) (leftOffsets, rightOffsets []uint64) {
	count := vectors.Count
	tempDistances := km.Workspace.AllocFloats(count)
	defer km.Workspace.FreeFloats(tempDistances)

	// Calculate difference between distance of each vector to the left and right
	// centroids.
	for i := range count {
		tempDistances[i] = vecdist.Measure(km.DistanceMetric, vectors.At(i), leftCentroid) -
			vecdist.Measure(km.DistanceMetric, vectors.At(i), rightCentroid)
		offsets[i] = uint64(i)
	}

	// Arg sort by the distance differences in order of increasing distance to
	// the left centroid, relative to the right centroid. Use a stable sort to
	// ensure that tests are deterministic.
	slices.SortStableFunc(offsets, func(i, j uint64) int {
		return cmp.Compare(tempDistances[i], tempDistances[j])
	})

	// Find split between distances, with negative distances going to the left
	// centroid and others going to the right. Enforce imbalance limit, such that
	// at least (allowImbalance / 100)% of the vectors go to each side.
	start := (count*allowImbalance + 99) / 100
	split := start
	for split < count-start {
		if tempDistances[offsets[split]] >= 0 {
			break
		}
		split++
	}

	return offsets[:split], offsets[split:]
}

// calculateTolerance computes a threshold distance value. Once new centroids
// are less than this distance from the old centroids, the K-means algorithm
// terminates.
func (km *BalancedKmeans) calculateTolerance(vectors vector.Set) float32 {
	tempVectorSet := km.Workspace.AllocVectorSet(4, vectors.Dims)
	defer km.Workspace.FreeVectorSet(tempVectorSet)

	// Use tolerance algorithm from scikit-learn:
	//   tolerance = mean(variances(vectors, axis=0)) * 1e-4
	return km.calculateMeanOfVariances(vectors) * 1e-4
}

// selectInitialLeftCentroid selects the left centroid randomly from the input
// vector set. This is according to the K-means++ algorithm, from this paper:
//
// "k-means++: The Advantages of Careful Seeding", by David Arthur and Sergei
// Vassilvitskii
// URL: http://ilpubs.stanford.edu:8090/778/1/2006-13.pdf
//
// The chosen vector is copied into "leftCentroid".
func (km *BalancedKmeans) selectInitialLeftCentroid(vectors vector.Set, leftCentroid vector.T) {
	// Randomly select the left centroid from the vector set.
	var leftOffset int
	if km.Rand != nil {
		leftOffset = km.Rand.Intn(vectors.Count)
	} else {
		leftOffset = rand.Intn(vectors.Count)
	}
	copy(leftCentroid, vectors.At(leftOffset))
	km.maybeNormalizeCentroid(leftCentroid)
}

// selectInitialRightCentroid continues the K-means++ algorithm begun in
// selectInitialLeftCentroid by randomly selecting from the remaining vectors,
// but with probability that is proportional to their distances from the left
// centroid. The chosen vector is copied into "rightCentroid".
func (km *BalancedKmeans) selectInitialRightCentroid(
	vectors vector.Set, leftCentroid, rightCentroid vector.T,
) {
	count := vectors.Count
	tempDistances := km.Workspace.AllocFloats(count)
	defer km.Workspace.FreeFloats(tempDistances)

	// Calculate distance of each vector in the set from the left centroid. Keep
	// track of min distance and sum of distances for calculating probabilities.
	var distanceSum float32
	distanceMin := float32(math.MaxFloat32)
	for i := range count {
		distance := vecdist.Measure(km.DistanceMetric, vectors.At(i), leftCentroid)
		tempDistances[i] = distance
		distanceSum += distance
		if distance < distanceMin {
			distanceMin = distance
		}
	}
	// Adjust the sum of distances to handle the case where the min distance is
	// not zero. For example, if the min distance is -10, then all distances need
	// to be adjusted by +10 so that the min distance becomes 0.
	distanceSum += float32(count) * -distanceMin

	// Calculate probability of each vector becoming the right centroid, equal
	// to its distance from the left centroid. Further vectors have a higher
	// probability. For Euclidean or Cosine distance, the left centroid has zero
	// distance from itself, and so will never be selected (unless there are
	// duplicates). However, InnerProduct can select the left centroid in rare
	// cases.
	num32.Scale(1/distanceSum, tempDistances)
	var cum, rnd float32
	if km.Rand != nil {
		rnd = km.Rand.Float32()
	} else {
		rnd = rand.Float32()
	}
	rightOffset := 0
	for i := range len(tempDistances) {
		cum += tempDistances[i] + -distanceMin
		if rnd < cum {
			rightOffset = i
			break
		}
	}
	copy(rightCentroid, vectors.At(rightOffset))
	km.maybeNormalizeCentroid(rightCentroid)
}

// calculateMeanOfVariances calculates the variance in each dimension of the
// input vectors and then returns the mean of those variances. Calculate this
// using the num32 package rather than the gonum stats package since we need to
// process float32 values and because it's necessary to compute variance on
// "columns" of vectors, and that's much faster using SIMD accelerated
// functions. However, still use the same corrected 2-pass algorithm as the
// stats package uses, from this paper:
//
// "Algorithms for computing the sample variance: Analysis and recommendations",
// by Chan, Tony F., Gene H. Golub, and Randall J. LeVeque.
// URL: https://cpsc.yale.edu/sites/default/files/files/tr222.pdf
//
// See formula 1.7 in the paper:
//
//	S = sum[i=1..N]((x[i] - mean(x))**2)
//	S -= 1/N * (sum[i=1..N](x[i] - mean(x)))**2
//
// The first term is the two-pass algorithm from figure 1.1a. The second term
// is for error correction of the first term that can result from floating-point
// precision loss during intermediate calculations.
func (km *BalancedKmeans) calculateMeanOfVariances(vectors vector.Set) float32 {
	tempVectorSet := km.Workspace.AllocVectorSet(4, vectors.Dims)
	defer km.Workspace.FreeVectorSet(tempVectorSet)

	// Start with the mean of the vectors.
	tempMean := tempVectorSet.At(0)
	vectors.Centroid(tempMean)

	// Prepare temp vector storage.
	tempVariance := tempVectorSet.At(1)
	num32.Zero(tempVariance)

	tempDiff := tempVectorSet.At(2)
	tempCompensation := tempVectorSet.At(3)
	num32.Zero(tempCompensation)

	// Compute the first term and part of second term.
	for i := range vectors.Count {
		// First: x[i]
		vector := vectors.At(i)
		// First: x[i] - mean(x)
		num32.SubTo(tempDiff, vector, tempMean)
		// Second: sum[i=1..N](x[i] - mean(x))
		num32.Add(tempCompensation, tempDiff)
		// First: (x[i] - mean(x))**2
		num32.Mul(tempDiff, tempDiff)
		// First: sum[i=1..N]((x[i] - mean(x))**2)
		num32.Add(tempVariance, tempDiff)
	}

	// Finish variance computation.
	// Second: (sum[i=1..N](x[i] - mean(x)))**2
	num32.Mul(tempCompensation, tempCompensation)
	// Second: 1/N * (sum[i=1..N](x[i] - mean(x)))**2
	num32.Scale(1/float32(vectors.Count), tempCompensation)
	// S = First - Second
	num32.Sub(tempVariance, tempCompensation)

	// Variance = S / (N-1)
	num32.Scale(1/float32(vectors.Count-1), tempVariance)

	// Calculate the mean of the variance elements.
	return num32.Sum(tempVariance) / float32(vectors.Dims)
}

// maybeNormalizeCentroid normalizes the centroid (i.e. "spherical centroid") if
// using InnerProduct distance. This prevents centroids with high magnitudes
// from attracting vectors simply because of their magnitude. Faiss normalizes
// centroids by default for InnerProduct.
func (km *BalancedKmeans) maybeNormalizeCentroid(centroid vector.T) {
	if km.DistanceMetric == vecdist.InnerProduct {
		num32.Normalize(centroid)
	}
}

// calcPartitionCentroid calculates the centroid of a subset of the given
// vectors, which represents the "average" of those vectors. The subset consists
// of vectors at the given set of offsets in the set. The result is written to
// the provided centroid vector, which the caller is expected to allocate.
//
// If the distance metric is L2Squared, the Euclidean centroid is calculated:
//
//	centroid = sum(x_i) / N, where x_i = ith vector and N = number of vectors
//
// Otherwise, if the distance metric is Cosine or InnerProduct, the spherical
// centroid is calculated, which ensures that the centroid is itself a unit
// vector:
//
//	centroid = sum(x_i) / ||sum(x_i)||, where x_i = ith vector
func calcPartitionCentroid(
	distanceMetric vecdist.Metric, vectors vector.Set, offsets []uint64, centroid vector.T,
) {
	copy(centroid, vectors.At(int(offsets[0])))
	for _, offset := range offsets[1:] {
		num32.Add(centroid, vectors.At(int(offset)))
	}

	if distanceMetric == vecdist.L2Squared {
		// Compute the mean vector by scaling the centroid by the inverse of N,
		// where N is the number of input vectors.
		num32.Scale(1/float32(len(offsets)), centroid)
	} else {
		// Compute the spherical centroid across the input vectors, which represents
		// their average direction. This can be found by summing all the vectors and
		// then normalizing the result. For Cosine distance, all vectors need to be
		// unit vectors, including centroids. For InnerProduct distance, centroids
		// should be unit vectors in order to avoid attracting vectors simply due to
		// their magnitude.
		num32.Normalize(centroid)
	}
}
