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

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
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
//
// For L2Squared distance, vectors are grouped by their distance from mean
// centroids (simple averaging of each dimension). For InnerProduct and Cosine
// distance metrics, vectors are grouped by their distance from spherical
// centroids (mean centroid normalized to unit length). This prevents
// high-magnitude centroids from disproportionately attracting vectors and
// continually growing in magnitude as centroids of centroids are computed.
// FAISS normalizes centroids when using InnerProduct distance for this reason.
//
// NOTE: ComputeCentroids always returns mean centroids, even for InnerProduct
// and Cosine metrics. Spherical centroids can be derived from mean centroids
// by normalization, but the reverse is not possible.
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
	DistanceMetric vecpb.DistanceMetric
}

// ComputeCentroids separates the given set of input vectors into a left and
// right partition using the K-means algorithm. It sets the leftCentroid and
// rightCentroid inputs to the centroids of those partitions, respectively. If
// pinLeftCentroid is true, then keep the input value of leftCentroid and only
// compute the value of rightCentroid.
//
// NOTE: The caller is responsible for allocating the input centroids with
// dimensions equal to the dimensions of the input vector set.
//
// NOTE: For InnerProduct and Cosine distance metrics, clustering uses spherical
// centroids (normalized to unit length), but ComputeCentroids still always
// returns mean centroids. See BalancedKmeans comment for details.
func (km *BalancedKmeans) ComputeCentroids(
	vectors vector.Set, leftCentroid, rightCentroid vector.T, pinLeftCentroid bool,
) {
	km.validateVectors(vectors)

	tempAssignments := km.Workspace.AllocUint64s(vectors.Count)
	defer km.Workspace.FreeUint64s(tempAssignments)

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
		km.AssignPartitions(vectors, leftCentroid, rightCentroid, tempAssignments)

		// Calculate new centroids.
		if !pinLeftCentroid {
			calcPartitionCentroid(vectors, tempAssignments, 0, newLeftCentroid)
		}
		calcPartitionCentroid(vectors, tempAssignments, 1, newRightCentroid)

		// Check for convergence using the scikit-learn algorithm.
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
// as the other. Each assignment will be set to 0 if the vector gets assigned
// to the left partition, or 1 if assigned to the right partition. It returns
// the number of vectors assigned to the left partition.
//
// NOTE: For InnerProduct and Cosine distance metrics, AssignPartitions groups
// vectors by their distance from spherical centroids (i.e. unit centroids). See
// BalancedKmeans comment.
func (km *BalancedKmeans) AssignPartitions(
	vectors vector.Set, leftCentroid, rightCentroid vector.T, assignments []uint64,
) int {
	count := vectors.Count
	if len(assignments) != count {
		panic(errors.AssertionFailedf(
			"assignments slice must have length %d, got %d", count, len(assignments)))
	}

	tempDistances := km.Workspace.AllocFloats(count)
	defer km.Workspace.FreeFloats(tempDistances)

	// For Cosine and InnerProduct distances, compute the norms (magnitudes) of
	// the left and right centroids. Invert the magnitude to avoid division in
	// the loop, as well as to take care of the division-by-zero case up front.
	spherical := km.DistanceMetric == vecpb.CosineDistance || km.DistanceMetric == vecpb.InnerProductDistance
	var invLeftNorm, invRightNorm float32
	if spherical {
		invLeftNorm = num32.Norm(leftCentroid)
		if invLeftNorm != 0 {
			invLeftNorm = 1 / invLeftNorm
		}
		invRightNorm = num32.Norm(rightCentroid)
		if invRightNorm != 0 {
			invRightNorm = 1 / invRightNorm
		}
	}

	// Calculate difference between distance of each vector to the left and right
	// centroids.
	var leftCount int
	for i := range count {
		var leftDistance, rightDistance float32
		if spherical {
			// Compute the distance between the input vector and the spherical
			// centroids. Because input vectors are expected to be normalized, Cosine
			// distance reduces to be InnerProduct distance. InnerProduct distance
			// is calculated like this:
			//
			//   sphericalCentroid = centroid / ||centroid||
			//   -(inputVector · sphericalCentroid)
			//
			// That is, we convert each mean centroid to a spherical centroid by
			// normalizing it (dividing by its norm). Then we compute the negative
			// dot product of the spherical centroid with the input vector. However,
			// we can use algebraic equivalencies to change the order of operations
			// to be more efficient:
			//
			//   -(inputVector · centroid) / ||centroid||
			leftDistance = -num32.Dot(vectors.At(i), leftCentroid) * invLeftNorm
			rightDistance = -num32.Dot(vectors.At(i), rightCentroid) * invRightNorm
		} else {
			// For L2Squared, compute Euclidean distance to the mean centroids.
			leftDistance = num32.L2SquaredDistance(vectors.At(i), leftCentroid)
			rightDistance = num32.L2SquaredDistance(vectors.At(i), rightCentroid)
		}
		tempDistances[i] = leftDistance - rightDistance
		if tempDistances[i] < 0 {
			leftCount++
		}
	}

	// Check imbalance limit, so that at least (allowImbalance / 100)% of the
	// vectors go to each side.
	minCount := (count*allowImbalance + 99) / 100
	if leftCount >= minCount && (count-leftCount) >= minCount {
		// Set assignments slice.
		for i := range count {
			if tempDistances[i] < 0 {
				assignments[i] = 0
			} else {
				assignments[i] = 1
			}
		}
		return leftCount
	}

	// Not enough vectors on left or right side, so rebalance them.
	tempOffsets := km.Workspace.AllocUint64s(count)
	defer km.Workspace.FreeUint64s(tempOffsets)

	// Arg sort by the distance differences in order of increasing distance to
	// the left centroid, relative to the right centroid. Use a stable sort to
	// ensure that tests are deterministic.
	for i := range count {
		tempOffsets[i] = uint64(i)
	}
	slices.SortStableFunc(tempOffsets, func(i, j uint64) int {
		return cmp.Compare(tempDistances[i], tempDistances[j])
	})

	if leftCount < minCount {
		leftCount = minCount
	} else if (count - leftCount) < minCount {
		leftCount = count - minCount
	}

	// Set assignments slice.
	for i := range count {
		if i < leftCount {
			assignments[tempOffsets[i]] = 0
		} else {
			assignments[tempOffsets[i]] = 1
		}
	}

	return leftCount
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
		distance := vecpb.MeasureDistance(km.DistanceMetric, vectors.At(i), leftCentroid)
		if km.DistanceMetric == vecpb.InnerProductDistance {
			// For inner product, rank vectors by their angular distance from the
			// left centroid, ignoring their magnitudes.
			// NOTE: Vectors have norm of one (i.e. they are unit vectors) when using
			// Cosine distance, so no need to perform this calculation.
			// NOTE: We don't need to normalize the left centroid because scaling
			// its magnitude just scales distances by the same proportion -
			// probabilities won't change.
			norm := num32.Norm(vectors.At(i))
			if norm != 0 {
				distance /= norm
			}
		}
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
	if distanceMin != 0 {
		num32.AddConst(-distanceMin, tempDistances)
	}

	// Calculate probability of each vector becoming the right centroid, equal
	// to its distance from the left centroid. Further vectors have a higher
	// probability. For Euclidean or Cosine distance, the left centroid has zero
	// distance from itself, and so will never be selected (unless there are
	// duplicates). However, InnerProduct can select the left centroid in rare
	// cases.
	if distanceSum != 0 {
		num32.Scale(1/distanceSum, tempDistances)
	}
	var cum, rnd float32
	if km.Rand != nil {
		rnd = km.Rand.Float32()
	} else {
		rnd = rand.Float32()
	}
	rightOffset := 0
	for i := range len(tempDistances) {
		cum += tempDistances[i]
		if rnd < cum {
			rightOffset = i
			break
		}
	}
	copy(rightCentroid, vectors.At(rightOffset))
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

// validateVectors ensures that if the Cosine distance metric is being used,
// that the vectors are unit vectors.
func (km *BalancedKmeans) validateVectors(vectors vector.Set) {
	if vectors.Count < 2 {
		panic(errors.AssertionFailedf("k-means requires at least 2 vectors"))
	}

	switch km.DistanceMetric {
	case vecpb.L2SquaredDistance, vecpb.InnerProductDistance:

	case vecpb.CosineDistance:
		utils.ValidateUnitVectors(vectors)

	default:
		panic(errors.AssertionFailedf("%s distance metric is not supported", km.DistanceMetric))
	}
}

// calcPartitionCentroid calculates the mean centroid of a subset of the given
// vectors, which represents the "average" of those vectors. The subset consists
// of vectors with a corresponding assignment value equal to "assignVal". For
// example, if "assignments" is [0, 1, 0, 0, 1] and "assignVal" is 0, then
// vectors at positions 0, 2, and 3 are in the subset. The result is written to
// the provided centroid vector, which the caller is expected to allocate.
func calcPartitionCentroid(
	vectors vector.Set, assignments []uint64, assignVal uint64, centroid vector.T,
) {
	var n int
	num32.Zero(centroid)
	for i, val := range assignments {
		if val != assignVal {
			continue
		}
		num32.Add(centroid, vectors.At(i))
		n++
	}

	// Compute the mean vector by scaling the centroid by the inverse of N,
	// where N is the number of input vectors.
	num32.Scale(1/float32(n), centroid)
}
