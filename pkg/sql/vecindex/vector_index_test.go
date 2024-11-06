// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})
	state := testState{T: t, Ctx: ctx}
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		if !strings.HasSuffix(path, ".ddt") {
			// Skip files that are not data-driven tests.
			return
		}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-index":
				return state.NewIndex(d)

			case "format-tree":
				return state.FormatTree(d)

			case "search":
				return state.Search(d)

			case "delete":
				return state.Delete(d)
			}

			t.Fatalf("unknown cmd: %s", d.Cmd)
			return ""
		})
	})
}

type testState struct {
	T          *testing.T
	Ctx        context.Context
	Quantizer  quantize.Quantizer
	InMemStore *vecstore.InMemoryStore
	Index      *VectorIndex
	Features   vector.Set
}

func (s *testState) NewIndex(d *datadriven.TestData) string {
	var err error
	dims := 2
	hideTree := false
	count := 0
	options := VectorIndexOptions{}
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "min-partition-size":
			require.Len(s.T, arg.Vals, 1)
			options.MinPartitionSize, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "max-partition-size":
			require.Len(s.T, arg.Vals, 1)
			options.MaxPartitionSize, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "quality-samples":
			require.Len(s.T, arg.Vals, 1)
			options.QualitySamples, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "load-features":
			require.Len(s.T, arg.Vals, 1)
			count, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "dims":
			require.Len(s.T, arg.Vals, 1)
			dims, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "beam-size":
			require.Len(s.T, arg.Vals, 1)
			options.BaseBeamSize, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "hide-tree":
			require.Len(s.T, arg.Vals, 0)
			hideTree = true
		}
	}

	s.Quantizer = quantize.NewRaBitQuantizer(dims, 42)
	s.InMemStore = vecstore.NewInMemoryStore(dims, 42)
	s.Index, err = NewVectorIndex(s.Ctx, s.InMemStore, s.Quantizer, &options)
	require.NoError(s.T, err)

	txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	// Insert empty root partition.
	require.NoError(s.T, s.Index.CreateRoot(s.Ctx, txn))

	vectors := vector.MakeSet(dims)
	childKeys := make([]vecstore.ChildKey, 0, count)
	if count != 0 {
		// Load features.
		s.Features = testutils.LoadFeatures(s.T, 10000)
		vectors = s.Features
		vectors.SplitAt(count)
		for i := 0; i < count; i++ {
			key := vecstore.PrimaryKey(fmt.Sprintf("vec%d", i))
			childKeys = append(childKeys, vecstore.ChildKey{PrimaryKey: key})
		}
	} else {
		// Parse vectors.
		for _, line := range strings.Split(d.Input, "\n") {
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			parts := strings.Split(line, ":")
			require.Len(s.T, parts, 2)

			vectors.Add(s.parseVector(parts[1]))
			key := vecstore.PrimaryKey(parts[0])
			childKeys = append(childKeys, vecstore.ChildKey{PrimaryKey: key})
		}
	}

	// Insert vectors into the store.
	for i := 0; i < vectors.Count; i++ {
		s.InMemStore.InsertVector(txn, childKeys[i].PrimaryKey, vectors.At(i))
	}

	// Build the tree, bottom-up.
	s.buildTree(txn, vectors, childKeys, options.MaxPartitionSize)

	if hideTree {
		return fmt.Sprintf("Created index with %d vectors with %d dimensions.\n",
			vectors.Count, vectors.Dims)
	}

	tree, err := s.Index.Format(s.Ctx, txn, FormatOptions{PrimaryKeyStrings: true})
	require.NoError(s.T, err)
	return tree
}

func (s *testState) FormatTree(d *datadriven.TestData) string {
	txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	tree, err := s.Index.Format(s.Ctx, txn, FormatOptions{PrimaryKeyStrings: true})
	require.NoError(s.T, err)
	return tree
}

func (s *testState) Search(d *datadriven.TestData) string {
	txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	var vector vector.T
	searchSet := vecstore.SearchSet{MaxResults: 1}
	options := SearchOptions{}

	var err error
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "use-feature":
			require.Len(s.T, arg.Vals, 1)
			offset, err := strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)
			vector = s.Features.At(offset)

		case "max-results":
			require.Len(s.T, arg.Vals, 1)
			searchSet.MaxResults, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "beam-size":
			require.Len(s.T, arg.Vals, 1)
			options.BaseBeamSize, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "skip-rerank":
			require.Len(s.T, arg.Vals, 0)
			options.SkipRerank = true
		}
	}

	if vector == nil {
		// Parse input as the vector to search for.
		vector = s.parseVector(d.Input)
	}

	err = s.Index.Search(s.Ctx, txn, vector, &searchSet, options)
	require.NoError(s.T, err)

	var buf bytes.Buffer
	results := searchSet.PopResults()
	for i := range results {
		result := &results[i]
		var errorBound string
		if result.ErrorBound != 0 {
			errorBound = fmt.Sprintf("±%s ", formatFloat(result.ErrorBound))
		}
		fmt.Fprintf(&buf, "%s: %s %s(centroid=%s)\n",
			string(result.ChildKey.PrimaryKey), formatFloat(result.QuerySquaredDistance),
			errorBound, formatFloat(result.CentroidDistance))
	}

	buf.WriteString(fmt.Sprintf("%d leaf vectors, ", searchSet.Stats.QuantizedLeafVectorCount))
	buf.WriteString(fmt.Sprintf("%d vectors, ", searchSet.Stats.QuantizedVectorCount))
	buf.WriteString(fmt.Sprintf("%d full vectors, ", searchSet.Stats.FullVectorCount))
	buf.WriteString(fmt.Sprintf("%d partitions", searchSet.Stats.PartitionCount))

	return buf.String()
}

func (s *testState) Delete(d *datadriven.TestData) string {
	notFound := false
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "not-found":
			require.Len(s.T, arg.Vals, 0)
			notFound = true
		}
	}

	txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	// Get root in order to acquire partition lock.
	_, err := s.InMemStore.GetPartition(s.Ctx, txn, vecstore.RootKey)
	require.NoError(s.T, err)

	if notFound {
		for _, line := range strings.Split(d.Input, "\n") {
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				continue
			}

			// Simulate case where the vector is deleted in the primary index, but
			// it cannot be found in the secondary index.
			s.InMemStore.DeleteVector(txn, []byte(line))
		}
	}

	// TODO(andyk): Add code to delete vector from index.

	tree, err := s.Index.Format(s.Ctx, txn, FormatOptions{PrimaryKeyStrings: true})
	require.NoError(s.T, err)
	return tree
}

// buildTree uses the K-means++ algorithm to build a K-means tree. Unlike the
// incremental algorithm, this builds the tree from the complete set of initial
// vectors. To start, the leaf level is built from the input vectors, with the
// number of partitions derived from "maxPartitionSize". Once the leaf level has
// been partitioned, the next higher level is built from the centroids of the
// leaf partitions. And so on, up to the root of the tree.
//
// TODO(andyk): Use the incremental algorithm instead, once it's ready. This
// alternate implementation is useful for testing and benchmarking. How much
// more accurate is it than the incremental version?
func (s *testState) buildTree(
	txn vecstore.Txn, vectors vector.Set, childKeys []vecstore.ChildKey, maxPartitionSize int,
) {
	rng := rand.New(rand.NewSource(42))
	level := vecstore.LeafLevel

	// Randomize vectors.
	randomized := vector.MakeSet(vectors.Dims)
	randomized.AddUndefined(vectors.Count)
	for i := 0; i < vectors.Count; i++ {
		s.Quantizer.RandomizeVector(s.Ctx, vectors.At(i), randomized.At(i), false /* invert */)
	}

	// Partition each level of the tree.
	for randomized.Count > maxPartitionSize {
		n := randomized.Count * 2 / maxPartitionSize
		randomized, childKeys = s.partitionVectors(txn, level, randomized, childKeys, n, rng)
		level++
	}

	unQuantizer := quantize.NewUnQuantizer(randomized.Dims)
	quantizedSet := unQuantizer.Quantize(s.Ctx, &randomized)
	root := vecstore.NewPartition(unQuantizer, quantizedSet, childKeys, level)
	err := s.InMemStore.SetRootPartition(s.Ctx, txn, root)
	require.NoError(s.T, err)
}

// partitionVectors partitions the given full-size vectors at one level of the
// tree using the K-means++ algorithm.
func (s *testState) partitionVectors(
	txn vecstore.Txn,
	level vecstore.Level,
	vectors vector.Set,
	childKeys []vecstore.ChildKey,
	numPartitions int,
	rng *rand.Rand,
) (centroids vector.Set, partitionKeys []vecstore.ChildKey) {
	centroids = vector.MakeSet(vectors.Dims)
	centroids.AddUndefined(numPartitions)
	partitionKeys = make([]vecstore.ChildKey, numPartitions)

	// Run K-means on the input vectors.
	km := kmeans{Rand: rng}
	partitionOffsets := make([]uint64, vectors.Count)
	km.Partition(s.Ctx, &vectors, &centroids, partitionOffsets)

	// Construct the partitions and insert them into the store.
	tempVectors := vector.MakeSet(vectors.Dims)
	for partitionIdx := 0; partitionIdx < numPartitions; partitionIdx++ {
		var partitionChildKeys []vecstore.ChildKey
		for vectorIdx := 0; vectorIdx < vectors.Count; vectorIdx++ {
			if partitionIdx == int(partitionOffsets[vectorIdx]) {
				tempVectors.Add(vectors.At(vectorIdx))
				partitionChildKeys = append(partitionChildKeys, childKeys[vectorIdx])
			}
		}

		quantizedSet := s.Quantizer.Quantize(s.Ctx, &tempVectors)
		partition := vecstore.NewPartition(s.Quantizer, quantizedSet, partitionChildKeys, level)

		partitionKey, err := s.InMemStore.InsertPartition(s.Ctx, txn, partition)
		require.NoError(s.T, err)
		partitionKeys[partitionIdx] = vecstore.ChildKey{PartitionKey: partitionKey}

		tempVectors.Clear()
	}

	return centroids, partitionKeys
}

// parseVector parses a vector string in this form: (1.5, 6, -4).
func (s *testState) parseVector(str string) vector.T {
	// Remove parentheses and split by commas.
	str = strings.TrimSpace(str)
	str = strings.TrimPrefix(str, "(")
	str = strings.TrimSuffix(str, ")")
	elems := strings.Split(str, ",")

	// Construct the vector.
	vector := make(vector.T, len(elems))
	for i, elem := range elems {
		elem = strings.TrimSpace(elem)
		value, err := strconv.ParseFloat(elem, 32)
		require.NoError(s.T, err)
		vector[i] = float32(value)
	}

	return vector
}

func formatFloat(value float32) string {
	s := strconv.FormatFloat(float64(value), 'f', 4, 32)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return s
}

// kmeans implements the K-means++ algorithm:
// http://ilpubs.stanford.edu:8090/778/1/2006-13.pdf
type kmeans struct {
	MaxIterations int
	Rand          *rand.Rand

	workspace    *internal.Workspace
	vectors      *vector.Set
	oldCentroids *vector.Set
	newCentroids *vector.Set
	partitions   []uint64
}

// Partition divides the input vectors into partitions. The caller is expected
// to allocate the "centroids" set with length equal to the desired number of
// partitions. Partition will write the centroid of each calculated partition
// into the set. In addition, the caller allocates the "partitions" slice with
// length equal to the number of input vectors. For each input vector, Partition
// will write the index of its partition into the corresponding entry in
// "partitions".
func (km *kmeans) Partition(
	ctx context.Context, vectors *vector.Set, centroids *vector.Set, partitions []uint64,
) {
	if vectors.Count != len(partitions) {
		panic(errors.AssertionFailedf("vector count %d cannot be different than partitions length %d",
			vectors.Count, len(partitions)))
	}

	km.workspace = internal.WorkspaceFromContext(ctx)
	km.vectors = vectors
	km.newCentroids = centroids
	km.partitions = partitions

	tempOldCentroids := km.workspace.AllocVectorSet(centroids.Count, vectors.Dims)
	defer km.workspace.FreeVectorSet(tempOldCentroids)
	km.oldCentroids = &tempOldCentroids

	km.selectInitialCentroids()

	maxIterations := km.MaxIterations
	if maxIterations == 0 {
		maxIterations = 32
	}

	for i := 0; i < maxIterations; i++ {
		km.computeNewCentroids()

		// Check if algorithm has converged.
		done := true
		for centroidIdx := 0; centroidIdx < km.oldCentroids.Count; centroidIdx++ {
			distance := num32.L2SquaredDistance(
				km.oldCentroids.At(centroidIdx), km.newCentroids.At(centroidIdx))
			if distance > 1e-4 {
				done = false
				break
			}
		}
		if done {
			break
		}

		// Swap old and new centroid slices.
		km.oldCentroids, km.newCentroids = km.newCentroids, km.oldCentroids

		// Re-assign vectors to one of the partitions.
		km.assignPartitions()
	}
}

// assignPartitions re-assigns each input vector to the partition with the
// closest centroid in "km.oldCentroids".
func (km *kmeans) assignPartitions() {
	vectorCount := km.vectors.Count
	centroidCount := km.oldCentroids.Count

	// Add vectors in each partition.
	for vecIdx := 0; vecIdx < vectorCount; vecIdx++ {
		var shortest float32
		shortestIdx := -1
		for centroidIdx := 0; centroidIdx < centroidCount; centroidIdx++ {
			distance := num32.L2SquaredDistance(km.vectors.At(vecIdx), km.oldCentroids.At(centroidIdx))
			if shortestIdx == -1 || distance < shortest {
				shortest = distance
				shortestIdx = centroidIdx
			}
		}
		km.partitions[vecIdx] = uint64(shortestIdx)
	}
}

// computeNewCentroids calculates a new centroid for each partition from the
// vectors that have been assigned to that partition, and stores the resulting
// centroids in "km.newCentroids".
func (km *kmeans) computeNewCentroids() {
	centroidCount := km.newCentroids.Count
	vectorCount := km.vectors.Count

	tempPartitionCounts := km.workspace.AllocUint64s(centroidCount)
	defer km.workspace.FreeUint64s(tempPartitionCounts)
	for i := 0; i < centroidCount; i++ {
		tempPartitionCounts[i] = 0
	}

	// Calculate new centroids.
	num32.Zero(km.newCentroids.Data)
	for vecIdx := 0; vecIdx < vectorCount; vecIdx++ {
		centroidIdx := int(km.partitions[vecIdx])
		num32.Add(km.newCentroids.At(centroidIdx), km.vectors.At(vecIdx))
		tempPartitionCounts[centroidIdx]++
	}

	// Divide each centroid by the count of vectors in its partition.
	for centroidIdx := 0; centroidIdx < centroidCount; centroidIdx++ {
		num32.Scale(1.0/float32(tempPartitionCounts[centroidIdx]), km.newCentroids.At(centroidIdx))
	}
}

// selectInitialCentroids sets "km.oldCentroids" to random input vectors chosen
// using the K-means++ algorithm.
func (km *kmeans) selectInitialCentroids() {
	count := km.vectors.Count
	tempVectorDistances := km.workspace.AllocFloats(count)
	defer km.workspace.FreeFloats(tempVectorDistances)

	// Randomly select the first centroid from the vector set.
	var offset int
	if km.Rand != nil {
		offset = km.Rand.Intn(count)
	} else {
		offset = rand.Intn(count)
	}
	copy(km.oldCentroids.At(0), km.vectors.At(offset))

	selected := 0
	for selected < km.oldCentroids.Count {
		// Calculate shortest distance from each vector to one of the already
		// selected centroids.
		var distanceSum float32
		for vecIdx := 0; vecIdx < count; vecIdx++ {
			distance := num32.L2SquaredDistance(km.vectors.At(vecIdx), km.oldCentroids.At(selected))
			if selected == 0 || distance < tempVectorDistances[vecIdx] {
				tempVectorDistances[vecIdx] = distance
				km.partitions[vecIdx] = uint64(selected)
			}
			distanceSum += tempVectorDistances[vecIdx]
		}

		// Calculate probability of each vector becoming the next centroid, with
		// the probability being proportional to the vector's shortest distance
		// to one of the already selected centroids.
		var cum, rnd float32
		if km.Rand != nil {
			rnd = km.Rand.Float32() * distanceSum
		} else {
			rnd = rand.Float32() * distanceSum
		}
		offset = 0
		for offset < len(tempVectorDistances) {
			cum += tempVectorDistances[offset]
			if rnd < cum {
				break
			}
			offset++
		}

		selected++
		copy(km.oldCentroids.At(selected), km.vectors.At(offset))
	}
}

func beginTransaction(ctx context.Context, t *testing.T, store vecstore.Store) vecstore.Txn {
	txn, err := store.BeginTransaction(ctx)
	require.NoError(t, err)
	return txn
}

func commitTransaction(ctx context.Context, t *testing.T, store vecstore.Store, txn vecstore.Txn) {
	err := store.CommitTransaction(ctx, txn)
	require.NoError(t, err)
}
