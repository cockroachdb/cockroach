// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann_test

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/commontest"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/memstore"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	state := testState{T: t, Ctx: ctx, Stopper: stop.NewStopper()}
	defer state.Stopper.Stop(ctx)

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		if regexp.MustCompile("/.+/").MatchString(path) {
			// Skip files that are in subdirs.
			return
		}
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

			case "search-for-insert":
				return state.SearchForInsert(d)

			case "search-for-delete":
				return state.SearchForDelete(d)

			case "insert":
				return state.Insert(d)

			case "delete":
				return state.Delete(d)

			case "force-split", "force-merge":
				return state.ForceSplitOrMerge(d)

			case "recall":
				return state.Recall(d)

			case "validate-tree":
				return state.ValidateTree(d)
			}

			t.Fatalf("unknown cmd: %s", d.Cmd)
			return ""
		})
	})
}

type testState struct {
	T          *testing.T
	Ctx        context.Context
	Workspace  workspace.T
	Stopper    *stop.Stopper
	Quantizer  quantize.Quantizer
	InMemStore *memstore.Store
	Index      *cspann.Index
	Options    cspann.IndexOptions
	Features   vector.Set
}

func (s *testState) NewIndex(d *datadriven.TestData) string {
	var err error
	dims := 2
	s.Options = cspann.IndexOptions{IsDeterministic: true}
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "min-partition-size":
			s.Options.MinPartitionSize = s.parseInt(arg)

		case "max-partition-size":
			s.Options.MaxPartitionSize = s.parseInt(arg)

		case "quality-samples":
			s.Options.QualitySamples = s.parseInt(arg)

		case "dims":
			dims = s.parseInt(arg)

		case "beam-size":
			s.Options.BaseBeamSize = s.parseInt(arg)
		}
	}

	s.Quantizer = quantize.NewRaBitQuantizer(dims, 42)
	s.InMemStore = memstore.New(dims, 42)
	s.Index, err = cspann.NewIndex(s.Ctx, s.InMemStore, s.Quantizer, 42, &s.Options, s.Stopper)
	require.NoError(s.T, err)

	// Suspend background fixups until ProcessFixups is explicitly called, so
	// that vector index operations can be deterministic.
	s.Index.SuspendFixups()

	// Insert initial vectors.
	return s.Insert(d)
}

func (s *testState) FormatTree(d *datadriven.TestData) string {
	txn := commontest.BeginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commontest.CommitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	var idxCtx cspann.Context
	idxCtx.Init(txn)
	tree, err := s.Index.Format(
		s.Ctx, &idxCtx, treeKey, cspann.FormatOptions{PrimaryKeyStrings: true})
	require.NoError(s.T, err)
	return tree
}

func (s *testState) Search(d *datadriven.TestData) string {
	var vec vector.T
	searchSet := cspann.SearchSet{MaxResults: 1}
	options := cspann.SearchOptions{}

	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "use-feature":
			vec = s.parseUseFeature(arg)

		case "max-results":
			searchSet.MaxResults = s.parseInt(arg)

		case "beam-size":
			options.BaseBeamSize = s.parseInt(arg)

		case "skip-rerank":
			options.SkipRerank = s.parseFlag(arg)

		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	if vec == nil {
		// Parse input as the vector to search for.
		vec = s.parseVector(d.Input)
	}

	// Search the index within a transaction.
	var idxCtx cspann.Context
	txn := commontest.BeginTransaction(s.Ctx, s.T, s.InMemStore)
	idxCtx.Init(txn)
	err := s.Index.Search(s.Ctx, &idxCtx, treeKey, vec, &searchSet, options)
	require.NoError(s.T, err)
	commontest.CommitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	var buf bytes.Buffer
	results := searchSet.PopResults()
	for i := range results {
		result := &results[i]
		var errorBound string
		if result.ErrorBound != 0 {
			errorBound = fmt.Sprintf("±%s ", utils.FormatFloat(result.ErrorBound, 2))
		}
		fmt.Fprintf(&buf, "%s: %s %s(centroid=%s)\n",
			string(result.ChildKey.KeyBytes), utils.FormatFloat(result.QuerySquaredDistance, 4),
			errorBound, utils.FormatFloat(result.CentroidDistance, 2))
	}

	buf.WriteString(fmt.Sprintf("%d leaf vectors, ", searchSet.Stats.QuantizedLeafVectorCount))
	buf.WriteString(fmt.Sprintf("%d vectors, ", searchSet.Stats.QuantizedVectorCount))
	buf.WriteString(fmt.Sprintf("%d full vectors, ", searchSet.Stats.FullVectorCount))
	buf.WriteString(fmt.Sprintf("%d partitions", searchSet.Stats.PartitionCount))

	// Handle any fixups triggered by the search.
	s.Index.ProcessFixups()

	return buf.String()
}

func (s *testState) SearchForInsert(d *datadriven.TestData) string {
	var vec vector.T

	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "use-feature":
			vec = s.parseUseFeature(arg)

		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	if vec == nil {
		// Parse input as the vector to search for.
		vec = s.parseVector(d.Input)
	}

	// Search the index within a transaction.
	var idxCtx cspann.Context
	txn := commontest.BeginTransaction(s.Ctx, s.T, s.InMemStore)
	idxCtx.Init(txn)
	result, err := s.Index.SearchForInsert(s.Ctx, &idxCtx, treeKey, vec)
	require.NoError(s.T, err)
	commontest.CommitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "partition %d, centroid=", result.ChildKey.PartitionKey)

	// Un-randomize the centroid and write it to buffer.
	original := make(vector.T, len(result.Vector))
	s.Index.UnRandomizeVector(result.Vector, original)
	utils.WriteVector(&buf, original, 4)

	fmt.Fprintf(&buf, ", sqdist=%s", utils.FormatFloat(result.QuerySquaredDistance, 4))
	if result.ErrorBound != 0 {
		fmt.Fprintf(&buf, "±%s", utils.FormatFloat(result.ErrorBound, 2))
	}
	buf.WriteByte('\n')

	// Handle any fixups triggered by the search.
	s.Index.ProcessFixups()

	return buf.String()
}

func (s *testState) SearchForDelete(d *datadriven.TestData) string {
	var buf bytes.Buffer
	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	var idxCtx cspann.Context
	for _, line := range strings.Split(d.Input, "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		key, vec := s.parseKeyAndVector(line)

		// Search within a transaction.
		txn := commontest.BeginTransaction(s.Ctx, s.T, s.InMemStore)
		idxCtx.Init(txn)
		result, err := s.Index.SearchForDelete(s.Ctx, &idxCtx, treeKey, vec, key)
		require.NoError(s.T, err)
		commontest.CommitTransaction(s.Ctx, s.T, s.InMemStore, txn)

		if result == nil {
			fmt.Fprintf(&buf, "%s: vector not found\n", string(key))
		} else {
			fmt.Fprintf(&buf, "%s: partition %d\n", string(key), result.ParentPartitionKey)
		}
	}

	// Handle any fixups triggered by the search.
	s.Index.ProcessFixups()

	return buf.String()
}

func (s *testState) Insert(d *datadriven.TestData) string {
	hideTree := false
	noFixups := false
	count := 0
	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "load-features":
			count = s.parseInt(arg)

		case "hide-tree":
			hideTree = s.parseFlag(arg)

		case "no-fixups":
			noFixups = s.parseFlag(arg)

		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	vectors := vector.MakeSet(s.Quantizer.GetDims())
	childKeys := make([]cspann.ChildKey, 0, count)
	if count != 0 {
		// Load features.
		s.Features = testutils.LoadFeatures(s.T, 10000)
		vectors = s.Features
		vectors.SplitAt(count)
		for i := 0; i < count; i++ {
			key := cspann.KeyBytes(fmt.Sprintf("vec%d", i))
			childKeys = append(childKeys, cspann.ChildKey{KeyBytes: key})
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
			key := cspann.KeyBytes(parts[0])
			childKeys = append(childKeys, cspann.ChildKey{KeyBytes: key})
		}
	}

	var idxCtx cspann.Context
	var wait sync.WaitGroup
	step := (s.Options.MinPartitionSize + s.Options.MaxPartitionSize) / 2
	for i := 0; i < vectors.Count; i++ {
		// Insert within the scope of a transaction.
		txn := commontest.BeginTransaction(s.Ctx, s.T, s.InMemStore)
		idxCtx.Init(txn)
		s.InMemStore.InsertVector(childKeys[i].KeyBytes, vectors.At(i))
		require.NoError(s.T,
			s.Index.Insert(s.Ctx, &idxCtx, treeKey, vectors.At(i), childKeys[i].KeyBytes))
		commontest.CommitTransaction(s.Ctx, s.T, s.InMemStore, txn)

		if (i+1)%step == 0 && !noFixups {
			// Run synchronous fixups so that test results are deterministic.
			s.Index.ProcessFixups()
		}
	}
	wait.Wait()

	if !noFixups {
		// Handle any remaining fixups.
		s.Index.ProcessFixups()
	}

	if hideTree {
		str := fmt.Sprintf("Created index with %d vectors with %d dimensions.\n",
			vectors.Count, vectors.Dims)
		return str + s.Index.FormatStats()
	}

	return s.FormatTree(d)
}

func (s *testState) Delete(d *datadriven.TestData) string {
	notFound := false
	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "not-found":
			notFound = s.parseFlag(arg)

		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	var idxCtx cspann.Context
	for i, line := range strings.Split(d.Input, "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		key, vec := s.parseKeyAndVector(line)

		// Delete within the scope of a transaction.
		txn := commontest.BeginTransaction(s.Ctx, s.T, s.InMemStore)

		// If notFound=true, then simulate case where the vector is deleted in
		// the primary index, but it cannot be found in the secondary index.
		if !notFound {
			idxCtx.Init(txn)
			err := s.Index.Delete(s.Ctx, &idxCtx, treeKey, vec, key)
			require.NoError(s.T, err)
		}
		s.InMemStore.DeleteVector(key)

		commontest.CommitTransaction(s.Ctx, s.T, s.InMemStore, txn)

		if (i+1)%s.Options.MaxPartitionSize == 0 {
			// Run synchronous fixups so that test results are deterministic.
			s.Index.ProcessFixups()
		}
	}

	// Handle any remaining fixups.
	s.Index.ProcessFixups()

	return s.FormatTree(d)
}

func (s *testState) ForceSplitOrMerge(d *datadriven.TestData) string {
	var parentPartitionKey, partitionKey cspann.PartitionKey
	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "parent-partition-key":
			parentPartitionKey = cspann.PartitionKey(s.parseInt(arg))

		case "partition-key":
			partitionKey = cspann.PartitionKey(s.parseInt(arg))

		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	if d.Cmd == "force-split" {
		s.Index.ForceSplit(s.Ctx, treeKey, parentPartitionKey, partitionKey)
	} else {
		s.Index.ForceMerge(s.Ctx, treeKey, parentPartitionKey, partitionKey)
	}

	// Ensure the fixup runs.
	s.Index.ProcessFixups()

	return s.FormatTree(d)
}

func (s *testState) Recall(d *datadriven.TestData) string {
	searchSet := cspann.SearchSet{MaxResults: 1}
	options := cspann.SearchOptions{}
	numSamples := 50
	var samples []int
	seed := 42
	var err error
	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "use-feature":
			// Use single designated sample.
			offset := s.parseInt(arg)
			numSamples = 1
			samples = []int{offset}

		case "samples":
			numSamples = s.parseInt(arg)

		case "seed":
			seed = s.parseInt(arg)

		case "topk":
			searchSet.MaxResults = s.parseInt(arg)

		case "beam-size":
			options.BaseBeamSize = s.parseInt(arg)

		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	data := s.InMemStore.GetAllVectors()

	// Construct list of feature offsets.
	if samples == nil {
		// Shuffle the remaining features.
		rng := rand.New(rand.NewSource(int64(seed)))
		remaining := make([]int, s.Features.Count-len(data))
		for i := range remaining {
			remaining[i] = i
		}
		rng.Shuffle(len(remaining), func(i, j int) {
			remaining[i], remaining[j] = remaining[j], remaining[i]
		})

		// Pick numSamples randomly from the remaining set
		samples = make([]int, numSamples)
		copy(samples, remaining[:numSamples])
	}

	txn := commontest.BeginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commontest.CommitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	// calcTruth calculates the true nearest neighbors for the query vector.
	calcTruth := func(queryVector vector.T, data []cspann.VectorWithKey) []cspann.KeyBytes {
		distances := make([]float32, len(data))
		offsets := make([]int, len(data))
		for i := 0; i < len(data); i++ {
			distances[i] = num32.L2SquaredDistance(queryVector, data[i].Vector)
			offsets[i] = i
		}
		sort.SliceStable(offsets, func(i int, j int) bool {
			res := cmp.Compare(distances[offsets[i]], distances[offsets[j]])
			if res != 0 {
				return res < 0
			}
			return data[offsets[i]].Key.Compare(data[offsets[j]].Key) < 0
		})

		truth := make([]cspann.KeyBytes, searchSet.MaxResults)
		for i := 0; i < len(truth); i++ {
			truth[i] = data[offsets[i]].Key.KeyBytes
		}
		return truth
	}

	// Search for sampled features.
	var idxCtx cspann.Context
	idxCtx.Init(txn)
	var sumMAP float64
	for i := range samples {
		// Calculate truth set for the vector.
		queryVector := s.Features.At(samples[i])
		truth := calcTruth(queryVector, data)

		// Calculate prediction set for the vector.
		err = s.Index.Search(s.Ctx, &idxCtx, treeKey, queryVector, &searchSet, options)
		require.NoError(s.T, err)
		results := searchSet.PopResults()

		prediction := make([]cspann.KeyBytes, searchSet.MaxResults)
		for res := 0; res < len(results); res++ {
			prediction[res] = results[res].ChildKey.KeyBytes
		}

		sumMAP += findMAP(prediction, truth)
	}

	recall := sumMAP / float64(numSamples) * 100
	quantizedLeafVectors := float64(searchSet.Stats.QuantizedLeafVectorCount) / float64(numSamples)
	quantizedVectors := float64(searchSet.Stats.QuantizedVectorCount) / float64(numSamples)
	fullVectors := float64(searchSet.Stats.FullVectorCount) / float64(numSamples)
	partitions := float64(searchSet.Stats.PartitionCount) / float64(numSamples)

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%.2f%% recall@%d\n", recall, searchSet.MaxResults))
	buf.WriteString(fmt.Sprintf("%.2f leaf vectors, ", quantizedLeafVectors))
	buf.WriteString(fmt.Sprintf("%.2f vectors, ", quantizedVectors))
	buf.WriteString(fmt.Sprintf("%.2f full vectors, ", fullVectors))
	buf.WriteString(fmt.Sprintf("%.2f partitions", partitions))
	return buf.String()
}

func (s *testState) ValidateTree(d *datadriven.TestData) string {
	txn := commontest.BeginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commontest.CommitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	var treeKey cspann.TreeKey
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "tree":
			treeKey = s.parseTreeID(arg)
		}
	}

	vectorCount := 0
	partitionKeys := []cspann.PartitionKey{cspann.RootKey}
	for {
		// Get all child keys for next level.
		var childKeys []cspann.ChildKey
		for _, key := range partitionKeys {
			partition, err := txn.GetPartition(s.Ctx, treeKey, key)
			require.NoError(s.T, err)
			childKeys = append(childKeys, partition.ChildKeys()...)
		}

		if len(childKeys) == 0 {
			break
		}

		// Verify full vectors exist for the level.
		refs := make([]cspann.VectorWithKey, len(childKeys))
		for i := range childKeys {
			refs[i].Key = childKeys[i]
		}
		err := txn.GetFullVectors(s.Ctx, treeKey, refs)
		require.NoError(s.T, err)
		for i := range refs {
			require.NotNil(s.T, refs[i].Vector)
		}

		// If this is not the leaf level, then process the next level.
		if childKeys[0].KeyBytes == nil {
			partitionKeys = make([]cspann.PartitionKey, len(childKeys))
			for i := range childKeys {
				partitionKeys[i] = childKeys[i].PartitionKey
			}
		} else {
			// This is the leaf level, so count vectors and end.
			vectorCount += len(childKeys)
			break
		}
	}

	return fmt.Sprintf("Validated index with %d vectors.\n", vectorCount)
}

func (s *testState) parseInt(arg datadriven.CmdArg) int {
	require.Len(s.T, arg.Vals, 1)
	val, err := strconv.Atoi(arg.Vals[0])
	require.NoError(s.T, err)
	return val
}

func (s *testState) parseFlag(arg datadriven.CmdArg) bool {
	require.Len(s.T, arg.Vals, 0)
	return true
}

func (s *testState) parseTreeID(arg datadriven.CmdArg) cspann.TreeKey {
	return memstore.ToTreeKey(memstore.TreeID(s.parseInt(arg)))
}

func (s *testState) parseUseFeature(arg datadriven.CmdArg) vector.T {
	return s.Features.At(s.parseInt(arg))
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

// parseKeyAndVector parses a line that may contain a key and vector separated
// by a colon. If there's no colon, it treats the line as just a key and gets
// the vector from the store.
func (s *testState) parseKeyAndVector(line string) (cspann.KeyBytes, vector.T) {
	parts := strings.Split(line, ":")
	if len(parts) == 1 {
		// Get the value from the store.
		key := cspann.KeyBytes(line)
		return key, s.InMemStore.GetVector(key)
	}

	// Parse the value after the colon.
	require.Len(s.T, parts, 2)
	key := cspann.KeyBytes(parts[0])
	return key, s.parseVector(parts[1])
}

// findMAP returns mean average precision, which compares a set of predicted
// results with the true set of results. Both sets are expected to be of equal
// length. It returns the percentage overlap of the predicted set with the truth
// set.
func findMAP(prediction, truth []cspann.KeyBytes) float64 {
	if len(prediction) != len(truth) {
		panic(errors.AssertionFailedf("prediction and truth sets are not same length"))
	}

	predictionMap := make(map[string]bool, len(prediction))
	for _, p := range prediction {
		predictionMap[string(p)] = true
	}

	var intersect float64
	for _, t := range truth {
		_, ok := predictionMap[string(t)]
		if ok {
			intersect++
		}
	}
	return intersect / float64(len(truth))
}

func TestRandomizeVector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create index.
	var workspace workspace.T
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	const dims = 97
	const count = 5
	quantizer := quantize.NewRaBitQuantizer(dims, 46)
	inMemStore := memstore.New(dims, 42)
	index, err := cspann.NewIndex(ctx, inMemStore, quantizer, 42, &cspann.IndexOptions{}, stopper)
	require.NoError(t, err)

	// Generate random vectors with exponentially increasing norms, in order
	// make distances more distinct.
	rng := rand.New(rand.NewSource(42))
	data := make([]float32, dims*count)
	for i := range data {
		vecIdx := float64(i / dims)
		data[i] = float32(rng.NormFloat64() * math.Pow(1.5, vecIdx))
	}

	original := vector.MakeSetFromRawData(data, dims)
	randomized := vector.MakeSet(dims)
	randomized.AddUndefined(count)
	for i := range original.Count {
		index.RandomizeVector(original.At(i), randomized.At(i))

		// Ensure that calling unRandomizeVector recovers original vector.
		randomizedInv := make([]float32, dims)
		index.UnRandomizeVector(randomized.At(i), randomizedInv)
		for j, val := range original.At(i) {
			require.InDelta(t, val, randomizedInv[j], 0.00001)
		}
	}

	// Ensure that distances are similar, whether using the original vectors or
	// the randomized vectors.
	originalSet := quantizer.Quantize(&workspace, original).(*quantize.RaBitQuantizedVectorSet)
	randomizedSet := quantizer.Quantize(&workspace, randomized).(*quantize.RaBitQuantizedVectorSet)

	distances := make([]float32, count)
	errorBounds := make([]float32, count)
	quantizer.EstimateSquaredDistances(&workspace, originalSet, original.At(0), distances, errorBounds)
	require.Equal(t, []float32{0, 272.75, 550.86, 950.93, 2421.41}, testutils.RoundFloats(distances, 2))
	require.Equal(t, []float32{37.58, 46.08, 57.55, 69.46, 110.57}, testutils.RoundFloats(errorBounds, 2))

	quantizer.EstimateSquaredDistances(&workspace, randomizedSet, randomized.At(0), distances, errorBounds)
	require.Equal(t, []float32{5.1, 292.72, 454.95, 1011.85, 2475.87}, testutils.RoundFloats(distances, 2))
	require.Equal(t, []float32{37.58, 46.08, 57.55, 69.46, 110.57}, testutils.RoundFloats(errorBounds, 2))
}

// TestIndexConcurrency builds an index on multiple goroutines, with background
// splits and merges enabled.
func TestIndexConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create index.
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Load features.
	vectors := testutils.LoadFeatures(t, 100)

	primaryKeys := make([]cspann.KeyBytes, vectors.Count)
	for i := 0; i < vectors.Count; i++ {
		primaryKeys[i] = cspann.KeyBytes(fmt.Sprintf("vec%d", i))
	}

	for i := 0; i < 10; i++ {
		options := cspann.IndexOptions{
			MinPartitionSize: 2,
			MaxPartitionSize: 8,
			BaseBeamSize:     2,
			QualitySamples:   4,
		}
		seed := int64(i)
		store := memstore.New(vectors.Dims, seed)
		quantizer := quantize.NewRaBitQuantizer(vectors.Dims, seed)
		index, err := cspann.NewIndex(ctx, store, quantizer, seed, &options, stopper)
		require.NoError(t, err)

		buildIndex(ctx, t, store, index, vectors, primaryKeys)

		vectorCount := validateIndex(ctx, t, store)
		require.Equal(t, vectors.Count, vectorCount)

		index.Close()
	}
}

func buildIndex(
	ctx context.Context,
	t *testing.T,
	store *memstore.Store,
	index *cspann.Index,
	vectors vector.Set,
	primaryKeys []cspann.KeyBytes,
) {
	// Insert block of vectors within the scope of a transaction.
	insertBlock := func(idxCtx *cspann.Context, start, end int) {
		for i := start; i < end; i++ {
			txn := commontest.BeginTransaction(ctx, t, store)
			idxCtx.Init(txn)
			store.InsertVector(primaryKeys[i], vectors.At(i))
			require.NoError(t,
				index.Insert(ctx, idxCtx, nil /* treeKey */, vectors.At(i), primaryKeys[i]))
			commontest.CommitTransaction(ctx, t, store, txn)
		}
	}

	// Insert vectors into the store on multiple goroutines.
	var wait sync.WaitGroup
	procs := runtime.GOMAXPROCS(-1)
	countPerProc := (vectors.Count + procs) / procs
	blockSize := index.Options().MinPartitionSize
	for i := 0; i < vectors.Count; i += countPerProc {
		end := min(i+countPerProc, vectors.Count)
		wait.Add(1)
		go func(start, end int) {
			// Break vector group into individual transactions that each insert a
			// block of vectors. Run any pending fixups after each block.
			var idxCtx cspann.Context
			for j := start; j < end; j += blockSize {
				insertBlock(&idxCtx, j, min(j+blockSize, end))
			}

			wait.Done()
		}(i, end)
	}
	wait.Wait()

	// Process any remaining fixups.
	index.ProcessFixups()
}

func validateIndex(ctx context.Context, t *testing.T, store *memstore.Store) int {
	txn := commontest.BeginTransaction(ctx, t, store)
	defer commontest.CommitTransaction(ctx, t, store, txn)
	treeKey := memstore.ToTreeKey(memstore.TreeID(0))

	vectorCount := 0
	partitionKeys := []cspann.PartitionKey{cspann.RootKey}
	for {
		// Get all child keys for next level.
		var childKeys []cspann.ChildKey
		for _, key := range partitionKeys {
			partition, err := txn.GetPartition(ctx, treeKey, key)
			require.NoError(t, err)
			childKeys = append(childKeys, partition.ChildKeys()...)
		}

		if len(childKeys) == 0 {
			break
		}

		// Verify full vectors exist for the level.
		refs := make([]cspann.VectorWithKey, len(childKeys))
		for i := range childKeys {
			refs[i].Key = childKeys[i]
		}
		err := txn.GetFullVectors(ctx, treeKey, refs)
		require.NoError(t, err)
		for i := range refs {
			if refs[i].Vector == nil {
				panic("vector is nil")
			}
			require.NotNil(t, refs[i].Vector)
		}

		// If this is not the leaf level, then process the next level.
		if childKeys[0].KeyBytes == nil {
			partitionKeys = make([]cspann.PartitionKey, len(childKeys))
			for i := range childKeys {
				partitionKeys[i] = childKeys[i].PartitionKey
			}
		} else {
			// This is the leaf level, so count vectors and end.
			vectorCount += len(childKeys)
			break
		}
	}

	return vectorCount
}
