// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})
	state := testState{T: t, Ctx: ctx, Stopper: stop.NewStopper()}
	defer state.Stopper.Stop(ctx)

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

			case "insert":
				return state.Insert(d)

			case "delete":
				return state.Delete(d)

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
	Stopper    *stop.Stopper
	Quantizer  quantize.Quantizer
	InMemStore *vecstore.InMemoryStore
	Index      *VectorIndex
	Options    VectorIndexOptions
	Features   vector.Set
}

func (s *testState) NewIndex(d *datadriven.TestData) string {
	var err error
	var stopper *stop.Stopper
	dims := 2
	s.Options = VectorIndexOptions{Seed: 42}
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "min-partition-size":
			require.Len(s.T, arg.Vals, 1)
			s.Options.MinPartitionSize, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "max-partition-size":
			require.Len(s.T, arg.Vals, 1)
			s.Options.MaxPartitionSize, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "quality-samples":
			require.Len(s.T, arg.Vals, 1)
			s.Options.QualitySamples, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "dims":
			require.Len(s.T, arg.Vals, 1)
			dims, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "beam-size":
			require.Len(s.T, arg.Vals, 1)
			s.Options.BaseBeamSize, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "background-fixups":
			require.Len(s.T, arg.Vals, 0)
			stopper = s.Stopper
		}
	}

	s.Quantizer = quantize.NewRaBitQuantizer(dims, 42)
	s.InMemStore = vecstore.NewInMemoryStore(dims, 42)
	s.Index, err = NewVectorIndex(s.Ctx, s.InMemStore, s.Quantizer, &s.Options, stopper)
	require.NoError(s.T, err)

	// Insert empty root partition.
	txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
	require.NoError(s.T, s.Index.CreateRoot(s.Ctx, txn))
	commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	// Insert initial vectors.
	return s.Insert(d)
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

func (s *testState) Insert(d *datadriven.TestData) string {
	var err error
	hideTree := false
	count := 0
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "load-features":
			require.Len(s.T, arg.Vals, 1)
			count, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "hide-tree":
			require.Len(s.T, arg.Vals, 0)
			hideTree = true
		}
	}

	vectors := vector.MakeSet(s.Quantizer.GetRandomDims())
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

	// Insert vectors into the store in randomly-sized blocks.
	var rng *rand.Rand
	if s.Index.cancel == nil {
		// Block size needs to be deterministic.
		rng = rand.New(rand.NewSource(42))
	} else {
		rng = rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	}
	var wait sync.WaitGroup
	i := 0
	for i < vectors.Count {
		step := rng.Intn(s.Options.MaxPartitionSize*2) + 1

		// Insert block of vectors within the scope of a transaction.
		insertBlock := func(start, end int) {
			txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
			for j := start; j < end; j++ {
				s.InMemStore.InsertVector(txn, childKeys[j].PrimaryKey, vectors.At(j))
				require.NoError(s.T, s.Index.Insert(s.Ctx, txn, vectors.At(j), childKeys[j].PrimaryKey))
			}
			commitTransaction(s.Ctx, s.T, s.InMemStore, txn)
		}

		// If background fixups are not enabled, do inserts in series, since the
		// test needs to be deterministic.
		end := min(i+step, vectors.Count)
		if s.Index.cancel == nil {
			insertBlock(i, end)

			// Run synchronous fixups so that test results are deterministic.
			require.NoError(s.T, s.runAllFixups())
		} else {
			// Run inserts in parallel.
			wait.Add(1)
			go func(i int) {
				insertBlock(i, end)
				wait.Done()
			}(i)
		}

		i += step
	}
	wait.Wait()

	// Handle any remaining fixups.
	require.NoError(s.T, s.runAllFixups())

	if hideTree {
		return fmt.Sprintf("Created index with %d vectors with %d dimensions.\n",
			vectors.Count, vectors.Dims)
	}

	return s.FormatTree(d)
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

	for i, line := range strings.Split(d.Input, "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// If vector to delete has a colon, then its value is specified as well
		// as its name. This is useful for forcing a certain value to delete.
		var key vecstore.PrimaryKey
		var vec vector.T
		parts := strings.Split(line, ":")
		if len(parts) == 1 {
			// Get the value from the store.
			key = vecstore.PrimaryKey(line)
			vec = s.InMemStore.GetVector(key)
		} else {
			require.Len(s.T, parts, 2)
			// Parse the value after the colon.
			key = vecstore.PrimaryKey(parts[0])
			vec = s.parseVector(parts[1])
		}

		// Delete within the scope of a transaction.
		txn := beginTransaction(s.Ctx, s.T, s.InMemStore)

		// If notFound=true, then simulate case where the vector is deleted in
		// the primary index, but it cannot be found in the secondary index.
		if !notFound {
			err := s.Index.Delete(s.Ctx, txn, vec, key)
			require.NoError(s.T, err)
		}
		s.InMemStore.DeleteVector(txn, key)

		commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

		if (i+1)%s.Options.MaxPartitionSize == 0 {
			// Periodically, run synchronous fixups so that test results are
			// deterministic.
			require.NoError(s.T, s.Index.fixups.runAll(s.Ctx))
		}
	}

	// Handle any remaining fixups.
	require.NoError(s.T, s.Index.fixups.runAll(s.Ctx))

	return s.FormatTree(d)
}

func (s *testState) Recall(d *datadriven.TestData) string {
	searchSet := vecstore.SearchSet{MaxResults: 1}
	options := SearchOptions{}
	samples := 50
	var err error
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "samples":
			require.Len(s.T, arg.Vals, 1)
			samples, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "topk":
			require.Len(s.T, arg.Vals, 1)
			searchSet.MaxResults, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)

		case "beam-size":
			require.Len(s.T, arg.Vals, 1)
			options.BaseBeamSize, err = strconv.Atoi(arg.Vals[0])
			require.NoError(s.T, err)
		}
	}

	txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	// calcTruth calculates the true nearest neighbors for the query vector.
	calcTruth := func(queryVector vector.T, data []vecstore.VectorWithKey) []vecstore.PrimaryKey {
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

		truth := make([]vecstore.PrimaryKey, searchSet.MaxResults)
		for i := 0; i < len(truth); i++ {
			truth[i] = data[offsets[i]].Key.PrimaryKey
		}
		return truth
	}

	data := s.InMemStore.GetAllVectors()

	// Search for last "samples" features.
	var sumMAP float64
	for feature := s.Features.Count - samples; feature < s.Features.Count; feature++ {
		// Calculate truth set for the vector.
		queryVector := s.Features.At(feature)
		truth := calcTruth(queryVector, data)

		// Calculate prediction set for the vector.
		err = s.Index.Search(s.Ctx, txn, queryVector, &searchSet, options)
		require.NoError(s.T, err)
		results := searchSet.PopResults()

		prediction := make([]vecstore.PrimaryKey, searchSet.MaxResults)
		for res := 0; res < len(results); res++ {
			prediction[res] = results[res].ChildKey.PrimaryKey
		}

		sumMAP += findMAP(prediction, truth)
	}

	recall := sumMAP / float64(samples) * 100
	quantizedLeafVectors := float64(searchSet.Stats.QuantizedLeafVectorCount) / float64(samples)
	quantizedVectors := float64(searchSet.Stats.QuantizedVectorCount) / float64(samples)
	fullVectors := float64(searchSet.Stats.FullVectorCount) / float64(samples)
	partitions := float64(searchSet.Stats.PartitionCount) / float64(samples)

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%.2f%% recall@%d\n", recall, searchSet.MaxResults))
	buf.WriteString(fmt.Sprintf("%.2f leaf vectors, ", quantizedLeafVectors))
	buf.WriteString(fmt.Sprintf("%.2f vectors, ", quantizedVectors))
	buf.WriteString(fmt.Sprintf("%.2f full vectors, ", fullVectors))
	buf.WriteString(fmt.Sprintf("%.2f partitions", partitions))
	return buf.String()
}

func (s *testState) ValidateTree(d *datadriven.TestData) string {
	txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
	defer commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

	vectorCount := 0
	partitionKeys := []vecstore.PartitionKey{vecstore.RootKey}
	for {
		// Get all child keys for next level.
		var childKeys []vecstore.ChildKey
		for _, key := range partitionKeys {
			partition, err := s.InMemStore.GetPartition(s.Ctx, txn, key)
			require.NoError(s.T, err)
			childKeys = append(childKeys, partition.ChildKeys()...)
		}

		if len(childKeys) == 0 {
			break
		}

		// Verify full vectors exist for the level.
		refs := make([]vecstore.VectorWithKey, len(childKeys))
		for i := range childKeys {
			refs[i].Key = childKeys[i]
		}
		err := s.InMemStore.GetFullVectors(s.Ctx, txn, refs)
		require.NoError(s.T, err)
		for i := range refs {
			require.NotNil(s.T, refs[i].Vector)
		}

		// If this is not the leaf level, then process the next level.
		if childKeys[0].PrimaryKey == nil {
			partitionKeys = make([]vecstore.PartitionKey, len(childKeys))
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

// runAllFixups forces all pending fixups to be processed.
func (s *testState) runAllFixups() error {
	if s.Index.cancel != nil {
		// Background fixup goroutine is running, so wait until it has processed
		// all fixups.
		s.Index.fixups.Wait()
		return nil
	}
	// Synchronously run fixups.
	return s.Index.fixups.runAll(s.Ctx)
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

func beginTransaction(ctx context.Context, t *testing.T, store vecstore.Store) vecstore.Txn {
	txn, err := store.BeginTransaction(ctx)
	require.NoError(t, err)
	return txn
}

func commitTransaction(ctx context.Context, t *testing.T, store vecstore.Store, txn vecstore.Txn) {
	err := store.CommitTransaction(ctx, txn)
	require.NoError(t, err)
}

// findMAP returns mean average precision, which compares a set of predicted
// results with the true set of results. Both sets are expected to be of equal
// length. It returns the percentage overlap of the predicted set with the truth
// set.
func findMAP(prediction, truth []vecstore.PrimaryKey) float64 {
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
