// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/datadriven"
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

			case "insert":
				return state.Insert(d)

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
	Options    VectorIndexOptions
	Features   vector.Set
}

func (s *testState) NewIndex(d *datadriven.TestData) string {
	var err error
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
		}
	}

	s.Quantizer = quantize.NewRaBitQuantizer(dims, 42)
	s.InMemStore = vecstore.NewInMemoryStore(dims, 42)
	s.Index, err = NewVectorIndex(s.Ctx, s.InMemStore, s.Quantizer, &s.Options)
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

	// Insert vectors into the store.
	for i := 0; i < vectors.Count; i++ {
		// Insert within the scope of a transaction.
		txn := beginTransaction(s.Ctx, s.T, s.InMemStore)
		s.InMemStore.InsertVector(txn, childKeys[i].PrimaryKey, vectors.At(i))
		require.NoError(s.T, s.Index.Insert(s.Ctx, txn, vectors.At(i), childKeys[i].PrimaryKey))
		commitTransaction(s.Ctx, s.T, s.InMemStore, txn)

		if (i+1)%s.Options.MaxPartitionSize == 0 {
			// Periodically, run synchronous fixups so that test results are
			// deterministic.
			require.NoError(s.T, s.Index.fixups.runAll(s.Ctx))
		}
	}

	// Handle any remaining fixups.
	require.NoError(s.T, s.Index.fixups.runAll(s.Ctx))

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
