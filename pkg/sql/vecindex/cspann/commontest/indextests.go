// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commontest

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestIndex abstracts operations needed by datadriven index tests that use the
// IndexTestState helper.
type TestIndex interface {
	// MakeNewIndex returns a newly constructed index with the given options.
	MakeNewIndex(
		ctx context.Context, dims int, metric vecpb.DistanceMetric, options *cspann.IndexOptions,
	) *cspann.Index

	// InsertVectors inserts the given set of vectors into the index. Each vector
	// is identified by a unique string key.
	InsertVectors(
		ctx context.Context, treeKey cspann.TreeKey, keys []string, vectors vector.Set,
	)

	// SearchVectors searches the index for the query vector, returning the key
	// values of the top "k" nearest vectors.
	SearchVectors(
		ctx context.Context,
		treeKey cspann.TreeKey,
		queryVector vector.T,
		beamSize, topK, rerankMultiplier int,
	) []string
}

// IndexTestState is a helper that constructs state used by index tests.
type IndexTestState struct {
	T         *testing.T
	Index     *cspann.Index
	Dataset   vector.Set
	TrainKeys []string

	testIndex TestIndex
}

// NewIndexTestState constructs a new IndexTestState for the given TestIndex.
func NewIndexTestState(t *testing.T, testIndex TestIndex) *IndexTestState {
	return &IndexTestState{
		T:         t,
		testIndex: testIndex,
	}
}

// NewIndex runs the "new-index" command.
func (s *IndexTestState) NewIndex(
	ctx context.Context, d *datadriven.TestData, treeKey cspann.TreeKey,
) int {
	var err error
	dims := 0
	datasetName := ""
	trainCount := 0
	distanceMetric := vecpb.L2SquaredDistance
	options := cspann.IndexOptions{
		RotAlgorithm:    vecpb.RotGivens,
		IsDeterministic: true,
		// Disable stalled op timeout, since it can interfere with stepping tests.
		StalledOpTimeout: func() time.Duration { return 0 },
		// Disable adaptive search for now, until it's fully supported for stores
		// other than the in-memory store.
		DisableAdaptiveSearch: true,
	}
	s.Dataset = vector.Set{}
	s.TrainKeys = nil

	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "dataset":
			require.Len(s.T, arg.Vals, 1)
			datasetName = arg.Vals[0]

		case "train-count":
			trainCount = testutils.ParseDataDrivenInt(s.T, arg)

		case "distance-metric":
			require.Len(s.T, arg.Vals, 1)
			switch strings.ToLower(arg.Vals[0]) {
			case "innerproduct":
				distanceMetric = vecpb.InnerProductDistance
			case "cosine":
				distanceMetric = vecpb.CosineDistance
			}
			require.NoError(s.T, err)

		case "rot-algorithm":
			require.Len(s.T, arg.Vals, 1)
			switch strings.ToLower(arg.Vals[0]) {
			case "matrix":
				options.RotAlgorithm = vecpb.RotMatrix
			case "givens":
				options.RotAlgorithm = vecpb.RotGivens
			case "none":
				options.RotAlgorithm = vecpb.RotNone
			default:
				require.Failf(s.T, "unrecognized rot algorithm %s", arg.Vals[0])
			}

		case "min-partition-size":
			options.MinPartitionSize = testutils.ParseDataDrivenInt(s.T, arg)

		case "max-partition-size":
			options.MaxPartitionSize = testutils.ParseDataDrivenInt(s.T, arg)

		case "quality-samples":
			options.QualitySamples = testutils.ParseDataDrivenInt(s.T, arg)

		case "dims":
			dims = testutils.ParseDataDrivenInt(s.T, arg)

		case "beam-size":
			options.BaseBeamSize = testutils.ParseDataDrivenInt(s.T, arg)

		case "read-only":
			options.ReadOnly = testutils.ParseDataDrivenFlag(s.T, arg)
		}
	}

	if datasetName != "" {
		dataset := testutils.LoadDataset(s.T, datasetName)

		if dims != 0 {
			// Trim dataset dimensions to make test run faster.
			s.Dataset = vector.MakeSet(min(dims, dataset.Dims))
			dims = s.Dataset.Dims
			for i := range dataset.Count {
				s.Dataset.Add(dataset.At(i)[:dims])
			}
		} else {
			s.Dataset = dataset
			dims = s.Dataset.Dims
		}
	} else if dims == 0 {
		// Default to 2 dimensions if not specified.
		dims = 2
	}

	s.Index = s.testIndex.MakeNewIndex(ctx, dims, distanceMetric, &options)

	if trainCount != 0 {
		// Insert train vectors into the index.
		vectors := s.Dataset.Slice(0, trainCount)
		s.TrainKeys = make([]string, 0, trainCount)
		for i := range trainCount {
			s.TrainKeys = append(s.TrainKeys, fmt.Sprintf("vec%d", i))
		}
		s.testIndex.InsertVectors(ctx, treeKey, s.TrainKeys, vectors)
	}

	return trainCount
}

// Insert runs the "insert" command.
func (s *IndexTestState) Insert(
	ctx context.Context, d *datadriven.TestData, treeKey cspann.TreeKey,
) int {
	var keys []string
	vectors := vector.MakeSet(s.Index.Quantizer().GetDims())

	// Parse vectors.
	for _, line := range strings.Split(d.Input, "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		parts := strings.Split(line, ":")
		require.Len(s.T, parts, 2)

		vec, err := vector.ParseVector(parts[1])
		require.NoError(s.T, err)
		vectors.Add(vec)
		keys = append(keys, parts[0])
	}

	s.testIndex.InsertVectors(ctx, treeKey, keys, vectors)

	return vectors.Count
}

// FormatTree runs the "format-tree" command.
func (s *IndexTestState) FormatTree(
	ctx context.Context, d *datadriven.TestData, treeKey cspann.TreeKey,
) string {
	var tree string
	RunTransaction(ctx, s.T, s.Index.Store(), func(txn cspann.Txn) {
		rootPartitionKey := cspann.RootKey
		for _, arg := range d.CmdArgs {
			switch arg.Key {
			case "root":
				rootPartitionKey = cspann.PartitionKey(testutils.ParseDataDrivenInt(s.T, arg))
			}
		}

		var err error
		options := cspann.FormatOptions{PrimaryKeyStrings: true, RootPartitionKey: rootPartitionKey}
		tree, err = s.Index.Format(ctx, treeKey, options)
		require.NoError(s.T, err)
	})
	return tree
}

// Recall runs the "rcall" command.
func (s *IndexTestState) Recall(
	ctx context.Context, d *datadriven.TestData, treeKey cspann.TreeKey,
) (topK, numSamples int, recall float64) {
	topK = 1
	numSamples = 50
	beamSize := 1
	rerankMultiplier := -1
	var samples []int
	seed := 42
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "use-dataset":
			// Use single designated sample.
			offset := testutils.ParseDataDrivenInt(s.T, arg)
			numSamples = 1
			samples = []int{offset}

		case "samples":
			numSamples = testutils.ParseDataDrivenInt(s.T, arg)

		case "seed":
			seed = testutils.ParseDataDrivenInt(s.T, arg)

		case "beam-size":
			beamSize = testutils.ParseDataDrivenInt(s.T, arg)

		case "topk":
			topK = testutils.ParseDataDrivenInt(s.T, arg)

		case "rerank-multiplier":
			rerankMultiplier = testutils.ParseDataDrivenInt(s.T, arg)
		}
	}

	dataVectors := s.Dataset.Slice(0, len(s.TrainKeys))

	// Construct random list of offsets into the test vectors in the dataset (i.e.
	// all vectors not part of the training set).
	if samples == nil {
		// Shuffle the remaining dataset vectors.
		rng := rand.New(rand.NewSource(int64(seed)))
		remaining := make([]int, s.Dataset.Count-len(s.TrainKeys))
		for i := range remaining {
			remaining[i] = len(s.TrainKeys) + i
		}
		rng.Shuffle(len(remaining), func(i, j int) {
			remaining[i], remaining[j] = remaining[j], remaining[i]
		})

		// Pick numSamples randomly from the remaining set
		samples = make([]int, numSamples)
		copy(samples, remaining[:numSamples])
	}

	// Search for sampled dataset vectors within a transaction.
	var sumRecall float64
	for i := range samples {
		// Calculate truth set for the vector.
		queryVector := s.Dataset.At(samples[i])

		truth := testutils.CalculateTruth(
			topK, s.Index.Quantizer().GetDistanceMetric(), queryVector, dataVectors, s.TrainKeys)

		prediction := s.testIndex.SearchVectors(
			ctx, treeKey, queryVector, beamSize, topK, rerankMultiplier)

		sumRecall += testutils.CalculateRecall(prediction, truth)
	}

	return topK, numSamples, sumRecall / float64(numSamples) * 100
}
