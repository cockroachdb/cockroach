// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestQuantizers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	state := testState{T: t, Ctx: ctx}

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "estimate-distances":
				return state.estimateDistances(t, d)

			case "calculate-recall":
				return state.calculateRecall(t, d)

			default:
				t.Fatalf("unknown cmd: %s", d.Cmd)
			}

			return ""
		})
	})
}

type testState struct {
	T         *testing.T
	Ctx       context.Context
	Workspace workspace.T
}

func (s *testState) estimateDistances(t *testing.T, d *datadriven.TestData) string {
	var queryVector vector.T
	var err error
	var vectors vector.Set
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "query":
			// Parse the query vector.
			queryVector, err = vector.ParseVector(arg.SingleVal(t))
			require.NoError(t, err)

			// Parse the input vectors.
			vectors = vector.MakeSet(len(queryVector))
			for _, line := range strings.Split(d.Input, "\n") {
				line = strings.TrimSpace(line)
				if len(line) == 0 {
					continue
				}

				vec, err := vector.ParseVector(line)
				require.NoError(t, err)
				vectors.Add(vec)
			}

		default:
			t.Fatalf("unknown arg: %s", arg.Key)
		}
	}

	addVectors := func(
		quantizer quantize.Quantizer, centroid vector.T, vectors vector.Set,
	) (quantizedSet quantize.QuantizedVectorSet, distances, errorBounds []float32) {
		// Quantize all vectors at once.
		quantizedSet = quantizer.Quantize(&s.Workspace, vectors)
		distances = make([]float32, quantizedSet.GetCount())
		errorBounds = make([]float32, quantizedSet.GetCount())
		quantizer.EstimateDistances(
			&s.Workspace, quantizedSet, queryVector, distances, errorBounds)

		// Now add the vectors one-by-one and ensure that distances and error
		// bounds stay the same.
		quantizedSet = quantizer.NewQuantizedVectorSet(vectors.Count, centroid)
		for i := range vectors.Count {
			quantizer.QuantizeInSet(&s.Workspace, quantizedSet, vectors.Slice(i, 1))
		}
		distances2 := make([]float32, quantizedSet.GetCount())
		errorBounds2 := make([]float32, quantizedSet.GetCount())
		quantizer.EstimateDistances(
			&s.Workspace, quantizedSet, queryVector, distances2, errorBounds2)
		require.Equal(t, distances2, distances)
		require.Equal(t, errorBounds2, errorBounds)

		return quantizedSet, distances, errorBounds
	}

	var buf bytes.Buffer
	doTest := func(metric vecdist.Metric, prec int) {
		centroid := vectors.Centroid(make(vector.T, vectors.Dims))

		// Test UnQuantizer.
		unQuantizer := quantize.NewUnQuantizer(len(queryVector), metric)
		quantizedSet, exact, errorBounds := addVectors(unQuantizer, centroid, vectors)
		unQuantizedSet := quantizedSet.(*quantize.UnQuantizedVectorSet)
		for _, error := range errorBounds {
			// ErrorBounds should always be zero for UnQuantizer.
			require.Zero(t, error)
		}

		// Test RaBitQuantizer.
		rabitQ := quantize.NewRaBitQuantizer(len(queryVector), 42, metric)
		quantizedSet, estimated, errorBounds := addVectors(rabitQ, centroid, vectors)
		rabitQSet := quantizedSet.(*quantize.RaBitQuantizedVectorSet)

		require.Equal(t, unQuantizedSet.GetCount(), rabitQSet.GetCount())

		buf.WriteString("  Query = ")
		utils.WriteVector(&buf, queryVector, 4)
		buf.WriteByte('\n')

		buf.WriteString("  Centroid = ")
		utils.WriteVector(&buf, rabitQSet.Centroid, 4)
		buf.WriteByte('\n')

		for i := range vectors.Count {
			var errorBound string
			if errorBounds[i] != 0 {
				errorBound = fmt.Sprintf(" ± %s", utils.FormatFloat(errorBounds[i], prec))
			}
			buf.WriteString("  ")
			utils.WriteVector(&buf, vectors.At(i), 4)
			fmt.Fprintf(&buf, ": exact is %s, estimate is %s%s\n",
				utils.FormatFloat(exact[i], 3), utils.FormatFloat(estimated[i], prec), errorBound)
		}
	}

	buf.WriteString("L2Squared\n")
	doTest(vecdist.L2Squared, 1)

	buf.WriteString("InnerProduct\n")
	doTest(vecdist.InnerProduct, 1)

	// For cosine distance, normalize the query and input vectors.
	num32.Normalize(queryVector)
	for i := range vectors.Count {
		num32.Normalize(vectors.At(i))
	}

	buf.WriteString("Cosine\n")
	doTest(vecdist.Cosine, 4)

	return buf.String()
}

func (s *testState) calculateRecall(t *testing.T, d *datadriven.TestData) string {
	var datasetName string
	randomize := false
	topK := 10
	count := 1000
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "dataset":
			require.Len(t, arg.Vals, 1)
			datasetName = arg.Vals[0]

		case "top-k":
			require.Len(t, arg.Vals, 1)
			val, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			topK = val

		case "randomize":
			require.Len(t, arg.Vals, 0)
			randomize = true

		case "count":
			require.Len(t, arg.Vals, 1)
			val, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			count = val
		}
	}

	// Use the first 98% of the vectors as data vectors and the other 2% as query
	// vectors.
	dataset := testutils.LoadDataset(t, datasetName+".gob")
	dataVectors := dataset.Slice(0, count*98/100)
	queryVectors := dataset.Slice(dataVectors.Count, count-dataVectors.Count)
	dataKeys := make([]int, dataVectors.Count)
	for i := range dataVectors.Count {
		dataKeys[i] = i
	}

	if randomize {
		var transform cspann.RandomOrthoTransformer
		transform.Init(cspann.RotGivens, dataset.Dims, 42)
		for i := range queryVectors.Count {
			transform.RandomizeVector(queryVectors.At(i), queryVectors.At(i))
		}
		for i := range dataVectors.Count {
			transform.RandomizeVector(dataVectors.At(i), dataVectors.At(i))
		}
	}

	calculateAvgRecall := func(metric vecdist.Metric) float64 {
		var recallSum float64
		var workspace workspace.T
		rabitQ := quantize.NewRaBitQuantizer(dataset.Dims, 42, metric)
		for i := range queryVectors.Count {
			query := queryVectors.At(i)
			rabitQSet := rabitQ.Quantize(&workspace, dataVectors)
			estimated := make([]float32, rabitQSet.GetCount())
			errorBounds := make([]float32, rabitQSet.GetCount())
			rabitQ.EstimateDistances(
				&workspace, rabitQSet, query, estimated, errorBounds)

			prediction := make([]int, len(estimated))
			for i := range prediction {
				prediction[i] = i
			}
			sort.Slice(prediction, func(i, j int) bool {
				return estimated[prediction[i]] < estimated[prediction[j]]
			})
			prediction = prediction[:topK]
			truth := testutils.CalculateTruth(topK, metric, query, dataVectors, dataKeys)
			recallSum += testutils.CalculateRecall(prediction, truth)
		}
		return recallSum / float64(queryVectors.Count)
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Euclidean: %.2f%% recall@%d\n",
		calculateAvgRecall(vecdist.L2Squared)*100, topK)

	fmt.Fprintf(&buf, "InnerProduct: %.2f%% recall@%d\n",
		calculateAvgRecall(vecdist.InnerProduct)*100, topK)

	// For cosine distance, normalize the query and input vectors.
	for i := range queryVectors.Count {
		num32.Normalize(queryVectors.At(i))
	}
	for i := range dataVectors.Count {
		num32.Normalize(dataVectors.At(i))
	}

	fmt.Fprintf(&buf, "Cosine: %.2f%% recall@%d\n",
		calculateAvgRecall(vecdist.Cosine)*100, topK)

	return buf.String()
}
