// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

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

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "estimate-distances":
				return testDistanceEstimation(t, d)

			default:
				t.Fatalf("unknown cmd: %s", d.Cmd)
			}

			return ""
		})
	})
}

func testDistanceEstimation(t *testing.T, d *datadriven.TestData) string {
	var workspace workspace.T
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

		case "query-feature":
			// Load the first 10 input vectors and use the requested input vector
			// as the query vector.
			vectors = testutils.LoadFeatures(t, 10)
			require.Len(t, arg.Vals, 1)
			val, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			queryVector = vectors.At(val)

		default:
			t.Fatalf("unknown arg: %s", arg.Key)
		}
	}

	var buf bytes.Buffer
	doTest := func(metric vecdist.Metric) {
		unquantizer := NewUnQuantizer(len(queryVector), metric)
		unQuantizedSet := unquantizer.Quantize(&workspace, vectors)
		exact := make([]float32, unQuantizedSet.GetCount())
		errorBounds := make([]float32, unQuantizedSet.GetCount())
		unquantizer.EstimateDistances(
			&workspace, unQuantizedSet, queryVector, exact, errorBounds)
		for _, error := range errorBounds {
			require.Zero(t, error)
		}

		rabitQ := NewRaBitQuantizer(len(queryVector), 42, metric)
		rabitQSet := rabitQ.Quantize(&workspace, vectors)
		estimated := make([]float32, rabitQSet.GetCount())
		rabitQ.EstimateDistances(
			&workspace, rabitQSet, queryVector, estimated, errorBounds)

		for i := range vectors.Count {
			var errorBound string
			if errorBounds[i] != 0 {
				errorBound = fmt.Sprintf(" ± %s", utils.FormatFloat(errorBounds[i], 2))
			}
			buf.WriteString("  ")
			utils.WriteVector(&buf, vectors.At(i), 4)
			fmt.Fprintf(&buf, ": exact is %s, estimate is %s%s\n",
				utils.FormatFloat(exact[i], 3), utils.FormatFloat(estimated[i], 3), errorBound)
		}
	}

	centroid := vectors.Centroid(make(vector.T, vectors.Dims))
	buf.WriteString("Centroid = ")
	utils.WriteVector(&buf, centroid, 4)

	buf.WriteString("\nL2Squared\n")
	doTest(vecdist.L2Squared)

	buf.WriteString("InnerProduct\n")
	doTest(vecdist.InnerProduct)

	// For cosine distance, normalize the query and input vectors.
	num32.Normalize(queryVector)
	for i := range vectors.Count {
		num32.Normalize(vectors.At(i))
	}

	buf.WriteString("Cosine\n")
	doTest(vecdist.Cosine)

	return buf.String()
}
