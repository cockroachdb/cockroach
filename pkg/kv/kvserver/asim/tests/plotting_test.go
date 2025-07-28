// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

// generatePlot creates a single plot for the given stat from simulation history.
// Hashes the plot file content directly to the provided hasher.
// If rewrite is false, the plot is generated but not saved to disk.
// Returns the filename or an error if plot generation fails.
func generatePlot(t *testing.T, stat string, sl [][]float64) []byte {
	p := plot.New()
	p.Title.Text = stat
	// TODO(tbg): convert to time.
	p.X.Label.Text = "Ticks"
	p.Y.Label.Text = stat

	var plotArgs []interface{}
	for storeIdx, storeTS := range sl {
		storeID := storeIdx + 1
		pts := make(plotter.XYs, len(storeTS))
		for i, val := range storeTS {
			pts[i].X = float64(i) // tick
			pts[i].Y = val
		}
		plotArgs = append(plotArgs, fmt.Sprintf("s%d", storeID), pts)
	}

	err := plotutil.AddLinePoints(p, plotArgs...)
	require.NoError(t, err)
	wt, err := p.WriterTo(8*vg.Inch, 6*vg.Inch, "png")
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = wt.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

// generateAllPlots creates plots for all available metrics from simulation history.
// All plot files are hashed directly to the provided hasher.
// If rewrite is false, plots are generated but not saved to disk.
// Returns a slice of filenames for all generated plots.
func generateAllPlots(
	t *testing.T,
	h history.History,
	testName string,
	sample int,
	outputDir string,
	hasher hash.Hash,
	rewrite bool,
) {
	ts := metrics.MakeTS(h.Recorded)

	// Need determinism due to hashing.
	var statNames []string
	for stat := range ts {
		statNames = append(statNames, stat)
	}
	sort.Strings(statNames)

	for _, stat := range statNames {
		hasher.Write([]byte(fmt.Sprintf("%v", ts[stat])))
		if rewrite {
			_ = os.MkdirAll(outputDir, 0755)
			path := filepath.Join(outputDir, fmt.Sprintf("%s_%d_%s.png", testName, sample, stat))
			b := generatePlot(t, stat, ts[stat])
			require.NoError(t, os.WriteFile(path, b, 0644))
		}
	}
}
