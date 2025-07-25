// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"fmt"
	"testing"

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
