// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

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
//
// TODO(tbg): introduce a SimulationEnv and make this a method on it.
func generateAllPlots(
	t *testing.T,
	buf *strings.Builder,
	h history.History,
	testName string,
	sample int,
	outputDir string,
	hasher hash.Hash,
	rewrite bool,
	tickInterval time.Duration,
	metricsMap map[string]struct{},
) {
	ts := metrics.MakeTS(h.Recorded)

	// Need determinism due to hashing.
	var statNames []string
	for stat := range ts {
		statNames = append(statNames, stat)
	}
	sort.Strings(statNames)

	// JSON format:
	//
	// tickInterval: <nanos>
	// metrics:
	//   some_metric:
	//     s1: [0, 1, 2, ...]
	jsonData := map[string]map[string][]float64{}
	for _, stat := range statNames {
		hasher.Write([]byte(fmt.Sprintf("%v", ts[stat])))
		if rewrite {
			_ = os.MkdirAll(outputDir, 0755)
			if false {
				// TODO(tbg): across the test suite, plots currently add up
				// to >20mb, which is too much to store in git, especially
				// given the frequent churn as allocator logic or the testing
				// changes just slightly.
				path := filepath.Join(outputDir, fmt.Sprintf("%s_%d_%s.png", testName, sample, stat))
				b := generatePlot(t, stat, ts[stat])
				require.NoError(t, os.WriteFile(path, b, 0644))
			}

			m := map[string][]float64{}
			for storeID, vals := range ts[stat] {
				m[fmt.Sprintf("s%d", storeID)] = vals
			}
			jsonData[stat] = m
		}

		// TODO(tbg): there are too many metrics to print here; they clutter up
		// the datadriven output. We should rely on assertions instead.
		if _, ok := metricsMap[stat]; ok {
			at0, ok0 := h.ShowRecordedValueAt(0, stat)
			s, ok := h.ShowRecordedValueAt(len(h.Recorded)-1, stat)
			if ok0 {
				_, _ = fmt.Fprintf(buf, "%s#%d: first: %s\n", stat, sample, at0)
			}
			if ok0 || ok {
				_, _ = fmt.Fprintf(buf, "%s#%d: last:  %s\n", stat, sample, s)
			}
		}
	}
	if rewrite {
		path := filepath.Join(outputDir, fmt.Sprintf("%s_%d.json", testName, sample))
		b, err := json.Marshal(map[string]interface{}{
			"tickInterval": tickInterval.Nanoseconds(),
			"metrics":      jsonData,
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, b, 0644))
	}
}
