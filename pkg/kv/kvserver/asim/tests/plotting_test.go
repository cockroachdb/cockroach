// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
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
)

// generateAllPlots creates plots for all available metrics from simulation history.
// All plot files are hashed directly to the provided hasher.
// If rewrite is false, plots are generated but not saved to disk.
// Returns a slice of filenames for all generated plots.
//
// TODO(tbg): introduce a SimulationEnv and make this a method on it.
// TODO(tbg): rename to generateArtifacts.
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
			m := map[string][]float64{}
			for idx, vals := range ts[stat] {
				m[fmt.Sprintf("s%d", idx+1)] = vals
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
		_ = os.MkdirAll(outputDir, 0755)
		path := filepath.Join(outputDir, fmt.Sprintf("%s_%d.json", testName, sample))
		b, err := json.Marshal(map[string]interface{}{
			"tickInterval": tickInterval.Nanoseconds(),
			"metrics":      jsonData,
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, b, 0644))
	}
}
