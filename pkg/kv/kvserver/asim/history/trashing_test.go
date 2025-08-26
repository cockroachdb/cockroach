// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package history

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/require"
)

func tsFromFunc(ticks int, f func(tick int) float64) []float64 {
	vs := make([]float64, ticks)
	for i := 0; i < ticks; i++ {
		vs[i] = f(i)
	}
	return vs
}

func TestThrashing(t *testing.T) {
	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	defer w.Check(t)
	for _, td := range []struct {
		vs   []float64
		desc string
		name string
	}{
		{
			name: "empty",
		},
		{
			name: "single",
			vs:   []float64{1.23},
		},
		{
			name: "monotonic",
			desc: `A monotonic sequence should have zero thrashing.`,
			vs:   []float64{1, 2, 3, 4},
		},
		{
			name: "last_point_adds_tv",
			desc: `Regression test to make sure thrashing in only the last index registers.`,
			vs:   []float64{2, 1, 2},
		},
		{
			name: "bathtub",
			vs:   []float64{10, 8, 4, 2, 1, 2, 4, 8, 10},
		},
		{
			name: "hump",
			vs:   []float64{1, 2, 4, 8, 10, 8, 4, 2, 1},
		},
		{
			name: "oscillate",
			desc: `An example that the thrashing percentage can easily exceed 100%`,
			vs: tsFromFunc(100, func(tick int) float64 {
				return math.Sin(math.Pi * float64(tick) / 10)
			}),
		},
		{
			name: "initial_outlier",
			desc: `An initial outlier leads to a large normalization factor, 
i.e. low thrashing percentage. This isn't necessarily good.`,
			vs: []float64{10250, 5004, 12, 1, 2},
		},
		{
			name: "final_outlier",
			vs:   []float64{1, 3, 2, 1, 2005},
		},
		{
			name: "monotonic_with_hiccup",
			desc: `A "basically" monotonic sequence that has a small hiccup near the
beginning. Sadly this currently results in the bulk of the sequence registering
as thrashing, which is not desired in any allocator-related use cases.
Two improvements come to mind:
- epsilon-hysteresis: small changes that are reversed quickly don't count, but
  it's hard to determine what "small" means.
- trend-based: if there is an overall trend, count only total variation
  in the direction opposing the trend. For example, we can count the number
  of upward and downward deltas, and if one direction is dominant, count only
  variation in the other direction.
TODO(tbg): try out and implement trend-based thrashing detection.
`,
			vs: []float64{100, 90, 91, 85, 80, 70, 60, 50, 40, 30, 20, 10, 0},
		},
		{
			name: "negative_crossover",
			desc: "Oscillating across zero in both directions",
			vs:   []float64{-10, -4, -6, -1, 4, 5, -2, 6},
		},
	} {
		t.Run(td.name, w.Run(t, td.name, func(t *testing.T) string {
			var buf strings.Builder
			if td.desc != "" {
				_, _ = fmt.Fprintln(&buf, strings.TrimSpace(td.desc))
			}
			_, _ = fmt.Fprintln(&buf, "input:", td.vs)
			if len(td.vs) > 1 {
				_, _ = fmt.Fprintln(&buf, asciigraph.Plot(td.vs, asciigraph.Width(40), asciigraph.Height(10)))
			}
			th := computeThrashing(td.vs)
			_, _ = fmt.Fprint(&buf, th)

			// Sanity check ThrashingSlice in the case of a single element.
			pre := th.normTV
			{
				ths := ThrashingSlice{th}
				ths.normalize()
				require.Equal(t, pre, ths[0].normTV, "normalization should not change normTV")
			}
			// Ditto for repeating the th in a ThrashingSlice, which shouldn't change
			// the range either.
			{
				ths := ThrashingSlice{th, th, th}
				ths.normalize()
				require.Equal(t, pre, ths[0].normTV, "normalization should not change normTV")
			}

			return buf.String()
		}))
	}
}
