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
	"testing/quick"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/assert"
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
			desc: `This sequence has high thrashing, since it ends up where it started.`,
			vs:   []float64{10, 8, 4, 2, 1, 2, 4, 8, 10},
		},
		{
			name: "hump",
			desc: `This sequence has high thrashing, since it ends up where it started.`,
			vs:   []float64{1, 2, 4, 8, 10, 8, 4, 2, 1},
		},
		{
			name: "oscillate",
			desc: `An example that the thrashing percentage can arbitarily exceed 200%.`,
			vs: tsFromFunc(100, func(tick int) float64 {
				return math.Sin(math.Pi * float64(tick) / 10)
			}),
		},
		{
			name: "initial_outlier",
			desc: `An initial outlier leads to a large normalization factor, 
i.e. low thrashing percentage. This isn't necessarily good, if this becomes
an issue we could use an inter-quantile range instead.`,
			vs: []float64{10250, 13, 12, 1, 2},
		},
		{
			name: "final_outlier",
			desc: `An almost monotonic function (the final outlier dominates total variation),
so it is assigned a small thrashing percentage.`,
			vs: []float64{1, 3, 2, 1, 2005},
		},
		{
			name: "monotonic_with_hiccup",
			desc: `A "basically" monotonic sequence that has a small hiccup near the
beginning. The trend-aware thrashing measure (tdtv) should be small.
`,
			vs: []float64{100, 90, 91, 85, 80, 70, 60, 50, 40, 30, 20, 10, 0},
		},
		{
			name: "negative_crossover",
			desc: `Oscillating across zero in both directions, mostly to check that
negative numbers don't cause issues.`,
			vs: []float64{-10, -4, -6, -1, 4, 5, -2, 6},
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

func TestTDTV(t *testing.T) {
	t.Run("scaling", func(t *testing.T) {
		r, seed := randutil.NewPseudoRand()
		require.NoError(t, quick.Check(func(iu, id uint64) bool {
			// Starting with ints avoids needing to write Inf and NaN checks.
			// We reduce the range to avoid hitting larger float computation
			// inaccuracies.
			u, d := float64(iu%1e9), float64(id%1e9)
			u, d = max(u, -u), max(d, -d) // take absolute values for both

			orig := tdtv(u, d)
			swapped := tdtv(d, u)
			const k = 4.76
			scaled := tdtv(k*u, k*d) / k
			const delta = 0.0001
			assert.InDelta(t, orig, swapped, delta)
			assert.InDelta(t, orig, scaled, delta)
			assert.InDelta(t, 0, tdtv(u, 0), delta)
			assert.InDelta(t, 2*u, tdtv(u, u), delta)
			assert.Greater(t, tdtv(2*u, d), tdtv(u, d))

			return true
		}, &quick.Config{MaxCount: 1000, Rand: r}), "seed: %d", seed)
	})

	var buf strings.Builder
	// Because of the scaling property of tdtv, we only need to test the case
	// where tu+tv equals one. To this end, we sweep out tu from [0,1] and chose
	// td as 1-tu.
	const N = 10
	sl := make([]float64, N+1)
	for i := range sl {
		tu := float64(i) / N
		td := 1 - tu
		sl[i] = tdtv(tu, td)
	}
	_, _ = fmt.Fprintf(&buf, "%.2f\n", sl)
	_, _ = fmt.Fprint(&buf, asciigraph.Plot(sl, asciigraph.Width(2*N), asciigraph.Height(N)))
	echotest.Require(t, buf.String(), datapathutils.TestDataPath(t, t.Name()+".txt"))
}

func TestExploreTDTV(t *testing.T) {
	// Not a real test, more a helper.
	sl := []float64{
		// You can paste a time series copied from viewer.html here to explore
		// the variation of its prefixes.
		0,
		1,
		2,
		3,
		2,
	}
	testutils.RunTrueAndFalse(t, "stripped", func(t *testing.T, stripped bool) {
		sl := sl
		if stripped {
			sl = stripLeaderingZeroes(sl)
		}
		const step = 100
		for i := min(step, len(sl)); i <= len(sl); i += min(step, len(sl)-i+1) {
			t.Logf("sl[:%d]: %+v", i, computeThrashing(sl[:i]))
		}
	})
}
