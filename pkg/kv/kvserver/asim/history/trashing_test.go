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
			vs:   []float64{1, 2, 3, 4},
		},
		{
			name: "last_point_adds_tv",
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
			vs: tsFromFunc(100, func(tick int) float64 {
				return math.Sin(math.Pi * float64(tick) / 10)
			}),
		},
		{
			name: "initial_outlier",
			vs:   []float64{10250, 5004, 12, 1, 2},
		},
		{
			name: "final_outlier",
			vs:   []float64{1, 3, 2, 1, 2005},
		},
	} {
		t.Run(td.name, w.Run(t, td.name, func(t *testing.T) string {
			var buf strings.Builder
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
