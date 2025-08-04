// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package history

import (
	"fmt"
	"math"
)

// thrashing measures the amount of thrashing in a time series (represented by
// a slice of values).
type thrashing struct {
	vs []float64 // the time series input
	// Runs are the maximal-length subsequences (computed in slice order) of
	// values within which all first differences have the same sign. For example,
	// the sequence [1,2,3,2,3,2,1] has the runs [1,2,3], [2,3], [2,1].
	// The number of runs equals one plus the number of sign changes in the first
	// differences.
	runs int
	// tv is the total variation (i.e. the sum of absolute first differences)
	// in all runs except the first. Excluding the first run ensures that a
	// monotonic sequence is assigned zero thrashing.
	tv float64
	// normTV is a normalization factor for the total variation. By default, it is
	// initialized to the range of the input values, i.e. max - min (or 1 if max
	// == min). `tv/normTV` then measures how many times thrashing has "swept out"
	// the effective range of values the time series has taken on. This measure
	// can be misleading if the time series has early outliers that are not
	// accounted for in the total variation, but for the applications at hand it
	// generally makes sense. `tv/normTV` can grow arbitrarily large if the time
	// series oscillates frequently (and enough datapoints are present). To get
	// a sense of a "thrashing rate", `tv/(normTV*T)` or `tv/(normTV*runs) could
	// be of interest.
	normTV float64
}

func (th thrashing) String() string {
	return fmt.Sprintf("tv=%.2f%% (%.1f) runs=%d", th.TVPercent(), th.tv, th.runs)
}

func (th thrashing) TVPercent() float64 {
	return 100 * th.tv / th.normTV
}

func computeThrashing(values []float64) thrashing {
	runs := 1
	var tv float64      // total variation (excluding first monotonic run)
	var runFirstIdx int // first index of current run, i.e. old run's last index + 1
	pos := len(values) > 1 && values[1]-values[0] >= 0
	for i := 1; i < len(values); i++ {
		d := values[i] - values[i-1]

		if d >= 0 != pos {
			// The run ended, and the new run starts here.
			pos = !pos
			runFirstIdx = i
			runs++
		}

		if runFirstIdx > 0 {
			tv += math.Abs(d)
		}
	}
	_, _, normTV := extrema(values)
	return thrashing{
		vs:     values,
		runs:   runs,
		tv:     tv,
		normTV: normTV,
	}
}

func extrema(vs []float64) (vmin, vmax, vrange float64) {
	vmin = math.Inf(1)
	vmax = math.Inf(-1)
	for _, v := range vs {
		if v < vmin {
			vmin = v
		}
		if v > vmax {
			vmax = v
		}
	}
	vrange = vmax - vmin
	if vrange == 0 {
		vrange = 1
	}
	return vmin, vmax, vrange
}

// ThrashingSlice is a slice of thrashing structs, one per store. These are
// assumed to be for the same metric.
type ThrashingSlice []thrashing

func (ths ThrashingSlice) normalize() {
	vmin := math.Inf(1)
	vmax := math.Inf(-1)
	for _, th := range ths {
		cmin, cmax, _ := extrema(th.vs)
		if cmin < vmin {
			vmin = cmin
		}
		if cmax > vmax {
			vmax = cmax
		}
	}
	normTV := vmax - vmin
	if normTV == 0 {
		normTV = 1
	}

	for i := range ths {
		ths[i].normTV = normTV
	}
}
