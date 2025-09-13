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
	// tdtv is the trend-discounting total variation. It's close to the total
	// variation if the time series has no preferred trend (i.e. upwards or
	// downwards), but if there is a clear trend, the tdtv will be much smaller,
	// counting mainly the movement against the trend. see tdtv for details.
	tdtv       float64
	uptv, dntv float64 // upwards and downwards total variations (both nonnegative)
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
	return fmt.Sprintf("tdtv=%.2f%% (%.1f/%.1f) uptv=%.1f dntv=%.1f runs=%d", th.TDTVPercent(), th.tdtv, th.normTV, th.uptv, th.dntv, th.runs)
}

func (th thrashing) TDTVPercent() float64 {
	return 100 * th.tdtv / th.normTV
}

func computeThrashing(values []float64) thrashing {
	runs := 1
	pos := len(values) > 1 && values[1]-values[0] >= 0
	var uptv, dntv float64
	for i := 1; i < len(values); i++ {
		d := values[i] - values[i-1]

		if d >= 0 != pos {
			// The run ended, and the new run starts here.
			pos = !pos
			runs++
		}

		if d >= 0 {
			uptv += d
		} else {
			dntv += -d
		}
	}
	_, _, normTV := extrema(values)
	return thrashing{
		vs:     values,
		runs:   runs,
		tdtv:   tdtv(uptv, dntv),
		uptv:   uptv,
		dntv:   dntv,
		normTV: normTV,
	}
}

// tdtv computes a "trend-discounting total variation" that discounts variation
// in the dominant direction (if there is one). If no direction is dominant, the
// result roughly matches the total variation.
//
// The inputs tu and td are the upwards and (positive) downwards total
// variations, respectively.
//
// Properties:
// - symmetric in tu, td
// - min(u,d) <= tdtv(tu,td) <= tu+td
// - tdtv(u,0) = tdtv(0,u) = 0
// - tdtv(u,u) = 2t
// - tdtv(ku,kd) = k*tdtv(u,d) for k>=0
// - tdtv(l*ku, kd) > tdtv(ku) for l>1 (same for kd)
func tdtv(tu, td float64) float64 {
	tmin := min(tu, td)
	if tmin == 0 {
		// There's only one direction of movement, so we discount all variation.
		return 0
	}
	frac := tmin / max(tu, td) // in [0, 1]
	// The exponent can't exceed 1 because that would violate the scaling property
	// (last property above). For the endpoint 1, tdtv=2min(tu,td). The choice of
	// 0.8 is somewhat arbitrary, but gives a reasonable trade-off.
	alpha := math.Pow(frac, 0.8)
	return alpha*(tu+td) + (1-alpha)*tmin
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
