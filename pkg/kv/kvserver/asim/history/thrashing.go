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
	// == min). `tdtv/normTV` then measures how many times thrashing has "swept out"
	// the effective range of values the time series has taken on.
	// `tdtv/normTV` can grow arbitrarily large if the time series oscillates
	// frequently (and enough datapoints are present). To get a sense of a
	// "thrashing rate", `tdtv/(normTV*T)` or `tdtv/(normTV*runs) could be of
	// interest.
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
// The inputs u and d are the upwards and (positive) downwards total variations,
// respectively.
//
// Properties:
//   - tdtv(u,d) = tdtv(d,u) (symmetry)
//     We want this property because we don't want to privilege either direction
//     of swing.
//   - min(u,d) <= tdtv(u,d) <= u+d
//     We want the upper bound because u+d is the total variation, and we are
//     measuring a trend-discounting version of that.
//     We want the lower bound so that even when there is a dominant trend, we
//     don't discount for more than that dominant trend.
//   - tdtv(u,0) = tdtv(0,u) = 0
//     In other words, monotonic functions have zero tdtv, as they should.
//   - tdtv(u,u) = 2⋅u
//     If there is no dominant direction, we want tdtv to equal the total variation.
//   - tdtv(k⋅u,k⋅d) = k⋅tdtv(u,d) for k≥0
//     Scaling property: we want tdtv to be determined by the ratio of upwards
//     and downwards variations only. i.e. it is unitless: tdtv(u,d)=tdtv(u/d,1).
//   - tdtv(l⋅u, d) > tdtv(u, d) for l>1 and u, d ≠ 0  (concavity)
//     Strict monotonicty in each component - if either variation increases,
//     this must not reduce tdtv. In other words, additional variation will
//     always be penalized; there is no way for variations to "offset each
//     other".
func tdtv(u, d float64) float64 {
	if u > d {
		u, d = d, u
		// We may now assume that u <= d.
	}

	if u == 0 {
		// The smaller direction is zero, so the function is monotonic.
		return 0
	}
	// NB: 0 < u <= d.
	r := u / d // in [0,1]

	// Any concave and increasing function alpha with alpha(0)=0 and alpha(1)=1
	// works here:
	// alpha(0) = 0 to satisfy tdtv(_,0) = 0
	// alpha(1) = 1 to satisfy tdtv(u,u) = 2⋅u
	//
	alpha := func(r float64) float64 { // [0,1] -> [0,1]
		// The exponent can't exceed 1 because that would violate the scaling property
		// (last property above).
		// - for p=1, we get tdtv=2⋅min(u,d), i.e. twice the smaller variation (i.e.
		//   complete trend-discounting).
		// - for p=0, tdtv equals the total variation (i.e. no trend discounting)
		//
		// The choice of 0.8 is somewhat arbitrary, but gives a reasonable trade-off.
		//
		// Other choices for alpha exist, like piecewise linear functions, or
		// log(1+lambda*r)/log(1+lambda).
		const p = 0.8
		return math.Pow(r, p)
	}
	// Interpolate between u (smaller variation) and u+d (total variation). The
	// more dominant a trend there is, the smaller r, and thus alpha(r), and thus
	// the tdtv.
	return u + alpha(r)*d
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
