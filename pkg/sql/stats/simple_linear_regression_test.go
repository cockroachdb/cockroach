// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestFloat64SimpleLinearRegression tests float64SimpleLinearRegression with
// some basic sets of x and y.
func TestFloat64SimpleLinearRegression(t *testing.T) {
	testCases := []struct {
		x, y       []float64
		xn, yn, r2 float64
	}{
		{
			x:  []float64{0},
			y:  []float64{0},
			xn: 1,
			yn: 0,
			r2: 1,
		},
		{
			x:  []float64{0, 0},
			y:  []float64{0, 1},
			xn: 1,
			yn: 0.5,
			r2: 0,
		},
		{
			x:  []float64{0, 1},
			y:  []float64{0, 0},
			xn: 2,
			yn: 0,
			r2: 1,
		},
		{
			x:  []float64{0, 1},
			y:  []float64{0, 1},
			xn: 2,
			yn: 2,
			r2: 1,
		},
		{
			x:  []float64{0, 1},
			y:  []float64{1, 0},
			xn: 2,
			yn: -1,
			r2: 1,
		},
		{
			x:  []float64{0, 0, 0},
			y:  []float64{0, 1, 2},
			xn: 1,
			yn: 1,
			r2: 0,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []float64{0, 0, 0},
			xn: 3,
			yn: 0,
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []float64{0, 1, 2},
			xn: 3,
			yn: 3,
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []float64{2, 1, 0},
			xn: 3,
			yn: -1,
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2, 3},
			y:  []float64{10, 30, 20, 40},
			xn: 4,
			yn: 45,
			r2: 0.64,
		},
		{
			x:  []float64{0, 1, 1, 3, -1, -2, 4, 10},
			y:  []float64{3, 7, 8, 9, 0, -4, 12, 34},
			xn: 5,
			yn: 17.625,
			r2: 0.9741577594371533,
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			yn, r2 := float64SimpleLinearRegression(tc.x, tc.y, tc.xn)
			if yn != tc.yn {
				t.Errorf("test case %d incorrect yn %v expected %v", i, yn, tc.yn)
			}
			if r2 != tc.r2 {
				t.Errorf("test case %d incorrect r2 %v expected %v", i, r2, tc.r2)
			}
		})
	}
}

// TestFloat64SimpleLinearRegressionRandom constructs a random line y = mx + b,
// picks several points on that line, and then checks that
// float64SimpleLinearRegression on those points finds something near that line.
func TestFloat64SimpleLinearRegressionRandom(t *testing.T) {
	const delta = 1e-3
	rng, seed := randutil.NewTestRand()
	for i := 0; i < 5; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			m := rng.NormFloat64() * 1e2
			b := rng.NormFloat64() * 1e4
			n := rng.Intn(5) + 2
			x := make([]float64, n)
			y := make([]float64, n)
			for i := range x {
				x[i] = rng.NormFloat64() * 1e4
				y[i] = m*x[i] + b
			}
			xₙ := rng.NormFloat64() * 1e4
			yₙExpected := m*xₙ + b
			yₙ, r2 := float64SimpleLinearRegression(x, y, xₙ)
			if math.Abs(yₙ-yₙExpected) > delta || math.Abs(r2-1) > delta {
				t.Errorf(
					"seed: %v, x: %v, y: %v, xₙ: %v, yₙ: %v, yₙExpected: %v, r2: %v",
					seed, x, y, xₙ, yₙ, yₙExpected, r2,
				)
			}
		})
	}
}

// TestQuantileSimpleLinearRegression tests quantileSimpleLinearRegression with
// some basic sets of x and y(t).
func TestQuantileSimpleLinearRegression(t *testing.T) {
	testCases := []struct {
		x  []float64
		y  []quantile
		xn float64
		yn quantile
		r2 float64
	}{
		{
			x:  []float64{0},
			y:  []quantile{zeroQuantile},
			xn: 1,
			yn: zeroQuantile,
			r2: 1,
		},
		{
			x:  []float64{9},
			y:  []quantile{{{0, 9}, {0.5, 99}, {1, 999}}},
			xn: 9,
			yn: quantile{{0, 9}, {0.5, 99}, {1, 999}},
			r2: 1,
		},
		{
			x:  []float64{0, 0},
			y:  []quantile{zeroQuantile, {{0, 1}, {1, 1}}},
			xn: 1,
			yn: quantile{{0, 0.5}, {1, 0.5}},
			r2: 0,
		},
		{
			x:  []float64{0, 1},
			y:  []quantile{zeroQuantile, zeroQuantile},
			xn: 2,
			yn: zeroQuantile,
			r2: 1,
		},
		{
			x:  []float64{0, 1},
			y:  []quantile{zeroQuantile, {{0, 1}, {1, 1}}},
			xn: 2,
			yn: quantile{{0, 2}, {1, 2}},
			r2: 1,
		},
		{
			x:  []float64{0, 0, 0},
			y:  []quantile{zeroQuantile, {{0, 1}, {1, 1}}, {{0, 2}, {1, 2}}},
			xn: 1,
			yn: quantile{{0, 1}, {1, 1}},
			r2: 0,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []quantile{zeroQuantile, zeroQuantile, zeroQuantile},
			xn: 3,
			yn: zeroQuantile,
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []quantile{zeroQuantile, {{0, 1}, {1, 1}}, {{0, 2}, {1, 2}}},
			xn: 3,
			yn: quantile{{0, 3}, {1, 3}},
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []quantile{{{0, 2}, {1, 2}}, {{0, 1}, {1, 1}}, zeroQuantile},
			xn: 3,
			yn: quantile{{0, -1}, {1, -1}},
			r2: 1,
		},
		{
			x: []float64{0, 1, 2},
			y: []quantile{
				{{0, 0}, {1, 0}},
				{{0, 0}, {1, 10}},
				{{0, 0}, {1, 20}},
			},
			xn: 3,
			yn: quantile{{0, 0}, {1, 30}},
			r2: 1,
		},
		{
			x: []float64{0, 1, 2},
			y: []quantile{
				{{0, 30}, {0.5, 40}, {1, 50}},
				{{0, 20}, {0.5, 30}, {1, 60}},
				{{0, 25}, {0.5, 50}, {1, 70}},
			},
			xn: 1,
			yn: quantile{{0, 25}, {0.5, 40}, {1, 60}},
			r2: 0.484375,
		},
		// A case that produces a malformed quantile which needs fixing.
		{
			x: []float64{0, 1, 2, 3},
			y: []quantile{
				{{0, 400}, {0.5, 419}, {1, 490}},
				{{0, 402}, {0.5, 415}, {1, 490}},
				{{0, 404}, {0.5, 411}, {1, 490}},
				{{0, 406}, {0.5, 407}, {1, 490}},
			},
			xn: 4,
			yn: quantile{{0, 403}, {0.5287356321839081, 408}, {1, 490}},
			r2: 1,
		},
		// Quantiles with horizontal and vertical sections.
		{
			x: []float64{0, 1, 2, 3, 4},
			y: []quantile{
				{{0, 500}, {0, 510}, {0.25, 510}, {0.5, 520}, {0.5, 530}, {0.75, 540}, {0.75, 550}, {1, 550}},
				{{0, 600}, {0.25, 600}, {0.25, 610}, {0.5, 610}, {0.5, 620}, {0.75, 620}, {1, 630}, {1, 640}},
				{{0, 700}, {0.25, 710}, {0.5, 710}, {0.75, 720}, {1, 720}},
				{{0, 800}, {0, 810}, {0.25, 810}, {0.25, 820}, {0.5, 820}, {0.5, 830}, {0.75, 830}, {0.75, 840}, {1, 850}, {1, 860}},
				{{0, 900}, {0.25, 910}, {0.5, 920}, {0.75, 930}, {0.75, 940}, {1, 940}, {1, 950}, {1, 960}},
			},
			xn: -1,
			yn: quantile{{0, 400}, {0, 405}, {0.25, 407}, {0.25, 409}, {0.5, 413}, {0.5, 425}, {0.75, 431}, {0.75, 434}, {1, 438}},
			r2: 0.9973157151167622,
		},
		// A case with varying numbers of points.
		{
			x: []float64{2, 1, 0, -1},
			y: []quantile{
				{{0, 200}, {1, 300}},
				{{0, 200}, {0.5, 275}, {1, 300}},
				{{0, 200}, {0.25, 275}, {0.5, 290}, {0.75, 300}, {1, 300}},
				{{0, 200}, {0.125, 275}, {0.25, 290}, {0.375, 295}, {0.5, 297}, {0.625, 299}, {0.75, 300}, {0.875, 300}, {1, 300}},
			},
			xn: 3,
			yn: quantile{{0, 184.375}, {0.24, 198.75}, {0.25806451612903225, 200}, {0.375, 218.125}, {0.5, 239}, {0.625, 253.62500000000003}, {0.75, 268.75}, {0.875, 284.375}, {1, 300}},
			r2: 0.9500423970245813,
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			yn, r2 := quantileSimpleLinearRegression(tc.x, tc.y, tc.xn)
			yn = yn.fixMalformed()
			if !reflect.DeepEqual(yn, tc.yn) {
				t.Errorf("test case %d incorrect yn %v expected %v", i, yn, tc.yn)
			}
			if r2 != tc.r2 {
				t.Errorf("test case %d incorrect r2 %v expected %v", i, r2, tc.r2)
			}
		})
	}
}

// TestQuantileSimpleLinearRegressionRandom constructs a random linear mapping
// from scalars to quantiles y(t) = m(t)x + b(t), picks several points in that
// mapping, and then checks that quantileSimpleLinearRegression on those points
// finds something near that linear mapping.
func TestQuantileSimpleLinearRegressionRandom(t *testing.T) {
	const delta = 1e-3
	rng, seed := randutil.NewTestRand()
	for i := 0; i < 5; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			m := randQuantile(rng)
			b := randQuantile(rng)
			n := rng.Intn(5) + 2
			x := make([]float64, n)
			y := make([]quantile, n)
			for i := range x {
				x[i] = rng.NormFloat64() * 1e4
				y[i] = m.mult(x[i]).add(b)
			}
			xₙ := rng.NormFloat64() * 1e4
			yₙExpected := m.mult(xₙ).add(b)
			yₙ, r2 := quantileSimpleLinearRegression(x, y, xₙ)
			d := yₙ.sub(yₙExpected)
			d2 := d.integrateSquared()
			if d2 > delta || math.Abs(r2-1) > delta {
				t.Errorf(
					"seed: %v,\nm: %v,\nb: %v,\nx: %v,\ny: %v,\nxₙ: %v,\nyₙ: %v,\nyₙExpected: %v,\nd: %v,\nd2: %v, r2: %v",
					seed, m, b, x, y, xₙ, yₙ, yₙExpected, d, d2, r2,
				)
			}
		})
	}
}
