// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Arjun Narayan

package main

import (
	"fmt"
	"math"
	"sort"
	"testing"
)

type params struct {
	iMin, iMax uint64
	theta      float64
}

var gens = []params{
	{0, 100, 0.99},
	{0, 100, 1.01},
}

func TestCreateZipfGenerator(t *testing.T) {
	for _, gen := range gens {
		_, err := NewZipfGenerator(gen.iMin, gen.iMax, gen.theta, false)
		if err != nil {
			t.Fatal(err)
		}
	}
}

var zetas = [][]float64{
	// n, theta, zeta(n,theta)
	{20.0, 0.99, 3.64309060779367},
	{200.0, 0.99, 6.02031118558},
	{1000, 0.99, 7.72895321728},
	{2000, 0.99, 8.47398788329},
	{10000, 0.99, 10.2243614596},
	{100000, 0.99, 12.7783380626},
	{1000000, 0.99, 15.391849746},
	{10000000, 0.99, 18.066242575},
	{100000000, 0.99, 20.80293049},
}

func TestZetaFromScratch(t *testing.T) {
	for _, zeta := range zetas {
		computedZeta, err := computeZetaFromScratch(uint64(zeta[0]), zeta[1])
		if err != nil {
			t.Fatalf("Failed to compute zeta(%d,%f): %s", uint64(zeta[0]), zeta[1], err)
		}
		if math.Abs(computedZeta-zeta[2]) > 0.000000001 {
			t.Fatalf("expected %6.4f, got %6.4f", zeta[2], computedZeta)
		}
	}
}

func TestZetaIncrementally(t *testing.T) {
	// Theta cannot be 1 by definition, so this is a safe initial value.
	oldTheta := 1.0
	for i, zeta := range zetas {
		var oldZetaN float64
		var oldN uint64
		// If theta has changed, recompute from scratch
		if zetas[i][0] != oldTheta {
			var err error
			oldZetaN, err = computeZetaFromScratch(uint64(zeta[0]), zeta[1])
			if err != nil {
				t.Fatalf("Failed to compute zeta(%d,%f): %s", uint64(zeta[0]), zeta[1], err)
			}
			oldN = uint64(zeta[0])
			continue
		}

		computedZeta, err := computeZetaIncrementally(oldN, uint64(zeta[0]), zeta[1], oldZetaN)
		if err != nil {
			t.Fatalf("Failed to compute zeta(%d,%f) incrementally: %s", uint64(zeta[0]), zeta[1], err)
		}
		if math.Abs(computedZeta-zeta[2]) > 0.000000001 {
			t.Fatalf("expected %6.4f, got %6.4f", zeta[2], computedZeta)
		}

		oldZetaN = computedZeta
		oldN = uint64(zeta[0])
	}
}

func runZipfGenerators(t *testing.T, withIncrements bool) {
	gen := gens[0]
	z, err := NewZipfGenerator(gen.iMin, gen.iMax, gen.theta, false)
	if err != nil {
		t.Fatal(err)
	}

	const ROLLS = 10000
	x := make([]int, ROLLS)

	for i := 0; i < ROLLS; i++ {
		x[i] = int(z.Uint64())
		z.zipfGenMu.mu.Lock()
		if x[i] < int(z.iMin) || x[i] > int(z.zipfGenMu.iMax) {
			t.Fatalf("zipf(%d,%d,%f) rolled %d at index %d", z.iMin, z.zipfGenMu.iMax, z.theta, x[i], i)
			z.zipfGenMu.mu.Unlock()
			if withIncrements {
				if err := z.IncrementIMax(); err != nil {
					t.Fatalf("could not increment iMax: %s", err)
				}
			}
		}
		z.zipfGenMu.mu.Unlock()
	}

	if withIncrements {
		return
	}

	sort.Ints(x)

	max := x[ROLLS-1]
	step := max / 20
	index := 0
	count := 0
	for i := 0; i < max; i += step {
		count = 0
		for {
			if x[index] >= i {
				break
			}
			index++
			count++
		}
		fmt.Printf("[%10d-%10d)        ", i, i+step)
		for j := 0; j < count; j++ {
			if j%50 == 0 {
				fmt.Printf("%c", '∎')
			}
		}
		fmt.Println()
	}
}

func TestZipfGenerator(t *testing.T) {
	runZipfGenerators(t, false)
	runZipfGenerators(t, true)
}
