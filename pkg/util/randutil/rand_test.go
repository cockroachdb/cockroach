// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randutil_test

import (
	"encoding/binary"
	"math"
	"testing"
	"testing/quick"

	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestPseudoRand(t *testing.T) {
	numbers := make(map[int]bool)
	// Make two random number generators and pull two numbers from each.
	rand1, _ := randutil.NewPseudoRand()
	rand2, _ := randutil.NewPseudoRand()
	numbers[rand1.Int()] = true
	numbers[rand1.Int()] = true
	numbers[rand2.Int()] = true
	numbers[rand2.Int()] = true
	// All four numbers should be distinct; no seed state is shared.
	if len(numbers) != 4 {
		t.Errorf("expected 4 unique numbers; got %d", len(numbers))
	}
}

func TestRandIntInRange(t *testing.T) {
	rand, _ := randutil.NewPseudoRand()
	for i := 0; i < 100; i++ {
		x := randutil.RandIntInRange(rand, 20, 40)
		if x < 20 || x >= 40 {
			t.Errorf("got result out of range: %d", x)
		}
	}
}

func TestRandBytes(t *testing.T) {
	rand, _ := randutil.NewPseudoRand()
	for i := 0; i < 100; i++ {
		x := randutil.RandBytes(rand, i)
		if len(x) != i {
			t.Errorf("got array with unexpected length: %d (expected %d)", len(x), i)
		}
	}
}

func TestTestRand(t *testing.T) {
	n1 := func() map[int]bool {
		numbers := make(map[int]bool)
		rand1, _ := randutil.NewTestRand()
		rand2, _ := randutil.NewTestRand()
		numbers[rand1.Int()] = true
		numbers[rand1.Int()] = true
		numbers[rand2.Int()] = true
		numbers[rand2.Int()] = true
		return numbers
	}()
	n2 := func() map[int]bool {
		numbers := make(map[int]bool)
		rand1, _ := randutil.NewTestRand()
		rand2, _ := randutil.NewTestRand()
		numbers[rand1.Int()] = true
		numbers[rand1.Int()] = true
		numbers[rand2.Int()] = true
		numbers[rand2.Int()] = true
		return numbers
	}()
	if len(n1) != len(n2) {
		t.Errorf("expected the same random numbers; got lengths %d and %d", len(n1), len(n2))
	}
	for k := range n1 {
		if !n2[k] {
			t.Errorf("expected the same random numbers; got unique number %d", k)
		}
	}
}

func TestDeterministicChoice(t *testing.T) {
	// Base config for quick checks.
	cfg := &quick.Config{
		MaxCount: 1000,
	}
	cfg.Rand, _ = randutil.NewTestRand()

	// 1) Determinism: same (p,key,salt) -> same result, always.
	propDeterministic := func(p float64, key []byte, salt uint64) bool {
		if p < 0 {
			p = 0
		} else if p > 1 {
			p = 1
		}
		a := randutil.DeterministicChoice(p, key, salt)
		b := randutil.DeterministicChoice(p, key, salt)
		return a == b
	}
	if err := quick.Check(propDeterministic, cfg); err != nil {
		t.Fatalf("determinism failed: %v", err)
	}
	// 2) Edge behavior: p==0 always false; p==1 always true.
	propEdges := func(key []byte, salt uint64) bool {
		return !randutil.DeterministicChoice(0, key, salt) && randutil.DeterministicChoice(1, key, salt)
	}
	if err := quick.Check(propEdges, cfg); err != nil {
		t.Fatalf("edge cases failed: %v", err)
	}
	// 3) Monotonicity in p: for p1 <= p2, true at p1 implies true at p2.
	propMonotone := func(key []byte, salt uint64, p1, p2 float64) bool {
		// clamp to [0,1]
		if p1 < 0 {
			p1 = 0
		} else if p1 > 1 {
			p1 = 1
		}
		if p2 < 0 {
			p2 = 0
		} else if p2 > 1 {
			p2 = 1
		}
		if p1 > p2 {
			p1, p2 = p2, p1
		}
		r1 := randutil.DeterministicChoice(p1, key, salt)
		r2 := randutil.DeterministicChoice(p2, key, salt)
		// If it was true at lower p, it cannot flip to false at higher p.
		return !r1 || r2
	}
	if err := quick.Check(propMonotone, cfg); err != nil {
		t.Fatalf("monotonicity failed: %v", err)
	}
	// 4) Approximate distribution: for many distinct keys and fixed salt,
	// the observed true-rate is close to p (within a sigma-based margin).
	cfgDist := *cfg
	// Use a smaller MaxCount for this heavier check.
	cfgDist.MaxCount = 50

	propDistribution := func(salt uint64, rawP float64) bool {
		// Keep p away from exact 0/1 for a meaningful variance.
		p := rawP
		if p < 0.02 {
			p = 0.02
		} else if p > 0.98 {
			p = 0.98
		} else if p > 1 || p < 0 {
			// Map arbitrary float into [0.02,0.98] if outside range.
			p = math.Mod(math.Abs(p), 1)
			if p < 0.02 {
				p = 0.02
			}
			if p > 0.98 {
				p = 0.98
			}
		}

		const N = 10000 // balanced for speed & reliability
		var cnt int
		var key [8]byte
		for i := 0; i < N; i++ {
			binary.LittleEndian.PutUint64(key[:], uint64(i))
			if randutil.DeterministicChoice(p, key[:], salt) {
				cnt++
			}
		}
		frac := float64(cnt) / float64(N)
		sigma := math.Sqrt(p * (1 - p) / float64(N))
		// Allow the larger of an absolute 1.5% or 5σ—robust across platforms.
		margin := math.Max(0.015, 5*sigma)
		return math.Abs(frac-p) <= margin
	}
	if err := quick.Check(propDistribution, &cfgDist); err != nil {
		t.Fatalf("distribution failed: %v", err)
	}
}
