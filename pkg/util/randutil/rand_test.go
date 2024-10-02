// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randutil_test

import (
	"testing"

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
