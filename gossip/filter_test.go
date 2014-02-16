// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package gossip

import (
	"fmt"
	"math"
	"testing"
)

// TestProbFalsePositive verifies some basic expectations
// of false positive computations.
func TestProbFalsePositive(t *testing.T) {
	// Simple cases.
	if probFalsePositive(0, 1, 1) != 0 {
		t.Error("P(FP) with no insertions != 0")
	}
	if probFalsePositive(1, 1, 1) != 1 {
		t.Error("P(FP) with full filter != 1")
	}
	if probFalsePositive(1, 1, 10) > probFalsePositive(10, 1, 10) {
		t.Error("P(FP) should increase with insertions")
	}
	if probFalsePositive(10, 1, 10) < probFalsePositive(10, 1, 100) {
		t.Error("P(FP) should decrease with space")
	}
}

// TestOptimalValues verifies optimal values make sense.
func TestOptimalValues(t *testing.T) {
	// Compute optimal values for 1000 insertions and various probabilities.
	M10, _ := computeOptimalValues(1000, 0.10)
	M05, _ := computeOptimalValues(1000, 0.05)
	M01, _ := computeOptimalValues(1000, 0.01)

	if M10 > M05 || M05 > M01 {
		t.Error("space efficiency should decrease with lower P(FP)", M10, M05, M01)
	}
}

// TestNewFilter verifies bad inputs, optimal values, size of slots data.
func TestNewFilter(t *testing.T) {
	if _, err := newFilter(0, 3, 0.10); err == nil {
		t.Error("newFilter should not accept N == 0")
	}
	if _, err := newFilter(1, 0, 0.10); err == nil {
		t.Error("newFilter should not accept 0 bits")
	}
	if _, err := newFilter(1, 9, 0.10); err == nil {
		t.Error("newFilter should not accept more than 8 bits")
	}
	if _, err := newFilter(1, 16, 0.10); err == nil {
		t.Error("newFilter should not accept bits B which are > 8")
	}
	f, err := newFilter(1000, 8, 0.01)
	if err != nil {
		t.Error("unable to create a filter")
	}
	M, K := computeOptimalValues(1000, 0.01)
	if M != f.M || K != f.K {
		t.Error("optimal values not used", M, K, f)
	}
	if len(f.Data) != int(M) {
		t.Error("slots data should require M bytes")
	}

	// Try all byte slot sizes.
	for B := uint32(1); B <= 8; B++ {
		f, err := newFilter(1000, B, 0.01)
		if err != nil {
			t.Error("unable to create a filter")
		}
		expSize := int((M*B + 7) / 8)
		if len(f.Data) != expSize {
			t.Error("slot sizes don't match", len(f.Data), expSize)
		}
		if f.MaxCount != 1<<B-1 {
			t.Error("max count incorrect", f.MaxCount, 1<<B-1)
		}
	}
}

// TestSlots tests slot increment and slot count fetching.
func TestSlots(t *testing.T) {
	for b := 1; b <= 8; b++ {
		f, err := newFilter(10, uint32(b), 0.10)
		if err != nil {
			t.Error("unable to create a filter")
		}
		// Verify all slots empty.
		for i := 0; i < len(f.Data); i++ {
			if f.Data[i] != 0 {
				t.Errorf("slot %d not empty", i)
			}
		}
		// Increment each slot and verify.
		for s := uint32(0); s < f.M; s++ {
			f.incrementSlot(s, 1)
			if f.getSlot(s) != 1 {
				t.Errorf("slot value %d != 1", f.getSlot(s))
			}
			// Increment past max count.
			f.incrementSlot(s, int32(f.MaxCount))
			if f.getSlot(s) != f.MaxCount {
				t.Errorf("slot value should be max %d != %d", f.getSlot(s), f.MaxCount)
			}
			// Decrement once.
			f.incrementSlot(s, -1)
			if f.getSlot(s) != f.MaxCount-1 {
				t.Errorf("slot value should be max-1 %d != %d", f.getSlot(s), f.MaxCount-1)
			}
			// Decrement past 0.
			f.incrementSlot(s, -int32(f.MaxCount))
			if f.getSlot(s) != 0 {
				t.Errorf("slot value should be 0 %d != 0", f.getSlot(s))
			}
			// Increment all slots up to MaxCount and verify gets.
			for i := uint32(0); i < f.MaxCount; i++ {
				f.incrementSlot(s, 1)
				if f.getSlot(s) != i+1 {
					t.Errorf("slot value should be %d != %d", i+1, f.getSlot(s))
				}
			}
		}
	}
}

// TestKeys adds keys, tests existence, and removes keys.
func TestKeys(t *testing.T) {
	f, err := newFilter(1000, 4, 0.01)
	if err != nil {
		t.Error("unable to create a filter")
	}
	if f.hasKey("a") {
		t.Error("filter shouldn't contain key a")
	}
	if f.addKey("a"); !f.hasKey("a") {
		t.Error("filter should contain key a")
	}
	if f.hasKey("b") {
		t.Error("filter should contain key b")
	}
	if f.removeKey("a"); f.hasKey("a") {
		t.Error("filter shouldn't contain key a after removal")
	}
	// Add key twice, verify it still exists after one removal.
	f.addKey("a")
	f.addKey("a")
	f.removeKey("a")
	if !f.hasKey("a") {
		t.Error("filter should still contain key a")
	}
}

// TestFalsePositives adds many keys and verifies false positive probability.
func TestFalsePositives(t *testing.T) {
	f, err := newFilter(1000, 4, 0.01)
	if err != nil {
		t.Error("unable to create a filter")
	}
	lastFP := float64(0)
	for i := 0; i < 1000; i++ {
		f.addKey(fmt.Sprintf("key-%d", i))
		if f.probFalsePositive() < lastFP {
			t.Error("P(FP) should increase")
		}
		lastFP = f.probFalsePositive()
	}
	for i := 0; i < 1000; i++ {
		if !f.hasKey(fmt.Sprintf("key-%d", i)) {
			t.Error("could not find key-", i)
		}
	}
	// Measure false positive rate empirically and verify
	// against filter's math.
	probFP := f.probFalsePositive()
	countFP := 0
	for i := 0; i < 1000; i++ {
		if f.hasKey(fmt.Sprintf("nonkey-%d", i)) {
			countFP++
		}
	}
	empFP := float64(countFP) / float64(1000)
	diff := math.Abs(probFP - empFP)
	if diff/probFP > 0.50 {
		t.Errorf("measured P(FP) > 50%% different from expected %f vs. %f", diff, empFP)
	}
}

// TestApproximateInsertions adds many keys with an overloaded filter and
// verifies that approximation degrades gracefully.
func TestApproximateInsertions(t *testing.T) {
	f, err := newFilter(10, 4, 0.10)
	if err != nil {
		t.Error("unable to create a filter")
	}
	for i := 0; i <= 200; i++ {
		f.addKey(fmt.Sprintf("key-%d", i))
		diff := i + 1 - int(f.approximateInsertions())
		if i > 150 && diff == 0 {
			t.Error("expected some approximation error at 150 insertions")
		}
	}
}
