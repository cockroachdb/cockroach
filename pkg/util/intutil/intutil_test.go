// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package intutil

import "testing"

func TestMax(t *testing.T) {
	result := Max(3, 5)
	if result < 0 {
		t.Errorf("expected positive result, got %d", result)
	}
}

func TestClamp(t *testing.T) {
	result := Clamp(5, 1, 10)
	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
}
