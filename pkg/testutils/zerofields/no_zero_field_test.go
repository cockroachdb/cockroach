// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package zerofields

import (
	"testing"

	"github.com/cockroachdb/errors"
)

func TestNoZeroField(t *testing.T) {
	type foo struct {
		A int
		B int
	}
	type bar struct {
		X, Y int
		Z    foo
	}
	testFooNonZero := bar{1, 2, foo{3, 4}}
	testFoo := testFooNonZero
	if err := NoZeroField(&testFoo); err != nil {
		t.Fatal(err)
	}
	if err := NoZeroField(interface{}(testFoo)); err != nil {
		t.Fatal(err)
	}
	testFoo = testFooNonZero
	testFoo.Y = 0
	if err, exp := NoZeroField(&testFoo), (zeroFieldErr{"Y"}); !errors.Is(err, exp) {
		t.Fatalf("expected error %v, found %v", exp, err)
	}
	testFoo = testFooNonZero
	testFoo.Z.B = 0
	if err, exp := NoZeroField(&testFoo), (zeroFieldErr{"Z.B"}); !errors.Is(err, exp) {
		t.Fatalf("expected error %v, found %v", exp, err)
	}
}
