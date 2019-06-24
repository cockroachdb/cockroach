// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zerofields

import "testing"

func TestNoZeroField(t *testing.T) {
	type foo struct {
		X, Y int
	}
	testFoo := foo{1, 2}
	if err := NoZeroField(&testFoo); err != nil {
		t.Fatal(err)
	}
	if err := NoZeroField(interface{}(testFoo)); err != nil {
		t.Fatal(err)
	}
	testFoo.Y = 0
	if err := NoZeroField(&testFoo); err == nil {
		t.Fatal("expected an error")
	}
}
