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
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/timeutil"
)

func TestSucceedsSoon(t *testing.T) {
	// Try a method which always succeeds.
	SucceedsSoon(t, func() error { return nil })

	// Try a method which succeeds after a known duration.
	start := timeutil.Now()
	duration := time.Millisecond * 10
	SucceedsSoon(t, func() error {
		elapsed := time.Since(start)
		if elapsed > duration {
			return nil
		}
		return Errorf("%s elapsed, waiting until %s elapses", elapsed, duration)
	})
}

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
