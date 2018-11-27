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
// permissions and limitations under the License.

// +build !deadlock
// +build race

package syncutil

import "testing"

func TestAssertHeld(t *testing.T) {
	type mutex interface {
		Lock()
		Unlock()
		AssertHeld()
	}

	testCases := []struct {
		m mutex
	}{
		{&Mutex{}},
		{&RWMutex{}},
	}
	for _, c := range testCases {
		// The normal, successful case.
		c.m.Lock()
		c.m.AssertHeld()
		c.m.Unlock()

		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("did not get expected panic")
				} else if a, e := r.(string), "mutex is not locked"; a != e {
					t.Fatalf("got %q, expected %q", a, e)
				}
			}()
			c.m.AssertHeld()
		}()
	}
}
