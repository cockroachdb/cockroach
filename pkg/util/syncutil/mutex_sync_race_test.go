// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
