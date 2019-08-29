// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !deadlock
// +build race

package syncutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

		// The unsuccessful case.
		require.PanicsWithValue(t, "mutex is not write locked", c.m.AssertHeld)
	}
}

func TestAssertRHeld(t *testing.T) {
	var m RWMutex

	// The normal, successful case.
	m.RLock()
	m.AssertRHeld()
	m.RUnlock()

	// The normal case with two readers.
	m.RLock()
	m.RLock()
	m.AssertRHeld()
	m.RUnlock()
	m.RUnlock()

	// The case where a write lock is held.
	m.Lock()
	m.AssertRHeld()
	m.Unlock()

	// The unsuccessful case with no readers.
	require.PanicsWithValue(t, "mutex is not read locked", m.AssertRHeld)
}
