// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !deadlock && race

package syncutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssertHeld(t *testing.T) {
	type mutex interface {
		Lock()
		TryLock() bool
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

		// The successful case with TryLock.
		require.True(t, c.m.TryLock())
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

	// The successful case with TryRLock.
	require.True(t, m.TryRLock())
	m.AssertRHeld()
	m.RUnlock()

	// The normal case with two readers and TryRLock.
	require.True(t, m.TryRLock())
	require.True(t, m.TryRLock())
	m.AssertRHeld()
	m.RUnlock()
	m.RUnlock()

	// The case where a write lock is held with TryLock.
	require.True(t, m.TryLock())
	m.AssertRHeld()
	m.Unlock()

	// The unsuccessful case with no readers.
	require.PanicsWithValue(t, "mutex is not read locked", m.AssertRHeld)
}
