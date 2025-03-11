// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBroadcastTicker(t *testing.T) {
	bcastTicker := NewBroadcastTicker(1 * time.Millisecond)
	defer bcastTicker.Stop()

	// Create multiple receivers while the ticker is locked, to avoid races.
	c1, recv1 := bcastTicker.allocNewTicker()
	c2, recv2 := bcastTicker.allocNewTicker()
	bcastTicker.mu.Lock()
	bcastTicker.registerLocked(c1)
	bcastTicker.registerLocked(c2)
	bcastTicker.mu.Unlock()

	// Collect ticks from both receivers, which should be in sync.
	tick1 := <-recv1.C
	tick2 := <-recv2.C
	require.Equal(t, tick1, tick2)
}

func TestBroadcastTicker_Stop(t *testing.T) {
	bcastTicker := NewBroadcastTicker(1 * time.Millisecond)
	recv1 := bcastTicker.NewTicker()

	// Stop the ticker.
	bcastTicker.Stop()

	// Ensure at most one more tick is received.
	ticks := 0
Loop:
	for {
		select {
		case <-recv1.C:
			ticks++
		case <-time.After(10 * time.Millisecond):
			break Loop
		}
	}
	require.LessOrEqual(t, ticks, 1)
}

func TestBroadcastTicker_Stop_Receiver(t *testing.T) {
	bcastTicker := NewBroadcastTicker(1 * time.Millisecond)
	defer bcastTicker.Stop()
	recv1 := bcastTicker.NewTicker()

	// Stop the receiver (disconnect from the ticker).
	recv1.Stop()

	// Ensure at most one more tick is received.
	ticks := 0
Loop:
	for {
		select {
		case <-recv1.C:
			ticks++
		case <-time.After(10 * time.Millisecond):
			break Loop
		}
	}
	require.LessOrEqual(t, ticks, 1)
}

func TestBroadcastTicker_Reset(t *testing.T) {
	bcastTicker := NewBroadcastTicker(24 * time.Hour)
	defer bcastTicker.Stop()
	recv1 := bcastTicker.NewTicker()

	// Drop the ticker duration.
	bcastTicker.Reset(1 * time.Millisecond)

	// Ensure at least one tick comes in with the new duration, else the test
	// would hang and time out.
	<-recv1.C
}
