// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

func TestStallableInjector_Basic(t *testing.T) {
	injector := NewStallableInjector(nil)

	// Initially disabled.
	require.False(t, injector.IsEnabled())
	require.Equal(t, StallModeNone, injector.Mode())

	// Operations should pass through when disabled.
	op := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test/file.sst"}
	err := injector.MaybeError(op)
	require.NoError(t, err)
	require.Equal(t, int64(0), injector.StalledOps())
}

func TestStallableInjector_Block(t *testing.T) {
	injector := NewStallableInjector(nil)

	// Set up blocking mode.
	injector.SetBlock()
	require.True(t, injector.IsEnabled())
	require.Equal(t, StallModeBlock, injector.Mode())

	// Start a goroutine that will block.
	var blocked atomic.Bool
	var released atomic.Bool
	go func() {
		blocked.Store(true)
		op := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test/file.sst"}
		_ = injector.MaybeError(op)
		released.Store(true)
	}()

	// Wait for the goroutine to block.
	injector.WaitForBlocked(1)
	require.Equal(t, 1, injector.BlockedCount())
	require.True(t, blocked.Load())
	require.False(t, released.Load())

	// Release the blocked operation.
	injector.Release()

	// Wait for the goroutine to complete.
	require.Eventually(t, func() bool {
		return released.Load()
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, 0, injector.BlockedCount())
	require.Equal(t, StallModeNone, injector.Mode())
	require.Equal(t, int64(1), injector.StalledOps())
}

func TestStallableInjector_BlockMultiple(t *testing.T) {
	injector := NewStallableInjector(nil)
	injector.SetBlock()

	const numGoroutines = 5
	var wg sync.WaitGroup
	var releasedCount atomic.Int32

	// Start multiple goroutines that will block.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test/file.sst"}
			_ = injector.MaybeError(op)
			releasedCount.Add(1)
		}()
	}

	// Wait for all to block.
	injector.WaitForBlocked(numGoroutines)
	require.Equal(t, numGoroutines, injector.BlockedCount())
	require.Equal(t, int32(0), releasedCount.Load())

	// Release all.
	injector.Release()
	wg.Wait()

	require.Equal(t, int32(numGoroutines), releasedCount.Load())
	require.Equal(t, 0, injector.BlockedCount())
}

func TestStallableInjector_Delay(t *testing.T) {
	injector := NewStallableInjector(nil)

	delay := 50 * time.Millisecond
	injector.SetDelay(delay)
	require.True(t, injector.IsEnabled())
	require.Equal(t, StallModeDelay, injector.Mode())

	op := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test/file.sst"}

	start := time.Now()
	err := injector.MaybeError(op)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsed, delay)
	require.Equal(t, int64(1), injector.StalledOps())
}

func TestStallableInjector_Throttle(t *testing.T) {
	injector := NewStallableInjector(nil)

	// Set a very low rate to make the effect measurable.
	opsPerSec := int64(10)
	injector.SetThrottle(opsPerSec)
	require.True(t, injector.IsEnabled())
	require.Equal(t, StallModeThrottle, injector.Mode())

	op := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test/file.sst"}

	// First operation should be fast (burst).
	start := time.Now()
	for i := 0; i < 5; i++ {
		err := injector.MaybeError(op)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)

	require.Equal(t, int64(5), injector.StalledOps())
	// With burst capacity, this should be relatively fast.
	require.Less(t, elapsed, time.Second)
}

func TestStallableInjector_Filter(t *testing.T) {
	// Only stall write operations.
	injector := NewStallableInjector(StallWriteOps)
	injector.SetDelay(50 * time.Millisecond)

	// Write operation should be delayed.
	writeOp := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test/file.sst"}
	start := time.Now()
	err := injector.MaybeError(writeOp)
	writeElapsed := time.Since(start)
	require.NoError(t, err)
	require.GreaterOrEqual(t, writeElapsed, 50*time.Millisecond)

	// Read operation should not be delayed.
	readOp := errorfs.Op{Kind: errorfs.OpFileRead, Path: "/test/file.sst"}
	start = time.Now()
	err = injector.MaybeError(readOp)
	readElapsed := time.Since(start)
	require.NoError(t, err)
	require.Less(t, readElapsed, 10*time.Millisecond)

	// Only write was counted as stalled.
	require.Equal(t, int64(1), injector.StalledOps())
}

func TestStallableInjector_DisableEnable(t *testing.T) {
	injector := NewStallableInjector(nil)

	// Set up delay but disable.
	injector.SetDelay(100 * time.Millisecond)
	require.True(t, injector.IsEnabled())

	injector.Disable()
	require.False(t, injector.IsEnabled())

	// Operation should not be delayed when disabled.
	op := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test/file.sst"}
	start := time.Now()
	err := injector.MaybeError(op)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Less(t, elapsed, 10*time.Millisecond)
	require.Equal(t, int64(0), injector.StalledOps())

	// Re-enable.
	injector.Enable()
	require.True(t, injector.IsEnabled())

	start = time.Now()
	err = injector.MaybeError(op)
	elapsed = time.Since(start)
	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	require.Equal(t, int64(1), injector.StalledOps())
}

func TestStallableInjector_String(t *testing.T) {
	injector := NewStallableInjector(nil)

	s := injector.String()
	require.Contains(t, s, "StallableInjector")
	require.Contains(t, s, "enabled=false")
	require.Contains(t, s, "mode=none")

	injector.SetBlock()
	s = injector.String()
	require.Contains(t, s, "enabled=true")
	require.Contains(t, s, "mode=block")
}

func TestStallableInjector_RepeatedBlockRelease(t *testing.T) {
	injector := NewStallableInjector(nil)

	// Multiple block/release cycles should work correctly.
	for i := 0; i < 3; i++ {
		injector.SetBlock()

		var blocked atomic.Bool
		go func() {
			op := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test/file.sst"}
			blocked.Store(true)
			_ = injector.MaybeError(op)
		}()

		injector.WaitForBlocked(1)
		require.True(t, blocked.Load())

		injector.Release()

		require.Eventually(t, func() bool {
			return injector.BlockedCount() == 0
		}, time.Second, 10*time.Millisecond)
	}
}

func TestDiskStallController_Basic(t *testing.T) {
	memFS := vfs.NewMem()
	controller := NewDiskStallController(memFS, nil)

	require.NotNil(t, controller.FS())
	require.Equal(t, memFS, controller.UnderlyingFS())
	require.NotNil(t, controller.Injector())
	require.False(t, controller.IsEnabled())
}

func TestDiskStallController_BlockRelease(t *testing.T) {
	memFS := vfs.NewMem()
	controller := NewDiskStallController(memFS, nil)
	fs := controller.FS()

	// Create a file first.
	f, err := fs.Create("/test.txt", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Now set up blocking.
	controller.Block()
	require.True(t, controller.IsEnabled())
	require.Equal(t, StallModeBlock, controller.Mode())

	// Start operation that will block.
	var blocked atomic.Bool
	var completed atomic.Bool
	go func() {
		blocked.Store(true)
		f, err := fs.Create("/test2.txt", vfs.WriteCategoryUnspecified)
		if err == nil {
			f.Close()
		}
		completed.Store(true)
	}()

	controller.WaitForBlocked(1)
	require.True(t, blocked.Load())
	require.False(t, completed.Load())
	require.Equal(t, 1, controller.BlockedCount())

	controller.Release()

	require.Eventually(t, func() bool {
		return completed.Load()
	}, time.Second, 10*time.Millisecond)
}

func TestDiskStallController_Delay(t *testing.T) {
	memFS := vfs.NewMem()
	controller := NewDiskStallController(memFS, nil)
	fs := controller.FS()

	delay := 50 * time.Millisecond
	controller.SetDelay(delay)

	start := time.Now()
	f, err := fs.Create("/test.txt", vfs.WriteCategoryUnspecified)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsed, delay)
	require.NoError(t, f.Close())
}

func TestDiskStallController_WithFilter(t *testing.T) {
	memFS := vfs.NewMem()
	// Only stall SST file operations.
	controller := NewDiskStallController(memFS, StallSSTOps)
	fs := controller.FS()

	delay := 50 * time.Millisecond
	controller.SetDelay(delay)

	// Non-SST file should not be delayed.
	start := time.Now()
	f, err := fs.Create("/test.log", vfs.WriteCategoryUnspecified)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Less(t, elapsed, delay)
	require.NoError(t, f.Close())

	// SST file should be delayed.
	start = time.Now()
	f, err = fs.Create("/test.sst", vfs.WriteCategoryUnspecified)
	elapsed = time.Since(start)
	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsed, delay)
	require.NoError(t, f.Close())
}

func TestPredicates(t *testing.T) {
	testCases := []struct {
		name      string
		predicate func(errorfs.Op) bool
		op        errorfs.Op
		expected  bool
	}{
		{
			name:      "StallAllOps matches write",
			predicate: StallAllOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test.txt"},
			expected:  true,
		},
		{
			name:      "StallAllOps matches read",
			predicate: StallAllOps,
			op:        errorfs.Op{Kind: errorfs.OpFileRead, Path: "/test.txt"},
			expected:  true,
		},
		{
			name:      "StallWriteOps matches write",
			predicate: StallWriteOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test.txt"},
			expected:  true,
		},
		{
			name:      "StallWriteOps does not match read",
			predicate: StallWriteOps,
			op:        errorfs.Op{Kind: errorfs.OpFileRead, Path: "/test.txt"},
			expected:  false,
		},
		{
			name:      "StallReadOps matches read",
			predicate: StallReadOps,
			op:        errorfs.Op{Kind: errorfs.OpFileRead, Path: "/test.txt"},
			expected:  true,
		},
		{
			name:      "StallReadOps does not match write",
			predicate: StallReadOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test.txt"},
			expected:  false,
		},
		{
			name:      "StallSyncOps matches sync",
			predicate: StallSyncOps,
			op:        errorfs.Op{Kind: errorfs.OpFileSync, Path: "/test.txt"},
			expected:  true,
		},
		{
			name:      "StallSyncOps does not match write",
			predicate: StallSyncOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/test.txt"},
			expected:  false,
		},
		{
			name:      "StallWALOps matches .log file",
			predicate: StallWALOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.log"},
			expected:  true,
		},
		{
			name:      "StallWALOps matches wal directory",
			predicate: StallWALOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/wal/000001"},
			expected:  true,
		},
		{
			name:      "StallWALOps does not match sst",
			predicate: StallWALOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.sst"},
			expected:  false,
		},
		{
			name:      "StallSSTOps matches sst",
			predicate: StallSSTOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.sst"},
			expected:  true,
		},
		{
			name:      "StallSSTOps does not match log",
			predicate: StallSSTOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.log"},
			expected:  false,
		},
		{
			name:      "StallManifestOps matches MANIFEST",
			predicate: StallManifestOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/MANIFEST-000001"},
			expected:  true,
		},
		{
			name:      "StallManifestOps does not match sst",
			predicate: StallManifestOps,
			op:        errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.sst"},
			expected:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.predicate(tc.op)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestCombinePredicates(t *testing.T) {
	// Combine WAL and SST predicates.
	combined := CombinePredicates(StallWALOps, StallSSTOps)

	walOp := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.log"}
	sstOp := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.sst"}
	otherOp := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/other.txt"}

	require.True(t, combined(walOp))
	require.True(t, combined(sstOp))
	require.False(t, combined(otherOp))
}

func TestAndPredicates(t *testing.T) {
	// Match only write operations on SST files.
	combined := AndPredicates(StallWriteOps, StallSSTOps)

	writeSST := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.sst"}
	readSST := errorfs.Op{Kind: errorfs.OpFileRead, Path: "/data/000001.sst"}
	writeLog := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.log"}

	require.True(t, combined(writeSST))
	require.False(t, combined(readSST))
	require.False(t, combined(writeLog))
}

func TestInvertPredicate(t *testing.T) {
	// Stall everything except SST files.
	inverted := InvertPredicate(StallSSTOps)

	sstOp := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.sst"}
	logOp := errorfs.Op{Kind: errorfs.OpFileWrite, Path: "/data/000001.log"}

	require.False(t, inverted(sstOp))
	require.True(t, inverted(logOp))
}
