// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package faults

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// TestDiskFaultsInProcess demonstrates in-process disk fault injection
// without real disk operations.
func TestDiskFaultsInProcess(t *testing.T) {
	ctx := context.Background()
	_ = ctx

	fc := NewController()

	// Create an in-memory VFS
	memFS := vfs.NewMem()

	// Wrap with fault injection - this wrapper always intercepts operations
	// but only applies faults when enabled
	injector := fc.DiskStallInjector(nil)
	wrappedFS := errorfs.Wrap(memFS, injector)

	// Create a test file - no faults enabled, should be fast
	f, err := wrappedFS.Create("test.txt", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.Equal(t, int64(0), fc.DiskStalledOps())

	// Now enable delay fault
	fc.SetDiskDelay(10 * time.Millisecond)

	start := time.Now()
	f, err = wrappedFS.Create("test2.txt", vfs.WriteCategoryUnspecified)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Should have been delayed
	require.GreaterOrEqual(t, elapsed, 10*time.Millisecond)
	require.Greater(t, fc.DiskStalledOps(), int64(0))

	// Disable faults
	fc.DisableDiskFaults()

	// Operations should be fast again
	start = time.Now()
	f, err = wrappedFS.Create("test3.txt", vfs.WriteCategoryUnspecified)
	elapsed = time.Since(start)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Less(t, elapsed, 10*time.Millisecond)
}

// TestDiskBlockAndRelease demonstrates blocking and releasing disk operations.
func TestDiskBlockAndRelease(t *testing.T) {
	fc := NewController()

	memFS := vfs.NewMem()
	injector := fc.DiskStallInjector(nil)
	wrappedFS := errorfs.Wrap(memFS, injector)

	// Enable blocking
	fc.SetDiskBlock()

	var writeCompleted atomic.Bool
	go func() {
		f, _ := wrappedFS.Create("blocked.txt", vfs.WriteCategoryUnspecified)
		if f != nil {
			f.Close()
		}
		writeCompleted.Store(true)
	}()

	// Wait for the operation to block
	fc.WaitForDiskBlocked(1)
	require.False(t, writeCompleted.Load())
	require.Equal(t, 1, fc.DiskBlockedCount())

	// Release
	fc.ReleaseDisk()

	// Should complete
	require.Eventually(t, func() bool {
		return writeCompleted.Load()
	}, 5*time.Second, 10*time.Millisecond)
}

// mockSender is a simple mock kv.Sender for testing.
type mockSender struct {
	sendCount atomic.Int64
}

func (m *mockSender) Send(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	m.sendCount.Add(1)
	return &kvpb.BatchResponse{}, nil
}

// TestSenderFaultsInProcess demonstrates in-process sender fault injection.
func TestSenderFaultsInProcess(t *testing.T) {
	fc := NewController()

	mock := &mockSender{}

	// Wrap the sender - wrapper always intercepts but only applies faults when enabled
	faultySender := fc.WrapSender(mock)

	// No faults - should pass through
	ctx := context.Background()
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("key1")}})

	_, pErr := faultySender.Send(ctx, ba)
	require.Nil(t, pErr)
	require.Equal(t, int64(1), mock.sendCount.Load())

	// Enable delay
	fc.SetSenderDelay(20 * time.Millisecond)

	start := time.Now()
	_, pErr = faultySender.Send(ctx, ba)
	elapsed := time.Since(start)
	require.Nil(t, pErr)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond)

	// Disable
	fc.DisableSenderFaults()

	start = time.Now()
	_, pErr = faultySender.Send(ctx, ba)
	elapsed = time.Since(start)
	require.Nil(t, pErr)
	require.Less(t, elapsed, 20*time.Millisecond)
}

// TestSenderBlockAndRelease demonstrates blocking and releasing sender operations.
func TestSenderBlockAndRelease(t *testing.T) {
	fc := NewController()

	mock := &mockSender{}
	faultySender := fc.WrapSender(mock)

	// Enable blocking
	fc.SetSenderBlock()

	var sendCompleted atomic.Bool
	var sendErr atomic.Pointer[kvpb.Error]
	go func() {
		ctx := context.Background()
		ba := &kvpb.BatchRequest{}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("key1")}})
		_, pErr := faultySender.Send(ctx, ba)
		if pErr != nil {
			sendErr.Store(pErr)
		}
		sendCompleted.Store(true)
	}()

	// Wait for the operation to block
	fc.WaitForSenderBlocked(1)
	require.False(t, sendCompleted.Load())

	// Release
	fc.ReleaseSender()

	// Should complete
	require.Eventually(t, func() bool {
		return sendCompleted.Load()
	}, 5*time.Second, 10*time.Millisecond)
	require.Nil(t, sendErr.Load())
}

// TestMultipleFaultsDynamically demonstrates changing fault types at runtime.
func TestMultipleFaultsDynamically(t *testing.T) {
	fc := NewController()

	mock := &mockSender{}
	faultySender := fc.WrapSender(mock)

	ctx := context.Background()
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("key1")}})

	// Phase 1: No faults
	_, pErr := faultySender.Send(ctx, ba)
	require.Nil(t, pErr)

	// Phase 2: Add delay
	fc.SetSenderDelay(10 * time.Millisecond)
	start := time.Now()
	_, pErr = faultySender.Send(ctx, ba)
	require.Nil(t, pErr)
	require.GreaterOrEqual(t, time.Since(start), 10*time.Millisecond)

	// Phase 3: Switch to error
	fc.SetSenderError(context.DeadlineExceeded)
	_, pErr = faultySender.Send(ctx, ba)
	require.NotNil(t, pErr)
	require.ErrorIs(t, pErr.GoError(), context.DeadlineExceeded)

	// Phase 4: Disable completely
	fc.DisableSenderFaults()
	_, pErr = faultySender.Send(ctx, ba)
	require.Nil(t, pErr)
}

// TestKeyFilteredFaults demonstrates filtering faults by key.
func TestKeyFilteredFaults(t *testing.T) {
	fc := NewController()

	mock := &mockSender{}
	faultySender := fc.WrapSender(mock)

	// Only affect keys starting with "slow-"
	fc.SetSenderKeyFilter(func(key []byte) bool {
		return len(key) >= 5 && string(key[:5]) == "slow-"
	})
	fc.SetSenderDelay(50 * time.Millisecond)

	ctx := context.Background()

	// Request with non-matching key - should be fast
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("fast-key")}})
	start := time.Now()
	_, pErr := faultySender.Send(ctx, ba)
	require.Nil(t, pErr)
	require.Less(t, time.Since(start), 50*time.Millisecond)

	// Request with matching key - should be slow
	ba = &kvpb.BatchRequest{}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("slow-key")}})
	start = time.Now()
	_, pErr = faultySender.Send(ctx, ba)
	require.Nil(t, pErr)
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}
