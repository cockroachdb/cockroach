// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package contentionutils

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// CapacityLimiter is used to specify the capacity of the buffer. This allows
// the size of the buffer to change during runtime.
type CapacityLimiter func() int64

// ConcurrentBufferGuard is a helper data structure that can be used to
// implement optimized concurrent linear write buffer.
//
// Note: this is a rather awkward implementation to work around the fact that
// Golang doesn't have generic (as of 1.17). Ideally, this would be implemented
// as a generic data structure as something like:
//
// template<typename T>
//
//	class ConcurrentBuffer<T> {
//	  std::vector<T> buffer;
//	  ...
//
// public:
//
//	  void write(T val);
//	  std::vector<T> read() const;
//	};
//
// To work around the lacking of generic, ConcurrentBufferGuard is designed to
// be embedded into higher-level structs that implements the buffer read/write
// operations, where the buffer's access is done in the higher-level structs.
type ConcurrentBufferGuard struct {
	flushSyncLock syncutil.RWMutex
	flushDone     sync.Cond

	limiter          CapacityLimiter
	onBufferFullSync onBufferFullHandler

	// atomicIdx is the index pointing into the fixed-length array within the
	// msgBlock.This should only be accessed using atomic package.
	atomicIdx int64
}

// onBufferFullHandler is called when the buffer is full. ConcurrentBufferGuard
// will handle the locking process to block all inflight writer requests. This
// means that onBufferFullHandler can safely assume that it is executed with
// exclusive access to the guarded buffer. The callback receives an integer
// index (currentWriterIndex) indicating the index where buffer is filled to.
type onBufferFullHandler func(currentWriterIndex int64)

// bufferWriteOp is called to perform a synchronized write to the guarded
// buffer. ConcurrentBufferGuard passes in a writerIdx into the callback.
// The callback can safely use the writerIdx to write to the guarded buffer
// without further synchronization.
type bufferWriteOp func(writerIdx int64)

// NewConcurrentBufferGuard returns a new instance of ConcurrentBufferGuard.
func NewConcurrentBufferGuard(
	limiter CapacityLimiter, fullHandler onBufferFullHandler,
) *ConcurrentBufferGuard {
	writeBuffer := &ConcurrentBufferGuard{
		limiter:          limiter,
		onBufferFullSync: fullHandler,
	}
	writeBuffer.flushDone.L = writeBuffer.flushSyncLock.RLocker()
	return writeBuffer
}

// AtomicWrite executes the bufferWriterOp atomically, where bufferWriterOp
// is a write operation into a shared linear buffer.
//
// Any write requests initially starts by holding a read lock (flushSyncLock)
// and then reserves a write-index to the shared buffer (a fixed-length array).
// If the reserved index is valid, AtomicWrite immediately executes the
// bufferWriteOp with the reserved index. However, if the reserved index is not
// valid, (that is, array index out of bound), there are two scenarios:
//  1. If the reserved index == size of the array, then the caller of AtomicWrite()
//     method is responsible for executing the onBufferFullHandler() callback. The
//     caller does so by upgrading the read-lock to a write-lock, therefore
//     blocks all future writers. After the callback is executed, the write-lock
//     is then downgraded to a read-lock.
//  2. If the reserved index > size of the array, then the caller of AtomicWrite()
//     is blocked until the array is flushed. This is achieved by waiting on the
//     conditional variable (flushDone) while holding onto the read-lock. After
//     the flush is completed, the writer is unblocked and allowed to retry.
func (c *ConcurrentBufferGuard) AtomicWrite(op bufferWriteOp) {
	size := c.limiter()
	c.flushSyncLock.RLock()
	defer c.flushSyncLock.RUnlock()
	for {
		reservedIdx := c.reserveMsgBlockIndex()
		if reservedIdx < size {
			op(reservedIdx)
			return
		} else if reservedIdx == size {
			c.syncRLocked()
		} else {
			c.flushDone.Wait()
		}
	}
}

// ForceSync blocks all inflight and upcoming write operation, to allow
// the onBufferFullHandler to be executed. This can be used to preemptively
// flushes the buffer.
func (c *ConcurrentBufferGuard) ForceSync() {
	c.flushSyncLock.Lock()
	defer c.flushSyncLock.Unlock()
	c.syncLocked()
}

// ForceSyncExec blocks all inflight and upcoming write operation, to allow
// the onBufferFullHandler to be executed. However, unlike ForceSync, ForceSyncExec
// executes the provided function prior to executing onBufferFullHandler, which allows
// callers to atomically apply state to this specific execution of the
// onBufferFullHandler.
func (c *ConcurrentBufferGuard) ForceSyncExec(fn func()) {
	c.flushSyncLock.Lock()
	defer c.flushSyncLock.Unlock()
	fn()
	c.syncLocked()
}

func (c *ConcurrentBufferGuard) syncRLocked() {
	// We upgrade the read-lock to a write-lock, then when we are done flushing,
	// the lock is downgraded to a read-lock.
	//
	// This code looks dangerous (upgrading a read-lock to a write-lock is not
	// an atomic operation), but it is safe here because the atomic add in
	// reserveMsgBlockIndex ensures that only one goroutine writing into the
	// buffer (the one that reserves the index just beyond the buffer's length)
	// will attempt to perform the flush. The other goroutines will either
	// write into the buffer and release their locks (if they reserved an index
	// within the buffer's length) or release their locks and wait on the
	// flushDone condition to try again.
	c.flushSyncLock.RUnlock()
	defer c.flushSyncLock.RLock()
	c.flushSyncLock.Lock()
	defer c.flushSyncLock.Unlock()
	c.syncLocked()
}

func (c *ConcurrentBufferGuard) syncLocked() {
	c.onBufferFullSync(c.currentWriterIndex())
	c.flushDone.Broadcast()
	c.rewindBuffer()
}

func (c *ConcurrentBufferGuard) rewindBuffer() {
	atomic.StoreInt64(&c.atomicIdx, 0)
}

func (c *ConcurrentBufferGuard) reserveMsgBlockIndex() int64 {
	return atomic.AddInt64(&c.atomicIdx, 1) - 1 // since array is 0-indexed.
}

func (c *ConcurrentBufferGuard) currentWriterIndex() int64 {
	sizeLimit := c.limiter()
	if curIdx := atomic.LoadInt64(&c.atomicIdx); curIdx < sizeLimit {
		return curIdx
	}
	return sizeLimit
}
