// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
)

// DiskStallController provides a high-level API for simulating disk stalls
// in unit tests. It wraps a VFS with an errorfs injector that can block,
// delay, or throttle I/O operations.
//
// Example usage:
//
//	controller := NewDiskStallController(vfs.NewMem(), nil)
//	env := storage.InitEnv(ctx, controller.FS(), ...)
//	db, _ := storage.Open(ctx, env, settings)
//
//	// Later in test:
//	controller.Block()           // Stall all I/O
//	controller.WaitForBlocked(1) // Wait for operation to block
//	// ... verify timeout behavior ...
//	controller.Release()         // Resume I/O
//
// Thread-safety: All methods are safe for concurrent use.
type DiskStallController struct {
	// wrappedFS is the VFS with error injection enabled.
	wrappedFS *errorfs.FS

	// injector controls the stall behavior.
	injector *StallableInjector

	// underlying is the original VFS before wrapping.
	underlying vfs.FS
}

// NewDiskStallController creates a new DiskStallController that wraps the
// given VFS.
//
// The opFilter function, if non-nil, determines which operations are subject
// to stalling. Common filters are available in this package:
//   - StallAllOps: stall all operations
//   - StallWriteOps: stall only write operations
//   - StallReadOps: stall only read operations
//   - StallSyncOps: stall only sync operations
//
// If opFilter is nil, all operations are affected.
func NewDiskStallController(fs vfs.FS, opFilter func(errorfs.Op) bool) *DiskStallController {
	injector := NewStallableInjector(opFilter)
	wrappedFS := errorfs.Wrap(fs, injector)

	return &DiskStallController{
		wrappedFS:  wrappedFS,
		injector:   injector,
		underlying: fs,
	}
}

// NewDiskStallControllerFromInjector creates a DiskStallController from an
// existing StallableInjector. This is used when the VFS wrapping is done
// externally (e.g., via Env.WrapFS) and we just need a controller handle.
//
// The wrappedFS parameter should be the already-wrapped VFS that the injector
// is attached to.
func NewDiskStallControllerFromInjector(injector *StallableInjector, wrappedFS vfs.FS) *DiskStallController {
	return &DiskStallController{
		wrappedFS:  nil, // Not an errorfs.FS since wrapping was done externally
		injector:   injector,
		underlying: wrappedFS,
	}
}

// FS returns the wrapped VFS that should be used by the storage engine.
// This VFS has stall injection capabilities.
func (c *DiskStallController) FS() vfs.FS {
	return c.wrappedFS
}

// UnderlyingFS returns the original VFS before wrapping.
func (c *DiskStallController) UnderlyingFS() vfs.FS {
	return c.underlying
}

// Injector returns the underlying StallableInjector for advanced use cases.
func (c *DiskStallController) Injector() *StallableInjector {
	return c.injector
}

// Block configures the controller to block all matching I/O operations
// indefinitely until Release() is called. This simulates a complete disk stall.
//
// Operations already in progress will not be affected; only new operations
// will block.
func (c *DiskStallController) Block() {
	c.injector.SetBlock()
}

// Release unblocks all currently blocked I/O operations and disables
// blocking mode. This simulates disk recovery from a stall.
func (c *DiskStallController) Release() {
	c.injector.Release()
}

// SetDelay configures the controller to add a fixed delay to all matching
// I/O operations. This simulates slow disk response times.
//
// Example: controller.SetDelay(100 * time.Millisecond)
func (c *DiskStallController) SetDelay(d time.Duration) {
	c.injector.SetDelay(d)
}

// SetThrottle configures the controller to limit I/O throughput.
// The opsPerSec parameter specifies the maximum operations per second.
//
// Note: This is approximate since we count operations rather than bytes.
// Each I/O operation consumes one "token" from the rate limiter.
func (c *DiskStallController) SetThrottle(opsPerSec int64) {
	c.injector.SetThrottle(opsPerSec)
}

// Disable disables all stall behavior. Operations will proceed normally.
// This is useful for cleanup or temporarily disabling stalls.
func (c *DiskStallController) Disable() {
	c.injector.Disable()
}

// Enable enables the stall behavior. Call this after configuring the stall
// mode if you previously called Disable().
func (c *DiskStallController) Enable() {
	c.injector.Enable()
}

// IsEnabled returns true if stalling is currently enabled.
func (c *DiskStallController) IsEnabled() bool {
	return c.injector.IsEnabled()
}

// WaitForBlocked blocks the calling goroutine until at least n operations
// are blocked waiting on I/O. This is essential for synchronizing test
// assertions - call this after Block() to ensure operations have actually
// reached the blocking point before checking behavior.
//
// Example:
//
//	controller.Block()
//	go func() { db.Put(key, value) }() // This will block
//	controller.WaitForBlocked(1)        // Wait for the Put to block
//	// Now safe to check timeouts, leadership changes, etc.
//	controller.Release()
func (c *DiskStallController) WaitForBlocked(n int) {
	c.injector.WaitForBlocked(n)
}

// BlockedCount returns the number of operations currently blocked on I/O.
func (c *DiskStallController) BlockedCount() int {
	return c.injector.BlockedCount()
}

// StalledOps returns the total number of operations that have been stalled
// since the controller was created. This is useful for verifying that
// operations are actually hitting the stall logic.
func (c *DiskStallController) StalledOps() int64 {
	return c.injector.StalledOps()
}

// Mode returns the current stall mode.
func (c *DiskStallController) Mode() StallMode {
	return c.injector.Mode()
}
