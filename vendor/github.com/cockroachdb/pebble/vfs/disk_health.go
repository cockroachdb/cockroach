// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultTickInterval is the default interval between two ticks of each
	// diskHealthCheckingFile loop iteration.
	defaultTickInterval = 2 * time.Second
	// preallocatedSlotCount is the default number of slots available for
	// concurrent filesystem operations. The slot count may be exceeded, but
	// each additional slot will incur an additional allocation. We choose 16
	// here with the expectation that it is significantly more than required in
	// practice. See the comment above the diskHealthCheckingFS type definition.
	preallocatedSlotCount = 16
)

// diskHealthCheckingFile is a File wrapper to detect slow disk operations, and
// call onSlowDisk if a disk operation is seen to exceed diskSlowThreshold.
//
// This struct creates a goroutine (in startTicker()) that, at every tick
// interval, sees if there's a disk operation taking longer than the specified
// duration. This setup is preferable to creating a new timer at every disk
// operation, as it reduces overhead per disk operation.
type diskHealthCheckingFile struct {
	File

	onSlowDisk        func(time.Duration)
	diskSlowThreshold time.Duration
	tickInterval      time.Duration

	stopper        chan struct{}
	lastWriteNanos int64
}

// newDiskHealthCheckingFile instantiates a new diskHealthCheckingFile, with the
// specified time threshold and event listener.
func newDiskHealthCheckingFile(
	file File, diskSlowThreshold time.Duration, onSlowDisk func(time.Duration),
) *diskHealthCheckingFile {
	return &diskHealthCheckingFile{
		File:              file,
		onSlowDisk:        onSlowDisk,
		diskSlowThreshold: diskSlowThreshold,
		tickInterval:      defaultTickInterval,

		stopper: make(chan struct{}),
	}
}

// startTicker starts a new goroutine with a ticker to monitor disk operations.
// Can only be called if the ticker goroutine isn't running already.
func (d *diskHealthCheckingFile) startTicker() {
	if d.diskSlowThreshold == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(d.tickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-d.stopper:
				return

			case <-ticker.C:
				lastWriteNanos := atomic.LoadInt64(&d.lastWriteNanos)
				if lastWriteNanos == 0 {
					continue
				}
				lastWrite := time.Unix(0, lastWriteNanos)
				now := time.Now()
				if lastWrite.Add(d.diskSlowThreshold).Before(now) {
					// diskSlowThreshold was exceeded. Call the passed-in
					// listener.
					d.onSlowDisk(now.Sub(lastWrite))
				}
			}
		}
	}()
}

// stopTicker stops the goroutine started in startTicker.
func (d *diskHealthCheckingFile) stopTicker() {
	close(d.stopper)
}

// Write implements the io.Writer interface.
func (d *diskHealthCheckingFile) Write(p []byte) (n int, err error) {
	d.timeDiskOp(func() {
		n, err = d.File.Write(p)
	})
	return n, err
}

// Close implements the io.Closer interface.
func (d *diskHealthCheckingFile) Close() error {
	d.stopTicker()
	return d.File.Close()
}

// Sync implements the io.Syncer interface.
func (d *diskHealthCheckingFile) Sync() (err error) {
	d.timeDiskOp(func() {
		err = d.File.Sync()
	})
	return err
}

// timeDiskOp runs the specified closure and makes its timing visible to the
// monitoring goroutine, in case it exceeds one of the slow disk durations.
func (d *diskHealthCheckingFile) timeDiskOp(op func()) {
	if d == nil {
		op()
		return
	}

	atomic.StoreInt64(&d.lastWriteNanos, time.Now().UnixNano())
	defer func() {
		atomic.StoreInt64(&d.lastWriteNanos, 0)
	}()
	op()
}

// diskHealthCheckingFS adds disk-health checking facilities to a VFS.
// It times disk write operations in two ways:
//
// 1. Wrapping vfs.Files.
//
// The bulk of write I/O activity is file writing and syncing, invoked through
// the `vfs.File` interface. This VFS wraps all files open for writing with a
// special diskHealthCheckingFile implementation of the vfs.File interface. See
// above for the implementation.
//
// 2. Monitoring filesystem metadata operations.
//
// Filesystem metadata operations (create, link, remove, rename, etc) are also
// sources of disk writes. Unlike a vfs.File which requires Write and Sync calls
// to be sequential, a vfs.FS may receive these filesystem metadata operations
// in parallel. To accommodate this parallelism, the diskHealthCheckingFS's
// write-oriented filesystem operations record their start times into a 'slot'
// on the filesystem. A single long-running goroutine periodically scans the
// slots looking for slow operations.
//
// The number of slots on a diskHealthCheckingFS grows to a working set of the
// maximum concurrent filesystem operations. This is expected to be very few
// for these reasons:
//  1. Pebble has limited write concurrency. Flushes, compactions and WAL
//  rotations are the primary sources of filesystem metadata operations. With
//  the default max-compaction concurrency, these operations require at most 5
//  concurrent slots if all 5 perform a filesystem metadata operation
//  simultaneously.
//  2. Pebble's limited concurrent I/O writers spend most of their time
//  performing file I/O, not performing the filesystem metadata operations that
//  require recording a slot on the diskHealthCheckingFS.
//  3. In CockroachDB, each additional store/Pebble instance has its own vfs.FS
//  which provides a separate goroutine and set of slots.
//  4. In CockroachDB, many of the additional sources of filesystem metadata
//  operations (like encryption-at-rest) are sequential with respect to Pebble's
//  threads.
type diskHealthCheckingFS struct {
	tickInterval      time.Duration
	diskSlowThreshold time.Duration
	onSlowDisk        func(string, time.Duration)
	fs                FS
	mu                struct {
		sync.Mutex
		tickerRunning bool
		stopper       chan struct{}
		inflight      []*slot
	}
	// prealloc preallocates the memory for mu.inflight slots and the slice
	// itself. The contained fields are not accessed directly except by
	// WithDiskHealthChecks when initializing mu.inflight. The number of slots
	// in d.mu.inflight will grow to the maximum number of concurrent file
	// metadata operations (create, remove, link, etc). If the number of
	// concurrent operations never exceeds preallocatedSlotCount, we'll never
	// incur an additional allocation.
	prealloc struct {
		slots        [preallocatedSlotCount]slot
		slotPtrSlice [preallocatedSlotCount]*slot
	}
}

type slot struct {
	name       string
	startNanos int64
}

// diskHealthCheckingFS implements FS.
var _ FS = (*diskHealthCheckingFS)(nil)

// WithDiskHealthChecks wraps an FS and ensures that all write-oriented
// operations on the FS are wrapped with disk health detection checks. Disk
// operations that are observed to take longer than diskSlowThreshold trigger an
// onSlowDisk call.
//
// A threshold of zero disables disk-health checking.
func WithDiskHealthChecks(
	innerFS FS, diskSlowThreshold time.Duration, onSlowDisk func(string, time.Duration),
) (FS, io.Closer) {
	if diskSlowThreshold == 0 {
		return innerFS, noopCloser{}
	}

	fs := &diskHealthCheckingFS{
		fs:                innerFS,
		tickInterval:      defaultTickInterval,
		diskSlowThreshold: diskSlowThreshold,
		onSlowDisk:        onSlowDisk,
	}
	fs.mu.stopper = make(chan struct{})
	// The fs holds preallocated slots and a preallocated array of slot pointers
	// with equal length. Initialize the inflight slice to use a slice backed by
	// the preallocated array with each slot initialized to a preallocated slot.
	fs.mu.inflight = fs.prealloc.slotPtrSlice[:]
	for i := range fs.mu.inflight {
		fs.mu.inflight[i] = &fs.prealloc.slots[i]
	}
	return fs, fs
}

func (d *diskHealthCheckingFS) timeFilesystemOp(name string, op func()) {
	if d == nil {
		op()
		return
	}

	// Record this operation's start time on the FS, so that the long-running
	// goroutine can monitor the filesystem operation.
	//
	// The diskHealthCheckingFile implementation uses a single field that is
	// atomically updated, taking advantage of the fact that writes to a single
	// vfs.File handle are not performed in parallel. The vfs.FS however may
	// receive write filesystem operations in parallel. To accommodate this
	// parallelism, writing goroutines append their start time to a
	// mutex-protected vector. On ticks, the long-running goroutine scans the
	// vector searching for start times older than the slow-disk threshold. When
	// a writing goroutine completes its operation, it atomically overwrites its
	// slot to signal completion.
	var s *slot
	func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		// If there's no long-running goroutine to monitor this filesystem
		// operation, start one.
		if !d.mu.tickerRunning {
			d.startTickerLocked()
		}

		startNanos := time.Now().UnixNano()
		for i := 0; i < len(d.mu.inflight); i++ {
			if atomic.LoadInt64(&d.mu.inflight[i].startNanos) == 0 {
				// This slot is not in use. Claim it.
				s = d.mu.inflight[i]
				s.name = name
				atomic.StoreInt64(&s.startNanos, startNanos)
				break
			}
		}
		// If we didn't find any unused slots, create a new slot and append it.
		// This slot will exist forever. The number of slots will grow to the
		// maximum number of concurrent filesystem operations over the lifetime
		// of the process. Only operations that grow the number of slots must
		// incur an allocation.
		if s == nil {
			s = &slot{
				name:       name,
				startNanos: startNanos,
			}
			d.mu.inflight = append(d.mu.inflight, s)
		}
	}()

	op()

	// Signal completion by zeroing the start time.
	atomic.StoreInt64(&s.startNanos, 0)
}

// startTickerLocked starts a new goroutine with a ticker to monitor disk
// filesystem operations. Requires d.mu and !d.mu.tickerRunning.
func (d *diskHealthCheckingFS) startTickerLocked() {
	d.mu.tickerRunning = true
	stopper := d.mu.stopper
	go func() {
		ticker := time.NewTicker(d.tickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Scan the inflight slots for any slots recording a start
				// time older than the diskSlowThreshold.
				d.mu.Lock()
				now := time.Now()
				for i := range d.mu.inflight {
					nanos := atomic.LoadInt64(&d.mu.inflight[i].startNanos)
					if nanos != 0 && time.Unix(0, nanos).Add(d.diskSlowThreshold).Before(now) {
						// diskSlowThreshold was exceeded. Invoke the provided
						// callback.
						d.onSlowDisk(d.mu.inflight[i].name, now.Sub(time.Unix(0, nanos)))
					}
				}
				d.mu.Unlock()
			case <-stopper:
				return
			}
		}
	}()
}

// Close implements io.Closer. Close stops the long-running goroutine that
// monitors for slow filesystem metadata operations. Close may be called
// multiple times. If the filesystem is used after Close has been called, a new
// long-running goroutine will be created.
func (d *diskHealthCheckingFS) Close() error {
	d.mu.Lock()
	if !d.mu.tickerRunning {
		// Nothing to stop.
		d.mu.Unlock()
		return nil
	}

	// Grab the stopper so we can request the long-running goroutine to stop.
	// Replace the stopper in case this FS is reused. It's possible to Close and
	// reuse a disk-health checking FS. This is to accommodate the on-by-default
	// behavior in Pebble, and the possibility that users may continue to use
	// the Pebble default FS beyond the lifetime of a single DB.
	stopper := d.mu.stopper
	d.mu.stopper = make(chan struct{})
	d.mu.tickerRunning = false
	d.mu.Unlock()

	// Ask the long-running goroutine to stop. This is a synchronous channel
	// send.
	stopper <- struct{}{}
	close(stopper)
	return nil
}

// Create implements the FS interface.
func (d *diskHealthCheckingFS) Create(name string) (File, error) {
	var f File
	var err error
	d.timeFilesystemOp(name, func() {
		f, err = d.fs.Create(name)
	})
	if err != nil {
		return f, err
	}
	if d.diskSlowThreshold == 0 {
		return f, nil
	}
	checkingFile := newDiskHealthCheckingFile(f, d.diskSlowThreshold, func(duration time.Duration) {
		d.onSlowDisk(name, duration)
	})
	checkingFile.startTicker()
	return WithFd(f, checkingFile), nil
}

// GetDiskUsage implements the FS interface.
func (d *diskHealthCheckingFS) GetDiskUsage(path string) (DiskUsage, error) {
	return d.fs.GetDiskUsage(path)
}

// Link implements the FS interface.
func (d *diskHealthCheckingFS) Link(oldname, newname string) error {
	var err error
	d.timeFilesystemOp(newname, func() {
		err = d.fs.Link(oldname, newname)
	})
	return err
}

// List implements the FS interface.
func (d *diskHealthCheckingFS) List(dir string) ([]string, error) {
	return d.fs.List(dir)
}

// Lock implements the FS interface.
func (d *diskHealthCheckingFS) Lock(name string) (io.Closer, error) {
	return d.fs.Lock(name)
}

// MkdirAll implements the FS interface.
func (d *diskHealthCheckingFS) MkdirAll(dir string, perm os.FileMode) error {
	var err error
	d.timeFilesystemOp(dir, func() {
		err = d.fs.MkdirAll(dir, perm)
	})
	return err
}

// Open implements the FS interface.
func (d *diskHealthCheckingFS) Open(name string, opts ...OpenOption) (File, error) {
	return d.fs.Open(name, opts...)
}

// OpenDir implements the FS interface.
func (d *diskHealthCheckingFS) OpenDir(name string) (File, error) {
	f, err := d.fs.OpenDir(name)
	if err != nil {
		return f, err
	}
	// Directories opened with OpenDir must be opened with health checking,
	// because they may be explicitly synced.
	checkingFile := newDiskHealthCheckingFile(f, d.diskSlowThreshold, func(duration time.Duration) {
		d.onSlowDisk(name, duration)
	})
	checkingFile.startTicker()
	return WithFd(f, checkingFile), nil
}

// PathBase implements the FS interface.
func (d *diskHealthCheckingFS) PathBase(path string) string {
	return d.fs.PathBase(path)
}

// PathJoin implements the FS interface.
func (d *diskHealthCheckingFS) PathJoin(elem ...string) string {
	return d.fs.PathJoin(elem...)
}

// PathDir implements the FS interface.
func (d *diskHealthCheckingFS) PathDir(path string) string {
	return d.fs.PathDir(path)
}

// Remove implements the FS interface.
func (d *diskHealthCheckingFS) Remove(name string) error {
	var err error
	d.timeFilesystemOp(name, func() {
		err = d.fs.Remove(name)
	})
	return err
}

// RemoveAll implements the FS interface.
func (d *diskHealthCheckingFS) RemoveAll(name string) error {
	var err error
	d.timeFilesystemOp(name, func() {
		err = d.fs.RemoveAll(name)
	})
	return err
}

// Rename implements the FS interface.
func (d *diskHealthCheckingFS) Rename(oldname, newname string) error {
	var err error
	d.timeFilesystemOp(newname, func() {
		err = d.fs.Rename(oldname, newname)
	})
	return err
}

// ReuseForWrite implements the FS interface.
func (d *diskHealthCheckingFS) ReuseForWrite(oldname, newname string) (File, error) {
	var f File
	var err error
	d.timeFilesystemOp(newname, func() {
		f, err = d.fs.ReuseForWrite(oldname, newname)
	})
	if err != nil {
		return f, err
	}
	if d.diskSlowThreshold == 0 {
		return f, nil
	}
	checkingFile := newDiskHealthCheckingFile(f, d.diskSlowThreshold, func(duration time.Duration) {
		d.onSlowDisk(newname, duration)
	})
	checkingFile.startTicker()
	return WithFd(f, checkingFile), nil
}

// Stat implements the FS interface.
func (d *diskHealthCheckingFS) Stat(name string) (os.FileInfo, error) {
	return d.fs.Stat(name)
}

type noopCloser struct{}

func (noopCloser) Close() error { return nil }
