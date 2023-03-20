// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type writeBlockingFile struct {
	*blocker
	wrapped vfs.File
}

func (f *writeBlockingFile) Close() error {
	f.wait()
	return f.wrapped.Close()
}

func (f *writeBlockingFile) Read(p []byte) (n int, err error) {
	// NB: intentionally don't wait.
	return f.wrapped.Read(p)
}

func (f *writeBlockingFile) ReadAt(p []byte, off int64) (n int, err error) {
	// NB: intentionally don't wait.
	return f.wrapped.ReadAt(p, off)
}

func (f *writeBlockingFile) Write(p []byte) (n int, err error) {
	f.wait()
	return f.wrapped.Write(p)
}

func (f *writeBlockingFile) Preallocate(offset, length int64) error {
	f.wait()
	return f.wrapped.Preallocate(offset, length)
}

func (f *writeBlockingFile) Stat() (os.FileInfo, error) {
	f.wait()
	return f.wrapped.Stat()
}

func (f *writeBlockingFile) Sync() error {
	f.wait()
	return f.wrapped.Sync()
}

func (f *writeBlockingFile) SyncTo(length int64) (fullSync bool, err error) {
	f.wait()
	return f.wrapped.SyncTo(length)
}

func (f *writeBlockingFile) SyncData() error {
	f.wait()
	return f.wrapped.SyncData()
}

func (f *writeBlockingFile) Prefetch(offset int64, length int64) error {
	f.wait()
	return f.wrapped.Prefetch(offset, length)
}

func (f *writeBlockingFile) Fd() uintptr {
	// NB: intentionally don't wait.
	return f.wrapped.Fd()
}

type blocker struct {
	errorf   func(string, ...interface{})
	teardown <-chan struct{}
	blocking atomic.Pointer[chan struct{}]
}

// block is not idempotent.
func (b *blocker) block() {
	ch := make(chan struct{})
	old := b.blocking.Swap(&ch)
	if old != nil {
		panic("blocking a blocked FS")
	}
}

// unblock is idempotent.
func (b *blocker) unblock() {
	old := b.blocking.Swap(nil)
	if old != nil {
		close(*old)
	}
}

func (b *blocker) wait() {
	blockCh := b.blocking.Load()
	if blockCh == nil {
		return
	}
	select {
	case <-b.teardown:
		// Already tearing down before we even blocked, so don't log this as
		// a hanging waiter but just unblock.
		return
	default:
	}
	select {
	case <-*blockCh:
	case <-b.teardown:
		b.errorf("timed out:\n%s", debug.Stack())
	}
}

type writeBlockingFS struct {
	blocker
	wrapped vfs.FS
}

func newWriteBlockingFS(
	wrapped vfs.FS, teardown <-chan struct{}, errorf func(string, ...interface{}),
) *writeBlockingFS {
	fs := &writeBlockingFS{
		wrapped: wrapped,
	}
	fs.blocker.teardown = teardown
	fs.blocker.errorf = errorf
	return fs
}

func (fs *writeBlockingFS) wrap(f vfs.File) *writeBlockingFile {
	return &writeBlockingFile{wrapped: f, blocker: &fs.blocker}
}

func (fs *writeBlockingFS) Create(name string) (vfs.File, error) {
	fs.wait()
	f, err := fs.wrapped.Create(name)
	if err != nil {
		return nil, err
	}
	return fs.wrap(f), nil
}

func (fs *writeBlockingFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	fs.wait()
	f, err := fs.wrapped.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return fs.wrap(f), err
}

func (fs *writeBlockingFS) OpenDir(name string) (vfs.File, error) {
	fs.wait()
	f, err := fs.wrapped.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return fs.wrap(f), err
}

func (fs *writeBlockingFS) Link(oldname, newname string) error {
	fs.wait()
	return fs.Link(oldname, newname)
}

func (fs *writeBlockingFS) Remove(name string) error {
	fs.wait()
	return fs.wrapped.Remove(name)
}

func (fs *writeBlockingFS) RemoveAll(name string) error {
	fs.wait()
	return fs.wrapped.RemoveAll(name)
}

func (fs *writeBlockingFS) Rename(oldname, newname string) error {
	fs.wait()
	return fs.wrapped.Rename(oldname, newname)
}

func (fs *writeBlockingFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	fs.wait()
	f, err := fs.wrapped.ReuseForWrite(oldname, newname)
	if err != nil {
		return nil, err
	}
	return fs.wrap(f), nil
}

func (fs *writeBlockingFS) MkdirAll(dir string, perm os.FileMode) error {
	fs.wait()
	return fs.wrapped.MkdirAll(dir, perm)
}

func (fs *writeBlockingFS) Lock(name string) (io.Closer, error) {
	fs.wait()
	return fs.wrapped.Lock(name)
}

func (fs *writeBlockingFS) List(dir string) ([]string, error) {
	// NB: intentionally don't wait.
	return fs.wrapped.List(dir)
}

func (fs *writeBlockingFS) Stat(name string) (os.FileInfo, error) {
	// NB: intentionally don't wait.
	return fs.wrapped.Stat(name)
}

func (fs *writeBlockingFS) PathBase(path string) string {
	// NB: intentionally don't wait.
	return fs.wrapped.PathBase(path)
}

func (fs *writeBlockingFS) PathJoin(elem ...string) string {
	// NB: intentionally don't wait.
	return fs.wrapped.PathJoin(elem...)
}

func (fs *writeBlockingFS) PathDir(path string) string {
	// NB: intentionally don't wait.
	return fs.wrapped.PathDir(path)
}

func (fs *writeBlockingFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	fs.wait()
	return fs.wrapped.GetDiskUsage(path)
}

func TestWriteBlockingFS(t *testing.T) {
	defer leaktest.AfterTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), testutils.DefaultSucceedsSoonDuration)
	defer cancel()
	fs := newWriteBlockingFS(vfs.NewMem(), ctx.Done(), t.Errorf)
	f, err := fs.Create("foo")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, f.Close())
	}()

	fs.block()
	defer fs.unblock()

	writeDone := make(chan struct{})
	s := stop.NewStopper()
	defer s.Stop(ctx)

	require.NoError(t, s.RunAsyncTask(ctx, "write", func(ctx context.Context) {
		_, err := f.Write([]byte("hello"))
		close(writeDone)
		assert.NoError(t, err)
	}))

	select {
	case <-time.After(time.Millisecond):
	case <-writeDone:
		t.Fatalf("write went through on blocked FS")
	}

	fs.unblock()

	select {
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatalf("write blocked despite unblocked FS")
	case <-writeDone:
	}
}

// TestPebbleNonDurableWriteOnBlockedFS sets up a blocked vfs.FS
// with an inflight (blocked) sync write. It then verifies that
// a non-sync write can succeed without blocking, and that the
// sync write succeeds only once the engine is unblocked.
func TestPebbleNonDurableWriteOnBlockedFS(t *testing.T) {
	defer leaktest.AfterTest(t)
	st := cluster.MakeTestingClusterSettings()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs := newWriteBlockingFS(vfs.NewMem(), ctx.Done(), t.Errorf)

	eng := storage.InMemFromFS(ctx, fs, "", st)
	defer eng.Close()

	s := stop.NewStopper()
	defer s.Stop(ctx)

	require.NoError(t, eng.PutMVCC(storage.MVCCKey{
		Key:       roachpb.Key("warmup-key"),
		Timestamp: hlc.Timestamp{WallTime: 1},
	}, storage.MVCCValue{Value: roachpb.MakeValueFromString("warmup-value")}))

	fs.block()
	defer fs.unblock()

	syncDone := make(chan struct{})
	{
		b := eng.NewBatch()
		defer b.Close()
		require.NoError(t, b.PutMVCC(storage.MVCCKey{
			Key:       roachpb.Key("syncing-key"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		}, storage.MVCCValue{Value: roachpb.MakeValueFromString("syncing-value")}))

		require.NoError(t, s.RunAsyncTask(ctx, "commit-sync", func(ctx context.Context) {
			defer close(syncDone)
			assert.NoError(t, b.CommitNoSyncWait())
			assert.NoError(t, b.SyncWait())
		}))
	}

	select {
	case <-time.After(time.Millisecond):
	case <-syncDone:
		t.Fatalf("sync done despite blocked engine")
	}

	b := eng.NewBatch()
	defer b.Close()

	require.NoError(t, b.PutMVCC(storage.MVCCKey{
		Key:       roachpb.Key("blocking-key"),
		Timestamp: hlc.Timestamp{WallTime: 1},
	}, storage.MVCCValue{Value: roachpb.MakeValueFromString("blocking-value")}))

	nonSyncDone := make(chan struct{})
	require.NoError(t, s.RunAsyncTask(ctx, "commit-not-sync", func(ctx context.Context) {
		defer close(nonSyncDone)
		assert.NoError(t, b.Commit(false /* sync */))
	}))

	select {
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatalf("non-sync write unexpectedly blocked")
	case <-syncDone:
		t.Fatalf("sync write unexpectedly went through despite blocked engine")
	case <-nonSyncDone:
	}

	fs.unblock()

	select {
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatalf("sync write unexpectedly blocked despite unblocked engine")
	case <-syncDone:
	}
}

func TestWriteWithStuckLeaseholderDisk(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs := newWriteBlockingFS(vfs.NewMem(), ctx.Done(), t.Errorf)

	var n1 base.TestServerArgs
	n1.StoreSpecs = []base.StoreSpec{
		{InMemory: true, FS: fs},
	}

	var args base.TestClusterArgs
	args.ReplicationMode = base.ReplicationManual
	args.ServerArgsPerNode = map[int]base.TestServerArgs{
		0: n1,
	}
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	rangeID := tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...).RangeID

	repl, _, err := tc.Servers[0].Stores().GetReplicaForRangeID(ctx, rangeID)
	require.NoError(t, err)

	for key := 0; key < 10000; key++ {
		if key == 3 {
			fs.block()
			defer fs.unblock()
		}

		var ba kvpb.BatchRequest
		ba.Timestamp = tc.Servers[0].Clock().Now()
		put := kvpb.NewPut(append(k[:len(k):len(k)], byte(key)), roachpb.MakeValueFromString("hello"))
		ba.Add(put)

		tCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		stopStack := time.AfterFunc(300*time.Millisecond, func() {
			// Once writes are blocked, ctx cancellation is broken because of:
			// https://github.com/cockroachdb/cockroach/issues/98957
			if false {
				buf := make([]byte, 1<<20)
				buf = buf[:runtime.Stack(buf, true)]
				t.Logf("%s", buf)
			}
		}).Stop
		_, pErr := repl.Send(tCtx, &ba)
		cancel()
		stopStack()
		t.Logf("write %d: %v", key, pErr)
		if key > 9 && ctx.Err() != nil {
			fs.unblock()
			break
		}
		require.NoError(t, pErr.GoError())
	}
}
