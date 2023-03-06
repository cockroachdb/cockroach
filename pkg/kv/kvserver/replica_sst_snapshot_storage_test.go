// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	io "io"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestSSTSnapshotStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newOnDiskEngine(ctx, t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)

	// Check that the storage lazily creates the directories on first write.
	_, err := eng.Stat(scratch.snapDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}

	f, err := scratch.NewFile(ctx, 0)
	require.NoError(t, err)

	// Check that even though the files aren't created, they are still recorded in SSTs().
	require.Equal(t, len(scratch.SSTs()), 1)

	// Check that the storage lazily creates the files on write.
	for _, fileName := range scratch.SSTs() {
		_, err := eng.Stat(fileName)
		if !oserror.IsNotExist(err) {
			t.Fatalf("expected %s to not exist", fileName)
		}
	}

	require.NoError(t, f.Write([]byte("foo")))

	// After writing to files, check that they have been flushed to disk.
	for _, fileName := range scratch.SSTs() {
		f, err := eng.Open(fileName)
		require.NoError(t, err)
		data, err := io.ReadAll(f)
		require.NoError(t, err)
		require.Equal(t, data, []byte("foo"))
		require.NoError(t, f.Close())
	}

	// Check that closing is idempotent.
	require.NoError(t, f.Finish())

	// Check that writing to a closed file is an error.
	err = f.Write([]byte("foo"))
	require.EqualError(t, err, "file has already been closed")

	// Check that closing an empty file is an error.
	f, err = scratch.NewFile(ctx, 0)
	require.NoError(t, err)
	require.EqualError(t, f.Finish(), "file is empty")

	// Check that Close removes the snapshot directory as well as the range
	// directory.
	require.NoError(t, scratch.Close())
	_, err = eng.Stat(scratch.snapDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}
	rangeDir := filepath.Join(sstSnapshotStorage.dir, strconv.Itoa(int(scratch.rangeID)))
	_, err = eng.Stat(rangeDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", rangeDir)
	}
	require.NoError(t, sstSnapshotStorage.Clear())
	_, err = eng.Stat(sstSnapshotStorage.dir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", sstSnapshotStorage.dir)
	}
}

func TestSSTSnapshotStorageConcurrentRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testSnapUUID2 := uuid.Must(uuid.FromBytes([]byte("foobar2345678910")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newOnDiskEngine(ctx, t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)

	runForSnap := func(snapUUID uuid.UUID) error {
		scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, snapUUID)

		// Check that the storage lazily creates the directories on first write.
		_, err := eng.Stat(scratch.snapDir)
		if !oserror.IsNotExist(err) {
			return errors.Errorf("expected %s to not exist", scratch.snapDir)
		}

		f, err := scratch.NewFile(ctx, 0)
		require.NoError(t, err)

		// Check that even though the files aren't created, they are still recorded in SSTs().
		require.Equal(t, len(scratch.SSTs()), 1)

		// Check that the storage lazily creates the files on write.
		for _, fileName := range scratch.SSTs() {
			_, err := eng.Stat(fileName)
			if !oserror.IsNotExist(err) {
				return errors.Errorf("expected %s to not exist", fileName)
			}
		}

		require.NoError(t, f.Write([]byte("foo")))

		// After writing to files, check that they have been flushed to disk.
		for _, fileName := range scratch.SSTs() {
			f, err := eng.Open(fileName)
			require.NoError(t, err)
			data, err := io.ReadAll(f)
			require.NoError(t, err)
			require.Equal(t, data, []byte("foo"))
			require.NoError(t, f.Close())
		}

		// Check that closing is idempotent.
		require.NoError(t, f.Finish())

		// Check that writing to a closed file is an error.
		err = f.Write([]byte("foo"))
		require.EqualError(t, err, "file has already been closed")

		// Check that closing an empty file is an error.
		f, err = scratch.NewFile(ctx, 0)
		require.NoError(t, err)
		require.EqualError(t, f.Finish(), "file is empty")

		// Check that Close removes the snapshot directory.
		require.NoError(t, scratch.Close())
		_, err = eng.Stat(scratch.snapDir)
		if !oserror.IsNotExist(err) {
			return errors.Errorf("expected %s to not exist", scratch.snapDir)
		}
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errChan := make(chan error)
	for _, snapID := range []uuid.UUID{testSnapUUID, testSnapUUID2} {
		snapID := snapID
		go func() {
			defer wg.Done()
			if err := runForSnap(snapID); err != nil {
				errChan <- err
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errChan:
		t.Fatal(err)
	default:
	}
	// Ensure that the range directory was deleted after the scratches were
	// closed.
	rangeDir := filepath.Join(sstSnapshotStorage.dir, strconv.Itoa(int(testRangeID)))
	_, err := eng.Stat(rangeDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", rangeDir)
	}
	require.NoError(t, sstSnapshotStorage.Clear())
	_, err = eng.Stat(sstSnapshotStorage.dir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", sstSnapshotStorage.dir)
	}
}

// TestSSTSnapshotStorageContextCancellation verifies that writing to an
// SSTSnapshotStorage is reactive to context cancellation.
func TestSSTSnapshotStorageContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	ctx := context.Background()
	cleanup, eng := newOnDiskEngine(ctx, t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	f, err := scratch.NewFile(ctx, 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Finish())
	}()

	// Before context cancellation.
	require.NoError(t, f.Write([]byte("foo")))

	// After context cancellation.
	cancel()
	err = f.Write([]byte("bar"))
	require.ErrorIs(t, err, context.Canceled)
}

// TestMultiSSTWriterInitSST tests that multiSSTWriter initializes each of the
// SST files associated with the replicated key ranges by writing a range
// deletion tombstone that spans the entire range of each respectively.
func TestMultiSSTWriterInitSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newOnDiskEngine(ctx, t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	keySpans := rditer.MakeReplicatedKeySpans(&desc)

	msstw, err := newMultiSSTWriter(
		ctx, cluster.MakeTestingClusterSettings(), scratch, keySpans, 0,
	)
	require.NoError(t, err)
	_, err = msstw.Finish(ctx)
	require.NoError(t, err)

	var actualSSTs [][]byte
	fileNames := msstw.scratch.SSTs()
	for _, file := range fileNames {
		sst, err := fs.ReadFile(eng, file)
		require.NoError(t, err)
		actualSSTs = append(actualSSTs, sst)
	}

	// Construct an SST file for each of the key ranges and write a rangedel
	// tombstone that spans from Start to End.
	var expectedSSTs [][]byte
	for _, s := range keySpans {
		func() {
			sstFile := &storage.MemObject{}
			sst := storage.MakeIngestionSSTWriter(ctx, cluster.MakeTestingClusterSettings(), sstFile)
			defer sst.Close()
			err := sst.ClearRawRange(s.Key, s.EndKey, true, true)
			require.NoError(t, err)
			err = sst.Finish()
			require.NoError(t, err)
			expectedSSTs = append(expectedSSTs, sstFile.Data())
		}()
	}

	require.Equal(t, len(actualSSTs), len(expectedSSTs))
	for i := range fileNames {
		require.Equal(t, actualSSTs[i], expectedSSTs[i])
	}
}

func newOnDiskEngine(ctx context.Context, t *testing.T) (func(), storage.Engine) {
	dir, cleanup := testutils.TempDir(t)
	eng, err := storage.Open(
		ctx,
		storage.Filesystem(dir),
		cluster.MakeClusterSettings(),
		storage.CacheSize(1<<20 /* 1 MiB */))
	if err != nil {
		t.Fatal(err)
	}
	return cleanup, eng
}
