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
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	cleanup, eng := newOnDiskEngine(t)
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
	defer func() {
		require.NoError(t, f.Close())
	}()

	// Check that even though the files aren't created, they are still recorded in SSTs().
	require.Equal(t, len(scratch.SSTs()), 1)

	// Check that the storage lazily creates the files on write.
	for _, fileName := range scratch.SSTs() {
		_, err := eng.Stat(fileName)
		if !oserror.IsNotExist(err) {
			t.Fatalf("expected %s to not exist", fileName)
		}
	}

	_, err = f.Write([]byte("foo"))
	require.NoError(t, err)

	// After writing to files, check that they have been flushed to disk.
	for _, fileName := range scratch.SSTs() {
		f, err := eng.Open(fileName)
		require.NoError(t, err)
		data, err := ioutil.ReadAll(f)
		require.NoError(t, err)
		require.Equal(t, data, []byte("foo"))
		require.NoError(t, f.Close())
	}

	// Check that closing is idempotent.
	require.NoError(t, f.Close())
	require.NoError(t, f.Close())

	// Check that writing to a closed file is an error.
	_, err = f.Write([]byte("foo"))
	require.EqualError(t, err, "file has already been closed")

	// Check that closing an empty file is an error.
	f, err = scratch.NewFile(ctx, 0)
	require.NoError(t, err)
	require.EqualError(t, f.Close(), "file is empty")
	_, err = f.Write([]byte("foo"))
	require.NoError(t, err)

	// Check that Clear removes the directory.
	require.NoError(t, scratch.Clear())
	_, err = eng.Stat(scratch.snapDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}
	require.NoError(t, sstSnapshotStorage.Clear())
	_, err = eng.Stat(sstSnapshotStorage.dir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", sstSnapshotStorage.dir)
	}
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

	cleanup, eng := newOnDiskEngine(t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	keyRanges := rditer.MakeReplicatedKeyRanges(&desc)

	msstw, err := newMultiSSTWriter(ctx, scratch, keyRanges, 0)
	require.NoError(t, err)
	err = msstw.Finish(ctx)
	require.NoError(t, err)

	var actualSSTs [][]byte
	fileNames := msstw.scratch.SSTs()
	for _, file := range fileNames {
		sst, err := eng.ReadFile(file)
		require.NoError(t, err)
		actualSSTs = append(actualSSTs, sst)
	}

	// Construct an SST file for each of the key ranges and write a rangedel
	// tombstone that spans from Start to End.
	var expectedSSTs [][]byte
	for _, r := range keyRanges {
		func() {
			sstFile := &storage.MemFile{}
			sst := storage.MakeIngestionSSTWriter(sstFile)
			defer sst.Close()
			err := sst.ClearRawRange(r.Start.Key, r.End.Key)
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
