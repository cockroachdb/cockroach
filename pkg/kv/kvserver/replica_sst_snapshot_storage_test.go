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
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestSSTSnapshotStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newEngine(t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)

	// Check that the storage lazily creates the directories on first write.
	_, err := os.Stat(scratch.snapDir)
	if !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}

	f, err := scratch.NewFile(ctx, 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Close())
	}()

	// The files are lazily created and also lazily added to the SSTs() slice.
	require.Equal(t, len(scratch.SSTs()), 0)

	// Check that the storage lazily creates the files on write.
	for _, fileName := range scratch.SSTs() {
		_, err := os.Stat(fileName)
		if !os.IsNotExist(err) {
			t.Fatalf("expected %s to not exist", fileName)
		}
	}

	_, err = f.Write([]byte("foo"))
	require.NoError(t, err)
	// After the SST file has been written to, it should be recorded in SSTs().
	require.Equal(t, len(scratch.SSTs()), 1)

	// After writing to files, check that they have been flushed to disk.
	for _, fileName := range scratch.SSTs() {
		require.FileExists(t, fileName)
		data, err := ioutil.ReadFile(fileName)
		require.NoError(t, err)
		require.Equal(t, data, []byte("foo"))
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
	_, err = os.Stat(scratch.snapDir)
	if !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}
	require.NoError(t, sstSnapshotStorage.Clear())
	_, err = os.Stat(sstSnapshotStorage.dir)
	if !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", sstSnapshotStorage.dir)
	}
}

// TestMultiSSTWriterInitSST tests that multiSSTWriter lazily creates each of
// the SST files associated with the three replicated key ranges and if the
// ranges are non-empty, writes a range deletion tombstone to each file that
// spans only the keys in the range.
func TestMultiSSTWriterInitSST(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newEngine(t)
	defer cleanup()
	defer eng.Close()

	testutils.RunTrueAndFalse(t, "insertKeys", func(t *testing.T, insertKeys bool) {
		desc := roachpb.RangeDescriptor{
			StartKey: roachpb.RKey("d"),
			EndKey:   roachpb.RKeyMax,
		}
		keyRanges := rditer.MakeReplicatedKeyRanges(&desc)
		ts := hlc.Timestamp{}
		sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
		var scratch *SSTSnapshotStorageScratch

		if insertKeys {
			// Expect a SST file to be created for a range only if that range contains
			// at least one key. So we insert a key that falls within each of the
			// three types of replicated key ranges and check to see if the number of
			// SST files created matches what we expect.
			insertedKeys := make([]roachpb.Key, len(keyRanges))
			for i, kr := range keyRanges {
				insertedKeys[i] = kr.Start.Key
			}
			for numberOfPuts := 1; numberOfPuts < len(keyRanges)+1; numberOfPuts++ {
				err := storage.MVCCPut(ctx, eng, nil, insertedKeys[numberOfPuts-1], ts, roachpb.MakeValueFromString("value"), nil)
				require.NoError(t, err)
				scratch = sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)
				msstw, err := newMultiSSTWriter(ctx, scratch, keyRanges, 0, eng)
				require.NoError(t, err)
				err = msstw.Finish(ctx)
				require.NoError(t, err)
				require.Equal(t, numberOfPuts, len(msstw.scratch.SSTs()))
			}

			var actualSSTs [][]byte
			fileNames := scratch.SSTs()
			for _, file := range fileNames {
				sst, err := eng.ReadFile(file)
				require.NoError(t, err)
				actualSSTs = append(actualSSTs, sst)
			}
			// Construct an SST file for each of the key ranges and write a rangedel
			// tombstone that spans [insertedKey, insertedKey.Next()
			var expectedSSTs [][]byte
			for i := range keyRanges {
				func() {
					sstFile := &storage.MemFile{}
					sst := storage.MakeIngestionSSTWriter(sstFile)
					defer sst.Close()
					err := sst.ClearRange(storage.MakeMVCCMetadataKey(insertedKeys[i]), storage.MakeMVCCMetadataKey(insertedKeys[i].Next()))
					require.NoError(t, err)
					err = sst.Finish()
					require.NoError(t, err)
					expectedSSTs = append(expectedSSTs, sstFile.Data())
				}()
			}

			for i := range fileNames {
				require.Equal(t, actualSSTs[i], expectedSSTs[i])
			}
		} else {
			// Without any inserted keys, we expect no SSTs to be created.
			scratch = sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)
			msstw, err := newMultiSSTWriter(ctx, scratch, keyRanges, 0, eng)
			require.NoError(t, err)
			err = msstw.Finish(ctx)
			require.NoError(t, err)
			require.Equal(t, 0, len(msstw.scratch.SSTs()))
		}
	})
}
