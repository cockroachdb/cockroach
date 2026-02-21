// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package snaprecv

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/mvccencoding"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
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
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID, nil)

	// Check that the storage lazily creates the directories on first write.
	_, err := eng.Env().Stat(scratch.snapDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}

	f, err := scratch.NewFile(ctx, 0)
	require.NoError(t, err)

	// Check that even though the files aren't created, they are still recorded in SSTs().
	require.Equal(t, len(scratch.SSTs()), 1)

	// Check that the storage lazily creates the files on write.
	for _, sst := range scratch.SSTs() {
		_, err := eng.Env().Stat(sst.Path)
		if !oserror.IsNotExist(err) {
			t.Fatalf("expected %s to not exist", sst.Path)
		}
	}

	require.NoError(t, f.Write([]byte("foo")))

	// After writing to files, check that they have been flushed to disk.
	for _, sst := range scratch.SSTs() {
		f, err := eng.Env().Open(sst.Path)
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
	_, err = eng.Env().Stat(scratch.snapDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}
	rangeDir := filepath.Join(sstSnapshotStorage.dir, strconv.Itoa(int(scratch.rangeID)))
	_, err = eng.Env().Stat(rangeDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", rangeDir)
	}
	require.NoError(t, sstSnapshotStorage.Clear())
	_, err = eng.Env().Stat(sstSnapshotStorage.dir)
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
		scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, snapUUID, nil)

		// Check that the storage lazily creates the directories on first write.
		_, err := eng.Env().Stat(scratch.snapDir)
		if !oserror.IsNotExist(err) {
			return errors.Errorf("expected %s to not exist", scratch.snapDir)
		}

		f, err := scratch.NewFile(ctx, 0)
		require.NoError(t, err)

		// Check that even though the files aren't created, they are still recorded in SSTs().
		require.Equal(t, len(scratch.SSTs()), 1)

		// Check that the storage lazily creates the files on write.
		for _, sst := range scratch.SSTs() {
			_, err := eng.Env().Stat(sst.Path)
			if !oserror.IsNotExist(err) {
				return errors.Errorf("expected %s to not exist", sst.Path)
			}
		}

		require.NoError(t, f.Write([]byte("foo")))

		// After writing to files, check that they have been flushed to disk.
		for _, sst := range scratch.SSTs() {
			f, err := eng.Env().Open(sst.Path)
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
		_, err = eng.Env().Stat(scratch.snapDir)
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
	_, err := eng.Env().Stat(rangeDir)
	if !oserror.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", rangeDir)
	}
	require.NoError(t, sstSnapshotStorage.Clear())
	_, err = eng.Env().Stat(sstSnapshotStorage.dir)
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
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID, nil)

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

// TestMultiSSTWriterInitSST tests the SSTS that MultiSSTWriter generates.
// In particular, certain SST files must contain range key deletes as well
// as range deletes to make sure that ingesting the SST clears any existing
// data.
func TestMultiSSTWriterInitSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "interesting", func(t *testing.T, interesting bool) {
		testutils.RunTrueAndFalse(t, "blobEnabled", func(t *testing.T, blobEnabled bool) {
			testMultiSSTWriterInitSSTInner(t, interesting, blobEnabled)
		})
	})
}

func testMultiSSTWriterInitSSTInner(t *testing.T, interesting bool, blobEnabled bool) {
	ctx := context.Background()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newOnDiskEngine(ctx, t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID, nil)
	desc := roachpb.RangeDescriptor{
		RangeID:  100,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	keySpans := rditer.MakeReplicatedKeySpans(&desc)
	localSpans := keySpans[:len(keySpans)-1]
	mvccSpan := keySpans[len(keySpans)-1]

	st := cluster.MakeTestingClusterSettings()

	// The tests rely on specific SST sizes; we cannot use MinLZ as the
	// compression can depend on the architecture.
	storage.CompressionAlgorithmStorage.Override(context.Background(), &st.SV, storage.StoreCompressionSnappy)

	// Enable value separation for snapshot SSTs if requested. We must override
	// all the relevant settings since they may be set by metamorphic testing.
	if blobEnabled {
		storage.ValueSeparationEnabled.Override(ctx, &st.SV, true)
		storage.ValueSeparationSnapshotSSTEnabled.Override(ctx, &st.SV, true)
		storage.ValueSeparationMinimumSize.Override(ctx, &st.SV, 256)
	} else {
		storage.ValueSeparationSnapshotSSTEnabled.Override(ctx, &st.SV, false)
	}

	msstw, err := NewMultiSSTWriter(ctx, st, scratch, localSpans, mvccSpan, MultiSSTWriterOptions{})
	require.NoError(t, err)

	var buf redact.StringBuilder
	logSize := func(buf io.Writer) {
		// sstSize increases on SST flush and is the total size of all finished
		// SSTs, estDataSize is the exact data size (think: sum of lengths of keys
		// and values) of all writes to all SSTs, including the pending one.
		_, _ = fmt.Fprintf(buf, ">> sstSize=%d estDataSize=%d\n", msstw.sstSize, msstw.EstimatedDataSize())
	}

	var putSpans []roachpb.Span
	if interesting {
		// Put rangedel and range keys into the spans. These cases can occur in
		// shared SST snapshots and if we don't fragment these additional ranged
		// inputs properly against the one the msstw lays down, pebble will error out
		// (since the resulting SST would be invalid).
		//
		// In practice such operations would likely only occur on the MVCC span, so
		// some of what is tested here is slightly academical. Still, we don't want to
		// make assumptions on what data we're required to handle.
		//
		// NB: we have related (basic) coverage for MVCC range deletions through
		// TestRaftSnapshotsWithMVCCRangeKeysEverywhere.

		putSpans = []roachpb.Span{
			{ // rangeID local span
				Key:    keys.AbortSpanKey(desc.RangeID, uuid.Nil),
				EndKey: keys.AbortSpanKey(desc.RangeID, uuid.NamespaceDNS),
			},
			{ // addressable local span
				Key:    keys.RangeDescriptorKey(desc.StartKey),
				EndKey: keys.RangeDescriptorKey(roachpb.RKey("f")),
			},
			// MVCC span
			{
				Key:    roachpb.Key("e"),
				EndKey: roachpb.Key("f"),
			},
		}
	}
	for _, span := range putSpans {
		// NB: we avoid covering the entire span because an SST can actually contain
		// the same rangedel twice (as long as they're added in increasing seqno
		// order), and we want to exercise the "tricky" case where pebble would
		// complain about lack of proper fragmentation.
		//
		// NB: sometimes we have dataSize=estDataSize following a ranged write
		// since the write gets hidden in fragmenters and only becomes visible
		// in stats when the SST is flushed.
		k := storage.EngineKey{Key: span.Key}.Encode()
		ek := storage.EngineKey{Key: span.EndKey}.Encode()
		require.NoError(t, msstw.putInternalRangeDelete(ctx, k, ek))
		rk := rangekey.Key{
			Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeKeySet),
		}
		require.NoError(t, msstw.putInternalRangeKey(ctx, k, ek, rk))
		_, _ = fmt.Fprintf(&buf, ">> rangekeyset [%s-%s)\n", span.Key, span.EndKey)
		logSize(&buf)
	}

	_, _ = fmt.Fprintln(&buf, ">> finishing msstw")
	_, _, err = msstw.Finish(ctx)
	require.NoError(t, err)

	logSize(&buf)

	var actualSSTs [][]byte
	localSSTs := msstw.scratch.SSTs()
	for _, sst := range localSSTs {
		sstData, err := fs.ReadFile(eng.Env(), sst.Path)
		require.NoError(t, err)
		actualSSTs = append(actualSSTs, sstData)
	}

	for i := range localSSTs {
		name := fmt.Sprintf("sst%d", i)
		require.NoError(t, storageutils.ReportSSTEntries(&buf, name, actualSSTs[i]))
	}

	// The echotest golden files are generated without blob files. When blob
	// files are enabled, we skip the echotest since we're testing the same
	// logical functionality, just with a different code path. The test values
	// are too small to trigger value separation anyway.
	if !blobEnabled {
		// Use the original golden file name (without the blobEnabled suffix).
		filename := strings.Replace(t.Name(), "/", "_", -1)
		filename = strings.Replace(filename, "_blobEnabled=false", "", 1)
		echotest.Require(t, buf.String(), filepath.Join(datapathutils.TestDataPath(t, "echotest", filename)))
	}

	// Verify that no blob files were created since the values are too small
	// to trigger value separation.
	require.False(t, scratch.HasBlobFiles())
}

func buildIterForScratch(
	t *testing.T, keySpans []roachpb.Span, scratch *SSTSnapshotStorageScratch,
) (storage.MVCCIterator, error) {
	var openFiles []objstorage.ReadableFile
	for _, sst := range scratch.SSTs()[len(keySpans)-1:] {
		f, err := vfs.Default.Open(sst.Path)
		require.NoError(t, err)
		openFiles = append(openFiles, f)
	}
	mvccSpan := keySpans[len(keySpans)-1]

	return storage.NewSSTIterator([][]objstorage.ReadableFile{openFiles}, storage.IterOptions{
		LowerBound: mvccSpan.Key,
		UpperBound: mvccSpan.EndKey,
	})
}

// TestMultiSSTWriterSize tests the effect of lowering the max size
// of sstables in a MultiSSTWriter, and ensuring that the produced sstables
// are still correct.
func TestMultiSSTWriterSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "blobEnabled", testMultiSSTWriterSizeInner)
}

func testMultiSSTWriterSizeInner(t *testing.T, blobEnabled bool) {
	ctx := context.Background()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newOnDiskEngine(ctx, t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	ref := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID, nil)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID, nil)
	settings := cluster.MakeTestingClusterSettings()

	// Enable value separation for snapshot SSTs if requested.
	if blobEnabled {
		storage.ValueSeparationEnabled.Override(ctx, &settings.SV, true)
		storage.ValueSeparationSnapshotSSTEnabled.Override(ctx, &settings.SV, true)
		// Set minimum size low enough that our test values will be separated.
		storage.ValueSeparationMinimumSize.Override(ctx, &settings.SV, 256)
	} else {
		storage.ValueSeparationSnapshotSSTEnabled.Override(ctx, &settings.SV, false)
	}

	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	keySpans := rditer.MakeReplicatedKeySpans(&desc)
	localSpans := keySpans[:len(keySpans)-1]
	mvccSpan := keySpans[len(keySpans)-1]

	// When blob files are enabled, use values large enough to trigger value
	// separation (> 256 bytes). Otherwise use small values.
	var testValue []byte
	if blobEnabled {
		testValue = make([]byte, 512) // large enough to be separated
		for i := range testValue {
			testValue[i] = byte(i % 256)
		}
	} else {
		testValue = []byte("foobarbaz")
	}

	// Make a reference msstw with the default size.
	referenceMsstw, err := NewMultiSSTWriter(ctx, settings, ref, localSpans, mvccSpan, MultiSSTWriterOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(0), referenceMsstw.dataSize)
	now := timeutil.Now().UnixNano()

	for i := range localSpans {
		require.NoError(t, referenceMsstw.put(ctx, storage.EngineKey{Key: localSpans[i].Key}, []byte("foo")))
	}

	for i := 0; i < 1000; i++ {
		key := binary.BigEndian.AppendUint32(append([]byte(nil), desc.StartKey...), uint32(i))
		mvccKey := storage.MVCCKey{Key: roachpb.Key(key), Timestamp: hlc.Timestamp{WallTime: now}}
		engineKey, ok := storage.DecodeEngineKey(storage.EncodeMVCCKey(mvccKey))
		require.True(t, ok)

		if i%50 == 0 {
			// Add a range key.
			endKey := binary.BigEndian.AppendUint32(desc.StartKey, uint32(i+10))
			require.NoError(t, referenceMsstw.putRangeKey(
				ctx, key, endKey, mvccencoding.EncodeMVCCTimestampSuffix(mvccKey.Timestamp.WallPrev()), []byte("")))
		}
		require.NoError(t, referenceMsstw.put(ctx, engineKey, testValue))
	}
	_, _, err = referenceMsstw.Finish(ctx)
	require.NoError(t, err)

	// Verify blob files are created when enabled and values are large enough.
	if blobEnabled {
		require.True(t, ref.HasBlobFiles(), "expected blob files to be created with large values")
		// Skip the rest of the test since Pebble's external iterator doesn't
		// support SSTs with blob references. We verified blob files were created.
		return
	}

	refIter, err := buildIterForScratch(t, keySpans, ref)
	require.NoError(t, err)
	defer refIter.Close()

	multiSSTWriter, err := NewMultiSSTWriter(ctx, settings, scratch, localSpans, mvccSpan, MultiSSTWriterOptions{
		MaxSSTSize: 100,
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), multiSSTWriter.dataSize)

	for i := range localSpans {
		require.NoError(t, multiSSTWriter.put(ctx, storage.EngineKey{Key: localSpans[i].Key}, []byte("foo")))
	}

	for i := 0; i < 1000; i++ {
		key := binary.BigEndian.AppendUint32(append([]byte(nil), desc.StartKey...), uint32(i))
		mvccKey := storage.MVCCKey{Key: roachpb.Key(key), Timestamp: hlc.Timestamp{WallTime: now}}
		engineKey, ok := storage.DecodeEngineKey(storage.EncodeMVCCKey(mvccKey))
		require.True(t, ok)
		if i%50 == 0 {
			// Add a range key.
			endKey := binary.BigEndian.AppendUint32(desc.StartKey, uint32(i+10))
			require.NoError(t, multiSSTWriter.putRangeKey(
				ctx, key, endKey, mvccencoding.EncodeMVCCTimestampSuffix(mvccKey.Timestamp.WallPrev()), []byte("")))
		}
		require.NoError(t, multiSSTWriter.put(ctx, engineKey, testValue))
	}

	_, _, err = multiSSTWriter.Finish(ctx)
	require.NoError(t, err)
	require.Greater(t, len(scratch.SSTs()), len(ref.SSTs()))
	require.False(t, scratch.HasBlobFiles())

	iter, err := buildIterForScratch(t, keySpans, scratch)
	require.NoError(t, err)
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: mvccSpan.Key})
	refIter.SeekGE(storage.MVCCKey{Key: mvccSpan.Key})
	valid, err := iter.Valid()
	valid2, err2 := refIter.Valid()
	require.NoError(t, err)
	require.NoError(t, err2)

	for valid && valid2 {

		require.Equal(t, iter.UnsafeKey(), refIter.UnsafeKey())
		val, err := iter.UnsafeValue()
		require.NoError(t, err)
		val2, err2 := refIter.UnsafeValue()
		require.NoError(t, err2)
		require.Equal(t, val, val2)
		iter.Next()
		refIter.Next()
		valid, err = iter.Valid()
		valid2, err2 = refIter.Valid()
		require.NoError(t, err)
		require.NoError(t, err2)
	}
	require.Equal(t, valid, valid2)
}

// TestMultiSSTWriterAddLastSpan tests that MultiSSTWriter initializes each of
// the SST files associated with the replicated key ranges by writing a range
// deletion tombstone that spans the entire range of each respectively, except
// for the last span which only gets a rangedel when explicitly added.
func TestMultiSSTWriterAddLastSpan(t *testing.T) {
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
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID, nil)
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	keySpans := rditer.MakeReplicatedKeySpans(&desc)
	localSpans := keySpans[:len(keySpans)-1]
	mvccSpan := keySpans[len(keySpans)-1]

	msstw, err := NewMultiSSTWriter(ctx, cluster.MakeTestingClusterSettings(), scratch, localSpans, mvccSpan, MultiSSTWriterOptions{})
	require.NoError(t, err)
	testKey := storage.MVCCKey{Key: roachpb.RKey("d1").AsRawKey(), Timestamp: hlc.Timestamp{WallTime: 1}}
	testEngineKey, _ := storage.DecodeEngineKey(storage.EncodeMVCCKey(testKey))
	require.NoError(t, msstw.put(ctx, testEngineKey, []byte("foo")))
	_, _, err = msstw.Finish(ctx)
	require.NoError(t, err)

	var actualSSTs [][]byte
	localSSTs := msstw.scratch.SSTs()
	for _, sst := range localSSTs {
		sstData, err := fs.ReadFile(eng.Env(), sst.Path)
		require.NoError(t, err)
		actualSSTs = append(actualSSTs, sstData)
	}

	// Construct an SST file for each of the key ranges and write a rangedel
	// tombstone that spans from Start to End.
	var expectedSSTs [][]byte
	for i, s := range keySpans {
		func() {
			sstFile := &storage.MemObject{}
			sst := storage.MakeIngestionSSTWriter(ctx, cluster.MakeTestingClusterSettings(), sstFile)
			defer sst.Close()
			if i < len(keySpans)-1 {
				err := sst.ClearRawRange(s.Key, s.EndKey, true, true)
				require.NoError(t, err)
			}
			if i == len(keySpans)-1 {
				require.NoError(t, sst.PutEngineKey(testEngineKey, []byte("foo")))
			}
			err = sst.Finish()
			require.NoError(t, err)
			expectedSSTs = append(expectedSSTs, sstFile.Data())
		}()
	}

	require.Equal(t, len(actualSSTs), len(expectedSSTs))
	for i := range localSSTs {
		require.Equal(t, actualSSTs[i], expectedSSTs[i])
	}
}

func newOnDiskEngine(ctx context.Context, t *testing.T) (func(), storage.Engine) {
	dir, cleanup := testutils.TempDir(t)
	eng, err := storage.Open(
		ctx,
		fs.MustInitPhysicalTestingEnv(dir),
		cluster.MakeTestingClusterSettings(),
		storage.CacheSize(1<<20 /* 1 MiB */))
	if err != nil {
		t.Fatal(err)
	}
	return cleanup, eng
}

// TestMultiSSTWriterReadOneSharedOrExternal tests the ReadOne method with
// sharedOrExternal=true, which allows internal key kinds like DEL, RangeDEL,
// RangeKeyUnset, and RangeKeyDelete. This is required for shared/external SST
// snapshots where such internal keys can be overlaid over points in those
// SSTs.
func TestMultiSSTWriterReadOneSharedOrExternal(t *testing.T) {
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
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID, nil)
	desc := roachpb.RangeDescriptor{
		RangeID:  100,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	keySpans := rditer.MakeReplicatedKeySpans(&desc)
	localSpans := keySpans[:len(keySpans)-1]
	mvccSpan := keySpans[len(keySpans)-1]

	st := cluster.MakeTestingClusterSettings()

	msstw, err := NewMultiSSTWriter(ctx, st, scratch, localSpans, mvccSpan, MultiSSTWriterOptions{})
	require.NoError(t, err)

	now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	// Helper to create a batch with a single entry and return the BatchReader.
	// Uses the engine to create a WriteBatch and get its Repr().
	makeBatchReader := func(fn func(storage.WriteBatch)) *storage.BatchReader {
		batch := eng.NewWriteBatch()
		defer batch.Close()
		fn(batch)
		repr := batch.Repr()
		// Make a copy since the batch repr may be invalidated when the batch is closed.
		reprCopy := make([]byte, len(repr))
		copy(reprCopy, repr)
		br, err := storage.NewBatchReader(reprCopy)
		require.NoError(t, err)
		return br
	}

	// Test 1: Normal point SET should work with sharedOrExternal=false.
	t.Run("point-set", func(t *testing.T) {
		mvccKey := storage.MVCCKey{Key: roachpb.Key("e"), Timestamp: now}
		encodedKey := storage.EncodeMVCCKey(mvccKey)
		br := makeBatchReader(func(b storage.WriteBatch) {
			ik := pebble.InternalKey{
				UserKey: encodedKey,
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindSet),
			}
			require.NoError(t, b.PutInternalPointKey(&ik, []byte("value")))
		})
		require.True(t, br.Next())
		ek, err := br.EngineKey()
		require.NoError(t, err)
		require.NoError(t, msstw.ReadOne(ctx, ek, false /* sharedOrExternal */, br))
	})

	// Test 2: Point DELETE should fail with sharedOrExternal=false.
	t.Run("point-delete-fails-without-shared", func(t *testing.T) {
		mvccKey := storage.MVCCKey{Key: roachpb.Key("f"), Timestamp: now}
		encodedKey := storage.EncodeMVCCKey(mvccKey)
		br := makeBatchReader(func(b storage.WriteBatch) {
			ik := pebble.InternalKey{
				UserKey: encodedKey,
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindDelete),
			}
			require.NoError(t, b.PutInternalPointKey(&ik, nil))
		})
		require.True(t, br.Next())
		ek, err := br.EngineKey()
		require.NoError(t, err)
		err = msstw.ReadOne(ctx, ek, false /* sharedOrExternal */, br)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected batch entry key kind")
	})

	// Test 3: Point DELETE should succeed with sharedOrExternal=true.
	t.Run("point-delete-succeeds-with-shared", func(t *testing.T) {
		mvccKey := storage.MVCCKey{Key: roachpb.Key("g"), Timestamp: now}
		encodedKey := storage.EncodeMVCCKey(mvccKey)
		br := makeBatchReader(func(b storage.WriteBatch) {
			ik := pebble.InternalKey{
				UserKey: encodedKey,
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindDelete),
			}
			require.NoError(t, b.PutInternalPointKey(&ik, nil))
		})
		require.True(t, br.Next())
		ek, err := br.EngineKey()
		require.NoError(t, err)
		require.NoError(t, msstw.ReadOne(ctx, ek, true /* sharedOrExternal */, br))
	})

	// Test 4: RangeDelete should fail with sharedOrExternal=false.
	t.Run("range-delete-fails-without-shared", func(t *testing.T) {
		startKey := storage.EncodeMVCCKey(storage.MVCCKey{Key: roachpb.Key("h")})
		endKey := storage.EncodeMVCCKey(storage.MVCCKey{Key: roachpb.Key("i")})
		br := makeBatchReader(func(b storage.WriteBatch) {
			require.NoError(t, b.ClearRawEncodedRange(startKey, endKey))
		})
		require.True(t, br.Next())
		ek, err := br.EngineKey()
		require.NoError(t, err)
		err = msstw.ReadOne(ctx, ek, false /* sharedOrExternal */, br)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected batch entry key kind")
	})

	// Test 5: RangeDelete should succeed with sharedOrExternal=true.
	t.Run("range-delete-succeeds-with-shared", func(t *testing.T) {
		startKey := storage.EncodeMVCCKey(storage.MVCCKey{Key: roachpb.Key("j")})
		endKey := storage.EncodeMVCCKey(storage.MVCCKey{Key: roachpb.Key("k")})
		br := makeBatchReader(func(b storage.WriteBatch) {
			require.NoError(t, b.ClearRawEncodedRange(startKey, endKey))
		})
		require.True(t, br.Next())
		ek, err := br.EngineKey()
		require.NoError(t, err)
		require.NoError(t, msstw.ReadOne(ctx, ek, true /* sharedOrExternal */, br))
	})

	// Finish the writer to ensure the SSTs are valid.
	_, _, err = msstw.Finish(ctx)
	require.NoError(t, err)
}
