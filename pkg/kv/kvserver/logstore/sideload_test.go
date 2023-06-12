// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstore

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/time/rate"
)

func mustEntryEq(t testing.TB, l, r raftpb.Entry) {
	el, err := raftlog.NewEntry(l)
	require.NoError(t, err)
	er, err := raftlog.NewEntry(r)
	require.NoError(t, err)
	require.Equal(t, el, er)
}

func mkEnt(
	enc raftlog.EntryEncoding, index, term uint64, as *kvserverpb.ReplicatedEvalResult_AddSSTable,
) raftpb.Entry {
	cmdIDKey := strings.Repeat("x", raftlog.RaftCommandIDLen)
	var cmd kvserverpb.RaftCommand
	cmd.ReplicatedEvalResult.AddSSTable = as
	b, err := protoutil.Marshal(&cmd)
	if err != nil {
		panic(err)
	}
	var ent raftpb.Entry
	ent.Index, ent.Term = index, term
	ent.Data = raftlog.EncodeCommandBytes(enc, kvserverbase.CmdIDKey(cmdIDKey), b)
	return ent
}

func TestSideloadingSideloadedStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	t.Run("Mem", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()
		testSideloadingSideloadedStorage(t, eng)
	})
	t.Run("Disk", func(t *testing.T) {
		ctx := context.Background()
		cleanup, eng := newOnDiskEngine(ctx, t)
		defer cleanup()
		defer eng.Close()
		testSideloadingSideloadedStorage(t, eng)
	})
}

func newTestingSideloadStorage(eng storage.Engine) *DiskSideloadStorage {
	return NewDiskSideloadStorage(
		cluster.MakeTestingClusterSettings(), 1,
		filepath.Join(eng.GetAuxiliaryDir(), "fake", "testing", "dir"),
		rate.NewLimiter(rate.Inf, math.MaxInt64), eng)
}

func testSideloadingSideloadedStorage(t *testing.T, eng storage.Engine) {
	ctx := context.Background()
	ss := newTestingSideloadStorage(eng)

	assertCreated := func(isCreated bool) {
		t.Helper()
		if is := ss.dirCreated; is != isCreated {
			t.Fatalf("assertion failed: expected dirCreated=%t, got %t", isCreated, is)
		}
		_, err := ss.eng.Stat(ss.dir)
		if !ss.dirCreated {
			require.True(t, oserror.IsNotExist(err), "%v", err)
		} else {
			require.NoError(t, err)
		}
	}

	assertCreated(false)

	const (
		lowTerm = 1
		highTerm
	)

	file := func(index kvpb.RaftIndex, term kvpb.RaftTerm) []byte { // take uint64 for convenience
		return []byte("content-" + strconv.Itoa(int(index)*int(term)))
	}

	if err := ss.Put(ctx, 1, highTerm, file(1, 1)); err != nil {
		t.Fatal(err)
	}

	assertCreated(true)

	if c, err := ss.Get(ctx, 1, highTerm); err != nil {
		t.Fatal(err)
	} else if exp := file(1, 1); !bytes.Equal(c, exp) {
		t.Fatalf("got %q, wanted %q", c, exp)
	}

	// Overwrites the occupied slot.
	if err := ss.Put(ctx, 1, highTerm, file(12345, 1)); err != nil {
		t.Fatal(err)
	}

	// ... consequently the old entry is gone.
	if c, err := ss.Get(ctx, 1, highTerm); err != nil {
		t.Fatal(err)
	} else if exp := file(12345, 1); !bytes.Equal(c, exp) {
		t.Fatalf("got %q, wanted %q", c, exp)
	}

	if err := ss.Clear(ctx); err != nil {
		t.Fatal(err)
	}

	assertCreated(false)

	for n, test := range []struct {
		fun func() error
		err error
	}{
		{
			err: errSideloadedFileNotFound,
			fun: func() error {
				_, err := ss.Get(ctx, 123, 456)
				return err
			},
		},
		{
			err: errSideloadedFileNotFound,
			fun: func() error {
				_, err := ss.Purge(ctx, 123, 456)
				return err
			},
		},
		{
			err: nil,
			fun: func() error {
				_, _, err := ss.TruncateTo(ctx, 123)
				return err
			},
		},
		{
			err: nil,
			fun: func() error {
				_, err := ss.Filename(ctx, 123, 456)
				return err
			},
		},
	} {
		if err := test.fun(); !errors.Is(err, test.err) {
			t.Fatalf("%d: expected %v, got %v", n, test.err, err)
		}
		if err := ss.Clear(ctx); err != nil {
			t.Fatalf("%d: %+v", n, err)
		}
		assertCreated(false)
	}

	// Write some payloads at various indexes. Note that this tests Put
	// on a recently Clear()ed storage. Randomize order for fun.
	payloads := []kvpb.RaftIndex{3, 5, 7, 9, 10}
	for n := range rand.Perm(len(payloads)) {
		i := payloads[n]
		if err := ss.Put(ctx, i, highTerm, file(i, highTerm)); err != nil {
			t.Fatalf("%d: %+v", i, err)
		}
	}

	assertCreated(true)

	// Write some more payloads, overlapping, at the past term.
	pastPayloads := append([]kvpb.RaftIndex{81}, payloads...)
	for _, i := range pastPayloads {
		if err := ss.Put(ctx, i, lowTerm, file(i, lowTerm)); err != nil {
			t.Fatal(err)
		}
	}

	// Just a sanity check that for the overlapping terms, we see both entries.
	for _, term := range []kvpb.RaftTerm{lowTerm, highTerm} {
		index := payloads[0] // exists at both lowTerm and highTerm
		if c, err := ss.Get(ctx, index, term); err != nil {
			t.Fatal(err)
		} else if exp := file(index, term); !bytes.Equal(c, exp) {
			t.Fatalf("got %q, wanted %q", c, exp)
		}
	}
	assertCreated(true)

	for n := range payloads {
		freed, retained, err := ss.BytesIfTruncatedFromTo(ctx, 0, payloads[n])
		require.NoError(t, err)
		freedWhatWasRetained, retainedNothing, err :=
			ss.BytesIfTruncatedFromTo(ctx, payloads[n], math.MaxUint64)
		require.NoError(t, err)
		require.Zero(t, retainedNothing)
		require.Equal(t, freedWhatWasRetained, retained)
		// Truncate indexes < payloads[n] (payloads is sorted in increasing order).
		freedByTruncateTo, retainedByTruncateTo, err := ss.TruncateTo(ctx, payloads[n])
		if err != nil {
			t.Fatalf("%d: %+v", n, err)
		}
		require.Equal(t, freedByTruncateTo, freed)
		require.Equal(t, retainedByTruncateTo, retained)
		// Index payloads[n] and above are still there (truncation is exclusive)
		// at both terms.
		for _, term := range []kvpb.RaftTerm{lowTerm, highTerm} {
			for _, i := range payloads[n:] {
				if _, err := ss.Get(ctx, i, term); err != nil {
					t.Fatalf("%d.%d: %+v", n, i, err)
				}
			}
			// Indexes below are gone.
			for _, i := range payloads[:n] {
				if _, err := ss.Get(ctx, i, term); !errors.Is(err, errSideloadedFileNotFound) {
					t.Fatalf("%d.%d: %+v", n, i, err)
				}
			}
		}
	}

	func() {
		// First add a file that shouldn't be in the sideloaded storage to ensure
		// sane behavior when directory can't be removed after full truncate.
		nonRemovableFile := filepath.Join(ss.Dir(), "cantremove.xx")
		f, err := eng.Create(nonRemovableFile)
		if err != nil {
			t.Fatalf("could not create non i*.t* file in sideloaded storage: %+v", err)
		}
		// We have to close the file right away, otherwise (at least with in-mem pebble)
		// we will be prevented from removing it below.
		require.NoError(t, f.Close())

		_, _, err = ss.TruncateTo(ctx, math.MaxUint64)
		// The sideloaded storage should not error out here; removing files
		// is optional. But the file should still be there!
		require.NoError(t, err)
		{
			_, err := eng.Stat(nonRemovableFile)
			require.NoError(t, err)
		}

		// Now remove extra file and let truncation proceed to remove directory.
		require.NoError(t, eng.Remove(nonRemovableFile))

		// Test that directory is removed when filepath.Glob returns 0 matches.
		_, _, err = ss.TruncateTo(ctx, math.MaxUint64)
		require.NoError(t, err)
		// Ensure directory is removed, now that all files should be gone.
		_, err = eng.Stat(ss.Dir())
		require.True(t, oserror.IsNotExist(err), "%v", err)

		// Repopulate with some random indexes to test deletion when there are a
		// non-zero number of filepath.Glob matches.
		payloads := []kvpb.RaftIndex{3, 5, 7, 9, 10}
		for n := range rand.Perm(len(payloads)) {
			i := payloads[n]
			require.NoError(t, ss.Put(ctx, i, highTerm, file(i, highTerm)))
		}
		assertCreated(true)
		freed, retained, err := ss.BytesIfTruncatedFromTo(ctx, 0, math.MaxUint64)
		require.NoError(t, err)
		require.Zero(t, retained)
		freedByTruncateTo, retainedByTruncateTo, err := ss.TruncateTo(ctx, math.MaxUint64)
		require.NoError(t, err)
		require.Zero(t, retainedByTruncateTo)
		require.Equal(t, freedByTruncateTo, freed)
		// Ensure directory is removed when all records are removed.
		_, err = eng.Stat(ss.Dir())
		require.True(t, oserror.IsNotExist(err), "%v", err)
	}()

	require.NoError(t, ss.Clear(ctx))

	assertCreated(false)

	// Sanity check that we can call BytesIfTruncatedFromTo and TruncateTo
	// without the directory existing.
	freed, retained, err := ss.BytesIfTruncatedFromTo(ctx, 0, 1)
	require.NoError(t, err)
	require.Zero(t, freed)
	require.Zero(t, retained)
	freed, retained, err = ss.TruncateTo(ctx, 1)
	require.NoError(t, err)
	require.Zero(t, freed)
	require.Zero(t, retained)

	assertCreated(false)

	// Repopulate with a few entries at indexes=1,2,4 and term 10 to test `maybePurgeSideloaded`
	// with.
	for index := kvpb.RaftIndex(1); index < 5; index++ {
		if index == 3 {
			continue
		}
		payload := []byte(strings.Repeat("x", 1+int(index)))
		require.NoError(t, ss.Put(ctx, index, 10, payload))
	}

	// Term too high and too low, respectively. Shouldn't delete anything.
	for _, term := range []kvpb.RaftTerm{9, 11} {
		if size, err := maybePurgeSideloaded(ctx, ss, 1, 10, term); err != nil || size != 0 {
			t.Fatalf("expected noop for term %d, got (%d, %v)", term, size, err)
		}
	}
	// This should delete 2 and 4. Index == size+1, so expect 6.
	if size, err := maybePurgeSideloaded(ctx, ss, 2, 4, 10); err != nil || size != 8 {
		t.Fatalf("unexpectedly got (%d, %v)", size, err)
	}
	// This should delete 1 (the lone survivor).
	if size, err := maybePurgeSideloaded(ctx, ss, 0, 100, 10); err != nil || size != 2 {
		t.Fatalf("unexpectedly got (%d, %v)", size, err)
	}
	// Nothing left.
	if size, err := maybePurgeSideloaded(ctx, ss, 0, 100, 10); err != nil || size != 0 {
		t.Fatalf("expected noop, got (%d, %v)", size, err)
	}
}

func TestRaftSSTableSideloadingInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	v1, v2 := raftlog.EntryEncodingStandardWithAC, raftlog.EntryEncodingSideloadedWithAC
	rangeID := roachpb.RangeID(1)

	type testCase struct {
		// Entry passed into maybeInlineSideloadedRaftCommand and the entry
		// after having (perhaps) been modified.
		thin, fat raftpb.Entry
		// Populate the raft entry cache and sideload storage before running the test.
		setup func(*raftentry.Cache, SideloadStorage)
		// If nonempty, the error expected from maybeInlineSideloadedRaftCommand.
		expErr string
		// If nonempty, a regex that the recorded trace span must match.
		expTrace string
	}

	sstFat := kvserverpb.ReplicatedEvalResult_AddSSTable{
		Data:  []byte("foo"),
		CRC32: 0, // not checked
	}
	sstThin := kvserverpb.ReplicatedEvalResult_AddSSTable{
		CRC32: 0, // not checked
	}

	putOnDisk := func(ec *raftentry.Cache, ss SideloadStorage) {
		if err := ss.Put(context.Background(), 5, 6, sstFat.Data); err != nil {
			t.Fatal(err)
		}
	}

	testCases := map[string]testCase{
		// Plain old v1 Raft command without payload. Don't touch.
		"v1-no-payload": {thin: mkEnt(v1, 5, 6, &sstThin), fat: mkEnt(v1, 5, 6, &sstThin)},
		// With payload, but command is v1. Don't touch. Note that the
		// first of the two shouldn't happen in practice or we have a
		// huge problem once we try to apply this entry.
		"v1-slim-with-payload": {thin: mkEnt(v1, 5, 6, &sstThin), fat: mkEnt(v1, 5, 6, &sstThin)},
		"v1-with-payload":      {thin: mkEnt(v1, 5, 6, &sstFat), fat: mkEnt(v1, 5, 6, &sstFat)},
		// v2 with payload, but payload is AWOL. This would be fatal in practice.
		"v2-with-payload-missing-file": {
			thin: mkEnt(v2, 5, 6, &sstThin), fat: mkEnt(v2, 5, 6, &sstThin),
			expErr: "not found",
		},
		// v2 with payload that's actually there. The request we'll see in
		// practice.
		"v2-with-payload-with-file-no-cache": {
			thin: mkEnt(v2, 5, 6, &sstThin), fat: mkEnt(v2, 5, 6, &sstFat),
			setup: putOnDisk, expTrace: "inlined entry not cached",
		},
		"v2-with-payload-with-file-with-cache": {
			thin: mkEnt(v2, 5, 6, &sstThin), fat: mkEnt(v2, 5, 6, &sstFat),
			setup: func(ec *raftentry.Cache, ss SideloadStorage) {
				putOnDisk(ec, ss)
				ec.Add(rangeID, []raftpb.Entry{mkEnt(v2, 5, 6, &sstFat)}, true)
			}, expTrace: "using cache hit",
		},
		"v2-fat-without-file": {
			thin: mkEnt(v2, 5, 6, &sstFat), fat: mkEnt(v2, 5, 6, &sstFat),
			setup:    func(ec *raftentry.Cache, ss SideloadStorage) {},
			expTrace: "already inlined",
		},
	}

	runOne := func(k string, test testCase) {
		ctx, getRecAndFinish := tracing.ContextWithRecordingSpan(
			context.Background(), tracing.NewTracer(), "test-recording")
		defer getRecAndFinish()

		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()
		ss := newTestingSideloadStorage(eng)
		ec := raftentry.NewCache(1024) // large enough
		if test.setup != nil {
			test.setup(ec, ss)
		}

		thinCopy := *(protoutil.Clone(&test.thin).(*raftpb.Entry))
		newEnt, err := MaybeInlineSideloadedRaftCommand(ctx, rangeID, thinCopy, ss, ec)
		if err != nil {
			if test.expErr == "" || !testutils.IsError(err, test.expErr) {
				t.Fatalf("%s: %+v", k, err)
			}
		} else if test.expErr != "" {
			t.Fatalf("%s: success, but expected error: %s", k, test.expErr)
		} else {
			mustEntryEq(t, thinCopy, test.thin)
		}

		if newEnt == nil {
			newEnt = &thinCopy
		}
		mustEntryEq(t, *newEnt, test.fat)

		if dump := getRecAndFinish().String(); test.expTrace != "" {
			if ok, err := regexp.MatchString(test.expTrace, dump); err != nil {
				t.Fatalf("%s: %+v", k, err)
			} else if !ok {
				t.Fatalf("%s: expected trace matching:\n%s\n\nbut got\n%s", k, test.expTrace, dump)
			}
		}
	}

	keys := make([]string, 0, len(testCases))
	for k := range testCases {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		runOne(k, testCases[k])
	}
}

func TestRaftSSTableSideloadingSideload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	addSST := kvserverpb.ReplicatedEvalResult_AddSSTable{
		Data: []byte("foo"), CRC32: 0, // not checked
	}

	addSSTStripped := addSST
	addSSTStripped.Data = nil

	entV1Reg := mkEnt(raftlog.EntryEncodingStandardWithAC, 10, 99, nil)
	entV1SST := mkEnt(raftlog.EntryEncodingStandardWithAC, 11, 99, &addSST)
	entV2Reg := mkEnt(raftlog.EntryEncodingSideloadedWithAC, 12, 99, nil)
	entV2SST := mkEnt(raftlog.EntryEncodingSideloadedWithAC, 13, 99, &addSST)
	entV2SSTStripped := mkEnt(raftlog.EntryEncodingSideloadedWithAC, 13, 99, &addSSTStripped)

	type tc struct {
		name              string
		preEnts, postEnts []raftpb.Entry
		ss                []string
		// Expectations.
		size              int64
		nonSideloadedSize int64
	}

	// Intentionally ignore the fact that real calls would always have an
	// unbroken run of `entry.Index`.
	testCases := []tc{
		{
			name:     "empty",
			preEnts:  nil,
			postEnts: nil,
			ss:       nil,
		},
		{
			name:              "v1",
			preEnts:           []raftpb.Entry{entV1Reg, entV1SST},
			postEnts:          []raftpb.Entry{entV1Reg, entV1SST},
			nonSideloadedSize: int64(len(entV1Reg.Data) + len(entV1SST.Data)),
		},
		{
			name:     "v2",
			preEnts:  []raftpb.Entry{entV2SST, entV2Reg},
			postEnts: []raftpb.Entry{entV2SSTStripped, entV2Reg},
			ss:       []string{"i13.t99"},
			size:     int64(len(addSST.Data)),
		},
		{
			name:              "mixed",
			preEnts:           []raftpb.Entry{entV1Reg, entV1SST, entV2Reg, entV2SST},
			postEnts:          []raftpb.Entry{entV1Reg, entV1SST, entV2Reg, entV2SSTStripped},
			ss:                []string{"i13.t99"},
			size:              int64(len(addSST.Data)),
			nonSideloadedSize: int64(len(entV1Reg.Data) + len(entV1SST.Data)),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()
			sideloaded := newTestingSideloadStorage(eng)
			postEnts, numSideloaded, size, nonSideloadedSize, err := MaybeSideloadEntries(ctx, test.preEnts, sideloaded)
			if err != nil {
				t.Fatal(err)
			}
			if len(addSST.Data) == 0 {
				t.Fatal("invocation mutated original AddSSTable struct in memory")
			}
			require.Equal(t, test.nonSideloadedSize, nonSideloadedSize)
			var expNumSideloaded int
			if test.size > 0 {
				expNumSideloaded = 1
			}
			require.Equal(t, expNumSideloaded, numSideloaded)
			require.Equal(t, test.postEnts, postEnts)
			if test.size != size {
				t.Fatalf("expected %d sideloadedSize, but found %d", test.size, size)
			}
			actKeys, err := sideloaded.eng.List(sideloaded.Dir())
			if oserror.IsNotExist(err) {
				t.Log("swallowing IsNotExist")
				err = nil
			}
			require.NoError(t, err)
			sort.Strings(actKeys)
			if !reflect.DeepEqual(actKeys, test.ss) {
				t.Fatalf("expected %v, got %v", test.ss, actKeys)
			}
		})
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
