// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"os"
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
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	ent.Data = raftlog.EncodeCommandBytes(enc, kvserverbase.CmdIDKey(cmdIDKey), b, 0 /* pri */)
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

// TODO(pavelkalinnikov): give these tests a good refactor.
func testSideloadingSideloadedStorage(t *testing.T, eng storage.Engine) {
	ctx := context.Background()
	ss := newTestingSideloadStorage(eng)

	assertExists := func(exists bool) {
		t.Helper()
		_, err := ss.eng.Env().Stat(ss.dir)
		if !exists {
			require.True(t, oserror.IsNotExist(err), err)
		} else {
			require.NoError(t, err)
		}
	}

	assertExists(false)

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

	assertExists(true)

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

	assertExists(false)

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
		assertExists(false)
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

	assertExists(true)

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
	assertExists(true)

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
		f, err := eng.Env().Create(nonRemovableFile, fs.UnspecifiedWriteCategory)
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
			_, err := eng.Env().Stat(nonRemovableFile)
			require.NoError(t, err)
		}

		// Now remove extra file and let truncation proceed to remove directory.
		require.NoError(t, eng.Env().Remove(nonRemovableFile))

		// Test that directory is removed when filepath.Glob returns 0 matches.
		_, _, err = ss.TruncateTo(ctx, math.MaxUint64)
		require.NoError(t, err)
		// Ensure directory is removed, now that all files should be gone.
		_, err = eng.Env().Stat(ss.Dir())
		require.True(t, oserror.IsNotExist(err), "%v", err)
		// Ensure HasAnyEntry doesn't find anything.
		found, err := ss.HasAnyEntry(ctx, 0, 10000)
		require.NoError(t, err)
		require.False(t, found)

		// Repopulate with some random indexes to test deletion when there are a
		// non-zero number of filepath.Glob matches.
		payloads := []kvpb.RaftIndex{3, 5, 7, 9, 10}
		for n := range rand.Perm(len(payloads)) {
			i := payloads[n]
			require.NoError(t, ss.Put(ctx, i, highTerm, file(i, highTerm)))
		}
		assertExists(true)
		// Verify the HasAnyEntry semantics.
		for _, check := range []struct {
			from, to kvpb.RaftIndex
			want     bool
		}{
			{from: 0, to: 3, want: false}, // 3 is excluded
			{from: 0, to: 4, want: true},  // but included if to == 4
			{from: 3, to: 5, want: true},  // 3 is included
			{from: 4, to: 5, want: false},
			{from: 50, to: 60, want: false},
			{from: 1, to: 10, want: true},
		} {
			found, err := ss.HasAnyEntry(ctx, check.from, check.to)
			require.NoError(t, err)
			require.Equal(t, check.want, found)
		}
		freed, retained, err := ss.BytesIfTruncatedFromTo(ctx, 0, math.MaxUint64)
		require.NoError(t, err)
		require.Zero(t, retained)
		freedByTruncateTo, retainedByTruncateTo, err := ss.TruncateTo(ctx, math.MaxUint64)
		require.NoError(t, err)
		require.Zero(t, retainedByTruncateTo)
		require.Equal(t, freedByTruncateTo, freed)
		// Ensure directory is removed when all records are removed.
		_, err = eng.Env().Stat(ss.Dir())
		require.True(t, oserror.IsNotExist(err), "%v", err)
	}()

	require.NoError(t, ss.Clear(ctx))

	assertExists(false)

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

	assertExists(false)

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
			actKeys, err := sideloaded.eng.Env().List(sideloaded.Dir())
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
		fs.MustInitPhysicalTestingEnv(dir),
		cluster.MakeClusterSettings(),
		storage.CacheSize(1<<20 /* 1 MiB */))
	if err != nil {
		t.Fatal(err)
	}
	return cleanup, eng
}

// TestSideloadStorageSync tests that the sideloaded storage syncs files and
// directories properly, to survive crashes.
func TestSideloadStorageSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "sync", func(t *testing.T, sync bool) {
		// Create a sideloaded storage with an in-memory FS. Use strict MemFS to be
		// able to emulate crash restart by rolling it back to last synced state.
		ctx := context.Background()
		memFS := vfs.NewCrashableMem()
		env, err := fs.InitEnv(ctx, memFS, "", fs.EnvConfig{}, nil /* statsCollector */)
		require.NoError(t, err)
		eng, err := storage.Open(ctx, env, cluster.MakeTestingClusterSettings(), storage.ForTesting)
		require.NoError(t, err)
		ss := newTestingSideloadStorage(eng)

		// Put an entry which should trigger the lazy creation of the sideloaded
		// directories structure, and create a file for this entry.
		const testEntry = "test-entry"
		require.NoError(t, ss.Put(ctx, 100 /* index */, 6 /* term */, []byte(testEntry)))
		if sync {
			require.NoError(t, ss.Sync())
		}
		// Cut off all syncs from this point, to emulate a crash.
		crashFS := memFS.CrashClone(vfs.CrashCloneCfg{})
		ss = nil
		eng.Close()
		// Reset filesystem to the last synced state.

		// Emulate process restart. Load from the last synced state.
		env, err = fs.InitEnv(ctx, crashFS, "", fs.EnvConfig{}, nil /* statsCollector */)
		require.NoError(t, err)
		eng, err = storage.Open(ctx, env, cluster.MakeTestingClusterSettings(), storage.ForTesting)
		require.NoError(t, err)
		defer eng.Close()
		ss = newTestingSideloadStorage(eng)

		// The sideloaded directory must exist because all its parents are synced.
		_, err = eng.Env().Stat(ss.Dir())
		require.NoError(t, err)

		// The stored entry is still durable if we synced the sideloaded storage
		// before the crash.
		got, err := ss.Get(ctx, 100 /* index */, 6 /* term */)
		if sync {
			require.NoError(t, err)
			require.Equal(t, testEntry, string(got))
		} else {
			require.ErrorIs(t, err, errSideloadedFileNotFound)
		}
		// A "control" check that missing entries are unconditionally missing.
		_, err = ss.Get(ctx, 200 /* index */, 7 /* term */)
		require.ErrorIs(t, err, errSideloadedFileNotFound)
	})
}

func TestMkdirAllAndSyncParents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		path   string   // a directory that exists
		sync   []string // prefix directories of `path` that were synced
		create string   // the directory to be created

		wantExist []string // directories we want to exist after a crash
		wantGone  []string // un-synced directories we expect removed after a crash
	}{{
		path:      "/",
		create:    "/a/b",
		wantExist: []string{"/", "/a", "/a/b"},
	}, {
		path:      "/a",
		sync:      []string{"/"},
		create:    "/a", // edge case, just makes sure the base dir exists
		wantExist: []string{"/", "/a"},
	}, {
		path:      "/a",
		create:    "/a", // same edge case, but we test that there is no sync
		wantExist: []string{"/"},
		wantGone:  []string{"/a"}, // "a" was created, but the root was not synced
	}, {
		path:      "/a/b",
		sync:      []string{"/"}, // "a" is not synced, so mkdir won't persist
		create:    "/a/b/c/d/",
		wantExist: []string{"/", "/a"},
		wantGone:  []string{"/a/b", "/a/b/c", "/a/b/c/d"},
	}, {
		path:      "/a/b",
		sync:      []string{"/", "/a"}, // "a" is synced, so mkdir will persist
		create:    "/a/b/c",
		wantExist: []string{"/", "/a", "/a/b", "/a/b/c"},
	}, {
		path:      "/a/b",
		sync:      []string{"/", "/a"}, // "a" is synced, so mkdir will persist
		create:    "/a/b/c/d",
		wantExist: []string{"/", "/a", "/a/b", "/a/b/c", "/a/b/c/d"},
	}, {
		path:      "/a/b/c",
		sync:      []string{"/", "/a"}, // "b" is not synced, and won't be because "c" exists
		create:    "/a/b/c/d",
		wantExist: []string{"/", "/a", "/a/b"},
		wantGone:  []string{"/a/b/c", "/a/b/c/d"},
	}, {
		path:      "/a/b/c",
		sync:      []string{"/", "/a"}, // "b" is not synced, and won't be because "c" exists
		create:    "/a/b/c/d",
		wantExist: []string{"/", "/a", "/a/b"},
		wantGone:  []string{"/a/b/c", "/a/b/c/d"},
	}, {
		path:      "/a/b/c",
		sync:      []string{"/", "/a"}, // "b" is not synced, and won't be because "c" exists
		create:    "/a/b/c/d",
		wantExist: []string{"/", "/a", "/a/b"},
		wantGone:  []string{"/a/b/c", "/a/b/c/d"},
	}, {
		path:      "a/b", // test relative paths too
		sync:      []string{"", "a"},
		create:    "a/b/c",
		wantExist: []string{"a", "a/b", "a/b/c"},
	}, {
		path:      "a/b",
		sync:      []string{""},
		create:    "a/b/c",
		wantExist: []string{"a"},
		wantGone:  []string{"a/b", "a/b/c"},
	}, {
		path:      "../a/b",
		sync:      []string{"", "..", "../a"},
		create:    "../a/b/c",
		wantExist: []string{"../a/b/c"},
	}, {
		path:      "../a/b",
		sync:      []string{"", ".."}, // "a" not synced, the dirs will be lost
		create:    "../a/b/c",
		wantExist: []string{".."},
		wantGone:  []string{"../a/b/c"},
	}} {
		t.Run("", func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			require.NoError(t, fs.MkdirAll(tc.path, os.ModePerm))
			for _, dir := range tc.sync {
				handle, err := fs.OpenDir(dir)
				require.NoError(t, err)
				require.NoError(t, handle.Sync())
				require.NoError(t, handle.Close())
			}
			require.NoError(t, mkdirAllAndSyncParents(fs, tc.create, os.ModePerm))

			assertExistence := func(t *testing.T, dirs []string, exist bool) {
				t.Helper()
				for _, dir := range dirs {
					handle, err := fs.OpenDir(dir)
					if exist {
						require.NoError(t, err, dir)
						require.NoError(t, handle.Close(), dir)
					} else {
						require.True(t, oserror.IsNotExist(err), dir)
					}
				}
			}

			// Before crash, all the relevant directories must exist.
			assertExistence(t, tc.wantExist, true)
			assertExistence(t, tc.wantGone, true)
			// After crash and resetting to the synced state, wantExist directories
			// must exist, and wantGone are lost.
			fs = fs.CrashClone(vfs.CrashCloneCfg{})
			assertExistence(t, tc.wantExist, true)
			assertExistence(t, tc.wantGone, false)
		})
	}
}

func TestMkdirAllAndSyncParentsErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("empty", func(t *testing.T) {
		fs := vfs.NewMem()
		// The root exists in an empty MemFS.
		require.NoError(t, mkdirAllAndSyncParents(fs, "/", os.ModePerm))
		// TODO(pavelkalinnikov): find a way to remove "/", and exercise the missing
		// root error for absolute paths.
		// For now, removing "/" does not succeed in MemFS.
		assert.ErrorContains(t, fs.Remove("/"), "empty file name")
	})

	t.Run("not-a-directory", func(t *testing.T) {
		memFS := vfs.NewMem()
		require.NoError(t, mkdirAllAndSyncParents(memFS, "/a", os.ModePerm))

		// Write a file, and try to trick mkdir into thinking that it's a directory.
		f, err := memFS.Create("/a/file", fs.UnspecifiedWriteCategory)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		for _, path := range []string{"/a/file", "/a/file/sub"} {
			require.ErrorContains(t, mkdirAllAndSyncParents(memFS, path, os.ModePerm), "not a directory")
		}
	})
}
