// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"golang.org/x/time/rate"
)

func entryEq(l, r raftpb.Entry) error {
	if reflect.DeepEqual(l, r) {
		return nil
	}
	_, lData := DecodeRaftCommand(l.Data)
	_, rData := DecodeRaftCommand(r.Data)
	var lc, rc kvserverpb.RaftCommand
	if err := protoutil.Unmarshal(lData, &lc); err != nil {
		return errors.Wrap(err, "unmarshalling LHS")
	}
	if err := protoutil.Unmarshal(rData, &rc); err != nil {
		return errors.Wrap(err, "unmarshalling RHS")
	}
	if !reflect.DeepEqual(lc, rc) {
		return errors.Newf("unexpected:\n%s", strings.Join(pretty.Diff(lc, rc), "\n"))
	}
	return nil
}

func mkEnt(
	v raftCommandEncodingVersion, index, term uint64, as *kvserverpb.ReplicatedEvalResult_AddSSTable,
) raftpb.Entry {
	cmdIDKey := strings.Repeat("x", raftCommandIDLen)
	var cmd kvserverpb.RaftCommand
	cmd.ReplicatedEvalResult.AddSSTable = as
	b, err := protoutil.Marshal(&cmd)
	if err != nil {
		panic(err)
	}
	var ent raftpb.Entry
	ent.Index, ent.Term = index, term
	ent.Data = encodeRaftCommand(v, kvserverbase.CmdIDKey(cmdIDKey), b)
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
		cleanup, eng := newOnDiskEngine(t)
		defer cleanup()
		defer eng.Close()
		testSideloadingSideloadedStorage(t, eng)
	})
}

func newTestingSideloadStorage(t *testing.T, eng storage.Engine) *diskSideloadStorage {
	st := cluster.MakeTestingClusterSettings()
	ss, err := newDiskSideloadStorage(
		st, 1, 2, filepath.Join(eng.GetAuxiliaryDir(), "fake", "testing", "dir"),
		rate.NewLimiter(rate.Inf, math.MaxInt64), eng,
	)
	require.NoError(t, err)
	return ss
}

func testSideloadingSideloadedStorage(t *testing.T, eng storage.Engine) {
	ctx := context.Background()
	ss := newTestingSideloadStorage(t, eng)

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

	file := func(i uint64) []byte { // take uint64 for convenience
		return []byte("content-" + strconv.Itoa(int(i)))
	}

	if err := ss.Put(ctx, 1, highTerm, file(1)); err != nil {
		t.Fatal(err)
	}

	assertCreated(true)

	if c, err := ss.Get(ctx, 1, highTerm); err != nil {
		t.Fatal(err)
	} else if exp := file(1); !bytes.Equal(c, exp) {
		t.Fatalf("got %q, wanted %q", c, exp)
	}

	// Overwrites the occupied slot.
	if err := ss.Put(ctx, 1, highTerm, file(12345)); err != nil {
		t.Fatal(err)
	}

	// ... consequently the old entry is gone.
	if c, err := ss.Get(ctx, 1, highTerm); err != nil {
		t.Fatal(err)
	} else if exp := file(12345); !bytes.Equal(c, exp) {
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
	payloads := []uint64{3, 5, 7, 9, 10}
	for n := range rand.Perm(len(payloads)) {
		i := payloads[n]
		if err := ss.Put(ctx, i, highTerm, file(i*highTerm)); err != nil {
			t.Fatalf("%d: %+v", i, err)
		}
	}

	assertCreated(true)

	// Write some more payloads, overlapping, at the past term.
	pastPayloads := append([]uint64{81}, payloads...)
	for _, i := range pastPayloads {
		if err := ss.Put(ctx, i, lowTerm, file(i*lowTerm)); err != nil {
			t.Fatal(err)
		}
	}

	// Just a sanity check that for the overlapping terms, we see both entries.
	for _, term := range []uint64{lowTerm, highTerm} {
		index := payloads[0] // exists at both lowTerm and highTerm
		if c, err := ss.Get(ctx, index, term); err != nil {
			t.Fatal(err)
		} else if exp := file(term * index); !bytes.Equal(c, exp) {
			t.Fatalf("got %q, wanted %q", c, exp)
		}
	}
	assertCreated(true)

	for n := range payloads {
		// Truncate indexes <= payloads[n] (payloads is sorted in increasing order).
		if _, _, err := ss.TruncateTo(ctx, payloads[n]); err != nil {
			t.Fatalf("%d: %+v", n, err)
		}
		// Index payloads[n] and above are still there (truncation is exclusive)
		// at both terms.
		for _, term := range []uint64{lowTerm, highTerm} {
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
		payloads := []uint64{3, 5, 7, 9, 10}
		for n := range rand.Perm(len(payloads)) {
			i := payloads[n]
			require.NoError(t, ss.Put(ctx, i, highTerm, file(i*highTerm)))
		}
		assertCreated(true)
		_, _, err = ss.TruncateTo(ctx, math.MaxUint64)
		require.NoError(t, err)
		// Ensure directory is removed when all records are removed.
		_, err = eng.Stat(ss.Dir())
		require.True(t, oserror.IsNotExist(err), "%v", err)
	}()

	require.NoError(t, ss.Clear(ctx))

	assertCreated(false)

	// Sanity check that we can call TruncateTo without the directory existing.
	_, _, err := ss.TruncateTo(ctx, 1)
	require.NoError(t, err)

	assertCreated(false)

	// Repopulate with a few entries at indexes=1,2,4 and term 10 to test `maybePurgeSideloaded`
	// with.
	for index := uint64(1); index < 5; index++ {
		if index == 3 {
			continue
		}
		payload := []byte(strings.Repeat("x", 1+int(index)))
		require.NoError(t, ss.Put(ctx, index, 10, payload))
	}

	// Term too high and too low, respectively. Shouldn't delete anything.
	for _, term := range []uint64{9, 11} {
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

	v1, v2 := raftVersionStandard, raftVersionSideloaded
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
		ctx, collect, cancel := tracing.ContextWithRecordingSpan(
			context.Background(), tracing.NewTracer(), "test-recording")
		defer cancel()

		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()
		ss := newTestingSideloadStorage(t, eng)
		ec := raftentry.NewCache(1024) // large enough
		if test.setup != nil {
			test.setup(ec, ss)
		}

		thinCopy := *(protoutil.Clone(&test.thin).(*raftpb.Entry))
		newEnt, err := maybeInlineSideloadedRaftCommand(ctx, rangeID, thinCopy, ss, ec)
		if err != nil {
			if test.expErr == "" || !testutils.IsError(err, test.expErr) {
				t.Fatalf("%s: %+v", k, err)
			}
		} else if test.expErr != "" {
			t.Fatalf("%s: success, but expected error: %s", k, test.expErr)
		} else if err := entryEq(thinCopy, test.thin); err != nil {
			t.Fatalf("%s: mutated the original entry: %s", k, pretty.Diff(thinCopy, test.thin))
		}

		if newEnt == nil {
			newEnt = &thinCopy
		}
		if err := entryEq(*newEnt, test.fat); err != nil {
			t.Fatalf("%s: %+v", k, err)
		}

		if dump := collect().String(); test.expTrace != "" {
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

	entV1Reg := mkEnt(raftVersionStandard, 10, 99, nil)
	entV1SST := mkEnt(raftVersionStandard, 11, 99, &addSST)
	entV2Reg := mkEnt(raftVersionSideloaded, 12, 99, nil)
	entV2SST := mkEnt(raftVersionSideloaded, 13, 99, &addSST)
	entV2SSTStripped := mkEnt(raftVersionSideloaded, 13, 99, &addSSTStripped)

	type tc struct {
		name              string
		preEnts, postEnts []raftpb.Entry
		ss                []string
		size              int64
	}

	// Intentionally ignore the fact that real calls would always have an
	// unbroken run of `entry.Index`.
	testCases := []tc{
		{
			name:     "empty",
			preEnts:  nil,
			postEnts: nil,
			ss:       nil,
			size:     0,
		},
		{
			name:     "v1",
			preEnts:  []raftpb.Entry{entV1Reg, entV1SST},
			postEnts: []raftpb.Entry{entV1Reg, entV1SST},
			size:     0,
		},
		{
			name:     "v2",
			preEnts:  []raftpb.Entry{entV2SST, entV2Reg},
			postEnts: []raftpb.Entry{entV2SSTStripped, entV2Reg},
			ss:       []string{"i13.t99"},
			size:     int64(len(addSST.Data)),
		},
		{
			name:     "mixed",
			preEnts:  []raftpb.Entry{entV1Reg, entV1SST, entV2Reg, entV2SST},
			postEnts: []raftpb.Entry{entV1Reg, entV1SST, entV2Reg, entV2SSTStripped},
			ss:       []string{"i13.t99"},
			size:     int64(len(addSST.Data)),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()
			sideloaded := newTestingSideloadStorage(t, eng)
			postEnts, size, err := maybeSideloadEntriesImpl(ctx, test.preEnts, sideloaded)
			if err != nil {
				t.Fatal(err)
			}
			if len(addSST.Data) == 0 {
				t.Fatal("invocation mutated original AddSSTable struct in memory")
			}
			if !reflect.DeepEqual(postEnts, test.postEnts) {
				t.Fatalf("result differs from expected: %s", pretty.Diff(postEnts, test.postEnts))
			}
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

// TestRaftSSTableSideloadingProposal runs a straightforward application of an `AddSSTable` command.
func TestRaftSSTableSideloadingProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "InMem", func(t *testing.T, engineInMem bool) {
		var eng storage.Engine
		if engineInMem {
			eng = storage.NewDefaultInMemForTesting()
		} else {
			var cleanup func()
			cleanup, eng = newOnDiskEngine(t)
			defer cleanup()
		}
		defer eng.Close()
		testRaftSSTableSideloadingProposal(t, eng)
	})
}

// TestRaftSSTableSideloadingProposal runs a straightforward application of an `AddSSTable` command.
func testRaftSSTableSideloadingProposal(t *testing.T, eng storage.Engine) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer SetMockAddSSTable()()

	stopper := stop.NewStopper()
	tc := testContext{}
	defer stopper.Stop(context.Background())
	tc.Start(t, stopper)

	tr := tc.store.ClusterSettings().Tracer
	tr.TestingRecordAsyncSpans() // we assert on async span traces in this test
	ctx, collect, cancel := tracing.ContextWithRecordingSpan(context.Background(), tr, "test-recording")
	defer cancel()

	const (
		key       = "foo"
		entrySize = 128
	)
	val := strings.Repeat("x", entrySize)

	ts := hlc.Timestamp{Logical: 1}

	if err := ProposeAddSSTable(ctx, key, val, ts, tc.store); err != nil {
		t.Fatal(err)
	}

	{
		var ba roachpb.BatchRequest
		get := getArgs(roachpb.Key(key))
		ba.Add(&get)
		ba.Header.RangeID = tc.repl.RangeID

		br, pErr := tc.store.Send(ctx, ba)
		if pErr != nil {
			t.Fatal(pErr)
		}
		v := br.Responses[0].GetInner().(*roachpb.GetResponse).Value
		if v == nil {
			t.Fatal("expected to read a value")
		}
		if valBytes, err := v.GetBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(valBytes, []byte(val)) {
			t.Fatalf("expected to read '%s', but found '%s'", val, valBytes)
		}
	}

	func() {
		tc.repl.raftMu.Lock()
		defer tc.repl.raftMu.Unlock()

		if err := testutils.MatchInOrder(
			collect().String(), "sideloadable proposal detected", "ingested SSTable",
		); err != nil {
			t.Fatal(err)
		}

		if n := tc.store.metrics.AddSSTableProposals.Count(); n == 0 {
			t.Fatalf("expected metric to show at least one AddSSTable proposal, but got %d", n)
		}

		if n := tc.store.metrics.AddSSTableApplications.Count(); n == 0 {
			t.Fatalf("expected metric to show at least one AddSSTable application, but got %d", n)
		}
		// We usually don't see copies because we hardlink and ingest the original SST. However, this
		// depends on luck and the file system, so don't try to assert it. We should, however, see
		// no more than one.
		expMaxCopies := int64(1)
		if n := tc.store.metrics.AddSSTableApplicationCopies.Count(); n > expMaxCopies {
			t.Fatalf("expected metric to show <= %d AddSSTable copies, but got %d", expMaxCopies, n)
		}
	}()

	// Force a log truncation followed by verification of the tracked raft log size. This exercises a
	// former bug in which the raft log size took the sideloaded payload into account when adding
	// to the log, but not when truncating.

	// Write enough keys to the range to make sure that a truncation will happen.
	for i := 0; i < RaftLogQueueStaleThreshold+1; i++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", i))
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", i)))
		if _, err := kv.SendWrapped(context.Background(), tc.store.TestSender(), &args); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tc.store.raftLogQueue.testingAdd(ctx, tc.repl, 99.99 /* priority */); err != nil {
		t.Fatal(err)
	}
	tc.store.MustForceRaftLogScanAndProcess()
	// SST is definitely truncated now, so recomputing the Raft log keys should match up with
	// the tracked size.
	verifyLogSizeInSync(t, tc.repl)
}

type mockSender struct {
	logEntries [][]byte
	done       bool
}

func (mr *mockSender) Send(req *SnapshotRequest) error {
	if req.LogEntries != nil {
		if mr.logEntries != nil {
			return errors.New("already have log entries")
		}
		mr.logEntries = req.LogEntries
	}
	return nil
}

func (mr *mockSender) Recv() (*SnapshotResponse, error) {
	if mr.done {
		return nil, io.EOF
	}
	status := SnapshotResponse_ACCEPTED
	if len(mr.logEntries) > 0 {
		status = SnapshotResponse_APPLIED
		mr.done = true
	}
	return &SnapshotResponse{Status: status}, nil
}

func newOnDiskEngine(t *testing.T) (func(), storage.Engine) {
	dir, cleanup := testutils.TempDir(t)
	eng, err := storage.NewDefaultEngine(
		1<<20,
		base.StorageConfig{
			Dir:       dir,
			MustExist: false,
		})
	if err != nil {
		t.Fatal(err)
	}
	return cleanup, eng
}

// This test verifies that when a snapshot is sent, sideloaded proposals are
// inlined.
func TestRaftSSTableSideloadingSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer SetMockAddSSTable()()

	ctx := context.Background()
	tc := testContext{}

	cleanup, eng := newOnDiskEngine(t)
	tc.engine = eng
	defer cleanup()
	defer eng.Close()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	var ba roachpb.BatchRequest
	ba.RangeID = tc.repl.RangeID

	// Disable log truncation as we want to be sure that we get to create
	// snapshots that have our sideloaded proposal in them.
	tc.store.SetRaftLogQueueActive(false)

	// Put a sideloaded proposal on the Range.
	key, val := "don't", "care"
	origSSTData, _ := MakeSSTable(key, val, hlc.Timestamp{}.Add(0, 1))
	{

		var addReq roachpb.AddSSTableRequest
		addReq.Data = origSSTData
		addReq.Key = roachpb.Key(key)
		addReq.EndKey = addReq.Key.Next()
		ba.Add(&addReq)

		_, pErr := tc.store.Send(ctx, ba)
		if pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Run a happy case snapshot. Check that it properly inlines the payload in
	// the contained log entries.
	inlinedEntry := func() raftpb.Entry {
		os, err := tc.repl.GetSnapshot(ctx, SnapshotRequest_VIA_SNAPSHOT_QUEUE, tc.store.StoreID())
		if err != nil {
			t.Fatal(err)
		}
		defer os.Close()

		mockSender := &mockSender{}
		if err := sendSnapshot(
			ctx,
			tc.store.cfg.Settings,
			mockSender,
			&fakeStorePool{},
			SnapshotRequest_Header{State: os.State, Priority: SnapshotRequest_RECOVERY},
			os,
			tc.repl.store.Engine().NewBatch,
			func() {},
		); err != nil {
			t.Fatal(err)
		}

		var ent raftpb.Entry
		var cmd kvserverpb.RaftCommand
		var finalEnt raftpb.Entry
		for _, entryBytes := range mockSender.logEntries {
			if err := protoutil.Unmarshal(entryBytes, &ent); err != nil {
				t.Fatal(err)
			}
			if sniffSideloadedRaftCommand(ent.Data) {
				_, cmdBytes := DecodeRaftCommand(ent.Data)
				if err := protoutil.Unmarshal(cmdBytes, &cmd); err != nil {
					t.Fatal(err)
				}
				if as := cmd.ReplicatedEvalResult.AddSSTable; as == nil {
					t.Fatalf("no AddSSTable found in sideloaded command %+v", cmd)
				} else if len(as.Data) == 0 {
					t.Fatalf("empty payload in sideloaded command: %+v", cmd)
				}
				finalEnt = ent
			}
		}
		if finalEnt.Index == 0 {
			t.Fatal("no sideloaded command found")
		}
		return finalEnt
	}()

	sideloadedIndex := inlinedEntry.Index

	// This happens to be a good point in time to check the `entries()` method
	// which has special handling to accommodate `term()`: when an empty
	// sideload storage is passed in, `entries()` should not inline, and in turn
	// also not populate the entries cache (since its contents must always be
	// fully inlined).
	func() {
		tc.repl.raftMu.Lock()
		defer tc.repl.raftMu.Unlock()
		tc.repl.mu.Lock()
		defer tc.repl.mu.Unlock()
		for _, withSS := range []bool{false, true} {
			tc.store.raftEntryCache.Clear(tc.repl.RangeID, sideloadedIndex+1)

			var ss SideloadStorage
			if withSS {
				ss = tc.repl.raftMu.sideloaded
			}
			rsl := stateloader.Make(tc.repl.RangeID)
			entries, err := entries(
				ctx, rsl, tc.store.Engine(), tc.repl.RangeID, tc.store.raftEntryCache,
				ss, sideloadedIndex, sideloadedIndex+1, 1<<20,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("no or too many entries returned from cache: %+v", entries)
			}
			ents, _, _, _ := tc.store.raftEntryCache.Scan(nil, tc.repl.RangeID, sideloadedIndex, sideloadedIndex+1, 1<<20)
			if withSS {
				// We passed the sideload storage, so we expect to get our
				// inlined index back from the cache.
				if len(ents) != 1 {
					t.Fatalf("no or too many entries returned from cache: %+v", ents)
				}
				if err := entryEq(inlinedEntry, ents[0]); err != nil {
					t.Fatalf("withSS=%t: %+v", withSS, err)
				}
			} else {
				// Without sideload storage, expect the cache to remain
				// unpopulated and the entry returned from entries() to not have
				// been inlined.
				if len(ents) != 0 {
					t.Fatalf("expected no cached entries, but got %+v", ents)
				}
				if expErr, err := `ReplicatedEvalResult.AddSSTable.Data: \[\]uint8\[\d+\] != \[\]uint8\[0\]`,
					entryEq(inlinedEntry, entries[0]); !testutils.IsError(
					err,
					expErr,
				) {
					t.Fatalf("expected specific mismatch on `Data` field, but got %v\nwanted: %s", err, expErr)
				}
			}
		}
	}()

	// Now run a snapshot that will fail since it doesn't find one of its on-disk
	// payloads. This can happen if the Raft log queue runs between the time the
	// (engine) snapshot is taken and the log entries are actually read from the
	// (engine) snapshot. We didn't run this before because we wanted the file
	// to stay in sideloaded storage for the previous test.
	func() {
		failingOS, err := tc.repl.GetSnapshot(ctx, SnapshotRequest_VIA_SNAPSHOT_QUEUE, tc.store.StoreID())
		if err != nil {
			t.Fatal(err)
		}
		defer failingOS.Close()

		// Remove the actual file.
		tc.repl.raftMu.Lock()
		if err := tc.repl.raftMu.sideloaded.Clear(ctx); err != nil {
			tc.repl.raftMu.Unlock()
			t.Fatal(err)
		}
		tc.repl.raftMu.Unlock()
		// Additionally we need to clear out the entry from the cache because
		// that would still save the day.
		tc.store.raftEntryCache.Clear(tc.repl.RangeID, sideloadedIndex+1)

		mockSender := &mockSender{}
		err = sendSnapshot(
			ctx,
			tc.store.cfg.Settings,
			mockSender,
			&fakeStorePool{},
			SnapshotRequest_Header{State: failingOS.State, Priority: SnapshotRequest_RECOVERY},
			failingOS,
			tc.repl.store.Engine().NewBatch,
			func() {},
		)
		if !errors.HasType(err, (*errMustRetrySnapshotDueToTruncation)(nil)) {
			t.Fatal(err)
		}
	}()
}

func TestRaftSSTableSideloadingTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer SetMockAddSSTable()()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	tc.Start(t, stopper)
	ctx := context.Background()

	const count = 10

	var indexes []uint64
	addLastIndex := func() {
		lastIndex, err := tc.repl.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, lastIndex)
	}
	for i := 0; i < count; i++ {
		addLastIndex()
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		if err := ProposeAddSSTable(ctx, key, val, tc.Clock().Now(), tc.store); err != nil {
			t.Fatalf("%d: %+v", i, err)
		}
	}
	// Append an extra entry which, if we truncate it, should definitely also
	// remove any leftover files (ok, unless the last one is reproposed but
	// that's *very* unlikely to happen for the last one)
	addLastIndex()

	fmtSideloaded := func() []string {
		tc.repl.raftMu.Lock()
		defer tc.repl.raftMu.Unlock()
		fs, _ := tc.repl.Engine().List(tc.repl.raftMu.sideloaded.Dir())
		sort.Strings(fs)
		return fs
	}

	// Check that when we truncate, the number of on-disk files changes in ways
	// we expect. Intentionally not too strict due to the possibility of
	// reproposals, etc; it could be made stricter, but this should give enough
	// confidence already that we're calling `PurgeTo` correctly, and for the
	// remainder unit testing on each impl's PurgeTo is more useful.
	for i := range indexes {
		const rangeID = 1
		newFirstIndex := indexes[i] + 1
		truncateArgs := truncateLogArgs(newFirstIndex, rangeID)
		log.Eventf(ctx, "truncating to index < %d", newFirstIndex)
		if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{RangeID: rangeID}, &truncateArgs); pErr != nil {
			t.Fatal(pErr)
		}
		sideloadStrings := fmtSideloaded()
		if minFiles := count - i; len(sideloadStrings) < minFiles {
			t.Fatalf("after truncation at %d (i=%d), expected at least %d files left, but have:\n%v",
				indexes[i], i, minFiles, sideloadStrings)
		}
	}

	if sideloadStrings := fmtSideloaded(); len(sideloadStrings) != 0 {
		t.Fatalf("expected all files to be cleaned up, but found %v", sideloadStrings)
	}

}
