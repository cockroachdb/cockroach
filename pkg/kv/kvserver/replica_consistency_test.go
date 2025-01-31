// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestReplicaChecksumVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	testutils.RunTrueAndFalse(t, "matchingVersion", func(t *testing.T, matchingVersion bool) {
		cc := kvserverpb.ComputeChecksum{
			ChecksumID: uuid.MakeV4(),
			Mode:       kvpb.ChecksumMode_CHECK_FULL,
		}
		if matchingVersion {
			cc.Version = batcheval.ReplicaChecksumVersion
		} else {
			cc.Version = 1
		}

		var g errgroup.Group
		g.Go(func() error { return tc.repl.computeChecksumPostApply(ctx, cc) })
		// NB: This timeout should be longer than storage.maxEfosWait.
		shortCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		rc, err := tc.repl.getChecksum(shortCtx, cc.ChecksumID)
		taskErr := g.Wait()

		if !matchingVersion {
			require.ErrorContains(t, taskErr, "incompatible versions")
			require.Error(t, err)
			// There is a race between computeChecksumPostApply and getChecksum, so we
			// either get an early error, or a timeout, both of which are correct.
			require.True(t, strings.Contains(err.Error(), "checksum task failed to start") ||
				strings.Contains(err.Error(), "did not start in time"))
			require.Nil(t, rc.Checksum)
		} else {
			require.NoError(t, taskErr)
			require.NoError(t, err)
			require.NotNil(t, rc.Checksum)
		}
	})
}

func TestStoreCheckpointSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := Store{}
	s.mu.replicasByKey = newStoreReplicaBTree()
	s.mu.replicaPlaceholders = map[roachpb.RangeID]*ReplicaPlaceholder{}

	makeDesc := func(rangeID roachpb.RangeID, start, end string) roachpb.RangeDescriptor {
		desc := roachpb.RangeDescriptor{RangeID: rangeID}
		if start != "" {
			desc.StartKey = roachpb.RKey(start)
			desc.EndKey = roachpb.RKey(end)
		}
		return desc
	}
	var descs []roachpb.RangeDescriptor
	addReplica := func(rangeID roachpb.RangeID, start, end string) {
		desc := makeDesc(rangeID, start, end)
		r := &Replica{RangeID: rangeID, startKey: desc.StartKey}
		r.shMu.state.Desc = &desc
		r.isInitialized.Store(desc.IsInitialized())
		require.NoError(t, s.addToReplicasByRangeIDLocked(r))
		if r.IsInitialized() {
			require.NoError(t, s.addToReplicasByKeyLocked(r, r.Desc()))
			descs = append(descs, desc)
		}
	}
	addPlaceholder := func(rangeID roachpb.RangeID, start, end string) {
		require.NoError(t, s.addPlaceholderLocked(
			&ReplicaPlaceholder{rangeDesc: makeDesc(rangeID, start, end)},
		))
	}

	addReplica(1, "a", "b")
	addReplica(4, "b", "c")
	addPlaceholder(5, "c", "d")
	addReplica(2, "e", "f")
	addReplica(3, "", "") // uninitialized

	want := [][]string{{
		// r1 with keys [a, b). The checkpoint includes range-ID replicated and
		// unreplicated keyspace for ranges 1-2 and 4. Range 2 is included because
		// it's a neighbour of r1 by range ID. The checkpoint also includes
		// replicated user keyspace {a-c} owned by ranges 1 and 4.
		"/Local/RangeID/{1\"\"-3\"\"}",
		"/Local/RangeID/{4\"\"-5\"\"}",
		"/Local/Range\"{a\"-c\"}",
		"/Local/Lock/Local/Range\"{a\"-c\"}",
		"/Local/Lock\"{a\"-c\"}",
		"{a-c}",
	}, {
		// r4 with keys [b, c). The checkpoint includes range-ID replicated and
		// unreplicated keyspace for ranges 3-4, 1 and 2. Range 3 is included
		// because it's a neighbour of r4 by range ID. The checkpoint also includes
		// replicated user keyspace {a-f} owned by ranges 1, 4, and 2.
		"/Local/RangeID/{3\"\"-5\"\"}",
		"/Local/RangeID/{1\"\"-2\"\"}",
		"/Local/RangeID/{2\"\"-3\"\"}",
		"/Local/Range\"{a\"-f\"}",
		"/Local/Lock/Local/Range\"{a\"-f\"}",
		"/Local/Lock\"{a\"-f\"}",
		"{a-f}",
	}, {
		// r2 with keys [e, f). The checkpoint includes range-ID replicated and
		// unreplicated keyspace for ranges 1-3 and 4. Ranges 1 and 3 are included
		// because they are neighbours of r2 by range ID. The checkpoint also
		// includes replicated user keyspace {b-f} owned by ranges 4 and 2.
		"/Local/RangeID/{1\"\"-4\"\"}",
		"/Local/RangeID/{4\"\"-5\"\"}",
		"/Local/Range\"{b\"-f\"}",
		"/Local/Lock/Local/Range\"{b\"-f\"}",
		"/Local/Lock\"{b\"-f\"}",
		"{b-f}",
	}}

	require.Len(t, want, len(descs))
	for i, desc := range descs {
		spans := s.checkpointSpans(&desc)
		got := make([]string, 0, len(spans))
		for _, s := range spans {
			got = append(got, s.String())
		}
		require.Equal(t, want[i], got, i)
	}
}

func TestGetChecksumNotSuccessfulExitConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	startChecksumTask := func(ctx context.Context, id uuid.UUID) error {
		return tc.repl.computeChecksumPostApply(ctx, kvserverpb.ComputeChecksum{
			ChecksumID: id,
			Mode:       kvpb.ChecksumMode_CHECK_FULL,
			Version:    batcheval.ReplicaChecksumVersion,
		})
	}

	// Checksum computation failed to start.
	id := uuid.MakeV4()
	c, _ := tc.repl.trackReplicaChecksum(id)
	close(c.started)
	rc, err := tc.repl.getChecksum(ctx, id)
	require.ErrorContains(t, err, "checksum task failed to start")
	require.Nil(t, rc.Checksum)

	// Checksum computation started, but failed.
	id = uuid.MakeV4()
	c, _ = tc.repl.trackReplicaChecksum(id)
	var g errgroup.Group
	g.Go(func() error {
		c.started <- func() {}
		close(c.started)
		close(c.result)
		return nil
	})
	rc, err = tc.repl.getChecksum(ctx, id)
	require.ErrorContains(t, err, "no checksum found")
	require.Nil(t, rc.Checksum)
	require.NoError(t, g.Wait())

	// The initial wait for the task start expires. This will take 10ms.
	id = uuid.MakeV4()
	rc, err = tc.repl.getChecksum(ctx, id)
	require.ErrorContains(t, err, "checksum computation did not start")
	require.Nil(t, rc.Checksum)

	// The computation has started, but the request context timed out.
	id = uuid.MakeV4()
	c, _ = tc.repl.trackReplicaChecksum(id)
	g.Go(func() error {
		c.started <- func() {}
		close(c.started)
		return nil
	})
	rc, err = tc.repl.getChecksum(ctx, id)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, rc.Checksum)
	require.NoError(t, g.Wait())

	// Context is canceled during the initial waiting.
	id = uuid.MakeV4()
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	rc, err = tc.repl.getChecksum(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, rc.Checksum)

	// The task failed to start because the checksum collection request did not
	// join. Later, when it joins, it doesn't find any trace and times out.
	id = uuid.MakeV4()
	c, _ = tc.repl.trackReplicaChecksum(id)
	require.NoError(t, startChecksumTask(context.Background(), id))
	// TODO(pavelkalinnikov): Avoid this long wait in the test.
	time.Sleep(2 * consistencyCheckSyncTimeout) // give the task time to give up
	_, ok := <-c.started
	require.False(t, ok) // ensure the task gave up
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc, err = tc.repl.getChecksum(ctx, id) // blocks for 100ms
	require.ErrorContains(t, err, "checksum computation did not start")
}

// TestReplicaChecksumSHA512 checks that a given dataset produces the expected
// checksum.
func TestReplicaChecksumSHA512(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t)

	ctx := context.Background()
	sb := &strings.Builder{}
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("z"),
	}

	// Hash the empty state.
	unlim := quotapool.NewRateLimiter("test", quotapool.Inf(), 0)
	rd, err := CalcReplicaDigest(ctx, desc, eng, kvpb.ChecksumMode_CHECK_FULL, unlim, nil /* settings */)
	require.NoError(t, err)
	fmt.Fprintf(sb, "checksum0: %x\n", rd.SHA512)

	// We incrementally add writes, and check the checksums after each write to
	// make sure they differ such that each write affects the checksum.
	kvs := []struct {
		key     string
		endKey  string
		ts      int64
		localTS int64
		value   string
	}{
		{"a", "", 1, 0, "a1"},
		{"a", "", 2, 0, ""},
		{"b", "", 3, 2, "b3"},
		{"i", "", 0, 0, "i0"},
		// Range keys can currently only be tombstones.
		{"p", "q", 1, 0, ""},
		{"x", "z", 9, 8, ""},
	}

	for i, kv := range kvs {
		key, endKey := roachpb.Key(kv.key), roachpb.Key(kv.endKey)
		ts := hlc.Timestamp{WallTime: kv.ts}
		localTS := hlc.ClockTimestamp{WallTime: kv.localTS}
		var value roachpb.Value
		if kv.value != "" {
			value = roachpb.MakeValueFromString(kv.value)
		}

		if len(endKey) > 0 {
			require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(
				ctx, eng, nil, key, endKey, ts, localTS, nil, nil, false, 0, 0, nil))
		} else {
			_, err = storage.MVCCPut(ctx, eng, key, ts, value, storage.MVCCWriteOptions{LocalTimestamp: localTS})
			require.NoError(t, err)
		}

		rd, err = CalcReplicaDigest(ctx, desc, eng, kvpb.ChecksumMode_CHECK_FULL, unlim, nil /* settings */)
		require.NoError(t, err)
		fmt.Fprintf(sb, "checksum%d: %x\n", i+1, rd.SHA512)
	}

	// We then do the same for replicated locks.
	locks := []struct {
		key   string
		str   lock.Strength
		txnID int64
	}{
		{"a", lock.Exclusive, 1},
		{"b", lock.Shared, 1},
		{"b", lock.Shared, 2},
	}

	for i, l := range locks {
		txnID := uuid.FromUint128(uint128.FromInts(0, uint64(l.txnID)))
		txn := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: txnID}}
		require.NoError(t, storage.MVCCAcquireLock(ctx, eng, &txn.TxnMeta, txn.IgnoredSeqNums, l.str, roachpb.Key(l.key), nil, 0, 0))

		rd, err = CalcReplicaDigest(ctx, desc, eng, kvpb.ChecksumMode_CHECK_FULL, unlim, nil /* settings */)
		require.NoError(t, err)
		fmt.Fprintf(sb, "checksum%d: %x\n", i+1, rd.SHA512)
	}

	// Run another check to obtain stats for the final state.
	rd, err = CalcReplicaDigest(ctx, desc, eng, kvpb.ChecksumMode_CHECK_FULL, unlim, nil /* settings */)
	require.NoError(t, err)
	jsonpb := protoutil.JSONPb{Indent: "  "}
	json, err := jsonpb.Marshal(&rd.RecomputedMS)
	require.NoError(t, err)
	fmt.Fprintf(sb, "stats: %s\n", string(json))

	echotest.Require(t, sb.String(), datapathutils.TestDataPath(t, "replica_consistency_sha512"))
}
