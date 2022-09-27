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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
			ChecksumID: uuid.FastMakeV4(),
			Mode:       roachpb.ChecksumMode_CHECK_FULL,
		}
		if matchingVersion {
			cc.Version = batcheval.ReplicaChecksumVersion
		} else {
			cc.Version = 1
		}
		var g errgroup.Group
		g.Go(func() error { return tc.repl.computeChecksumPostApply(ctx, cc) })
		rc, err := tc.repl.getChecksum(ctx, cc.ChecksumID)
		taskErr := g.Wait()
		if !matchingVersion {
			require.ErrorContains(t, taskErr, "incompatible versions")
			require.ErrorContains(t, err, "checksum task failed to start")
			require.Nil(t, rc.Checksum)
		} else {
			require.NoError(t, taskErr)
			require.NoError(t, err)
			require.NotNil(t, rc.Checksum)
		}
	})
}

func TestGetChecksumNotSuccessfulExitConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	startChecksumTask := func(ctx context.Context, id uuid.UUID) error {
		return tc.repl.computeChecksumPostApply(ctx, kvserverpb.ComputeChecksum{
			ChecksumID: id,
			Mode:       roachpb.ChecksumMode_CHECK_FULL,
			Version:    batcheval.ReplicaChecksumVersion,
		})
	}

	// Checksum computation failed to start.
	id := uuid.FastMakeV4()
	c, _ := tc.repl.getReplicaChecksum(id, timeutil.Now())
	close(c.started)
	rc, err := tc.repl.getChecksum(ctx, id)
	require.ErrorContains(t, err, "checksum task failed to start")
	require.Nil(t, rc.Checksum)

	// Checksum computation started, but failed.
	id = uuid.FastMakeV4()
	c, _ = tc.repl.getReplicaChecksum(id, timeutil.Now())
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
	id = uuid.FastMakeV4()
	rc, err = tc.repl.getChecksum(ctx, id)
	require.ErrorContains(t, err, "checksum computation did not start")
	require.Nil(t, rc.Checksum)
	require.ErrorContains(t, startChecksumTask(context.Background(), id),
		"checksum collection request gave up")

	// The computation has started, but the request context timed out.
	id = uuid.FastMakeV4()
	c, _ = tc.repl.getReplicaChecksum(id, timeutil.Now())
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
	id = uuid.FastMakeV4()
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	rc, err = tc.repl.getChecksum(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, rc.Checksum)
	require.ErrorContains(t, startChecksumTask(context.Background(), id),
		"checksum collection request gave up")

	// The task failed to start because the checksum collection request did not
	// join. Later, when it joins, it finds out that the task gave up.
	id = uuid.FastMakeV4()
	c, _ = tc.repl.getReplicaChecksum(id, timeutil.Now())
	require.NoError(t, startChecksumTask(context.Background(), id))
	// TODO(pavelkalinnikov): Avoid this long wait in the test.
	time.Sleep(2 * consistencyCheckSyncTimeout) // give the task time to give up
	_, ok := <-c.started
	require.False(t, ok) // ensure the task gave up
	rc, err = tc.repl.getChecksum(context.Background(), id)
	require.ErrorContains(t, err, "checksum task failed to start")
}

// TestReplicaChecksumSHA512 checks that a given dataset produces the expected
// checksum.
func TestReplicaChecksumSHA512(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t)

	ctx := context.Background()
	sb := &strings.Builder{}
	lim := quotapool.NewRateLimiter("rate", 1e9, 0)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	repl := &Replica{} // We don't actually need the replica at all, just the method.
	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("z"),
	}

	// Hash the empty state.
	rh, err := repl.sha512(ctx, desc, eng, nil, roachpb.ChecksumMode_CHECK_FULL, lim)
	require.NoError(t, err)
	fmt.Fprintf(sb, "checksum0: %x\n", rh.SHA512[:])

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
				ctx, eng, nil, key, endKey, ts, localTS, nil, nil, false, 0, nil))
		} else {
			require.NoError(t, storage.MVCCPut(ctx, eng, nil, key, ts, localTS, value, nil))
		}

		rh, err = repl.sha512(ctx, desc, eng, nil, roachpb.ChecksumMode_CHECK_FULL, lim)
		require.NoError(t, err)
		fmt.Fprintf(sb, "checksum%d: %x\n", i+1, rh.SHA512[:])
	}

	// Run another check to obtain a snapshot and stats for the final state.
	kvSnapshot := roachpb.RaftSnapshotData{}
	rh, err = repl.sha512(ctx, desc, eng, &kvSnapshot, roachpb.ChecksumMode_CHECK_FULL, lim)
	require.NoError(t, err)

	jsonpb := protoutil.JSONPb{Indent: "  "}
	json, err := jsonpb.Marshal(&rh.RecomputedMS)
	require.NoError(t, err)
	fmt.Fprintf(sb, "stats: %s\n", string(json))

	fmt.Fprint(sb, "snapshot:\n")
	for _, kv := range kvSnapshot.KV {
		fmt.Fprintf(sb, "  %s=%q\n", storage.MVCCKey{Key: kv.Key, Timestamp: kv.Timestamp}, kv.Value)
	}
	for _, rkv := range kvSnapshot.RangeKV {
		fmt.Fprintf(sb, "  %s=%q\n",
			storage.MVCCRangeKey{StartKey: rkv.StartKey, EndKey: rkv.EndKey, Timestamp: rkv.Timestamp},
			rkv.Value)
	}

	echotest.Require(t, sb.String(), testutils.TestDataPath(t, "replica_consistency_sha512"))
}
