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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
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
		tc.repl.computeChecksumPostApply(ctx, cc)
		rc, err := getChecksum(ctx, &tc.repl.checksumStorage, cc.ChecksumID)
		if !matchingVersion {
			if !testutils.IsError(err, "no checksum found") {
				t.Fatal(err)
			}
			require.Nil(t, rc)
		} else {
			require.NoError(t, err)
			require.NotNil(t, rc.Checksum)
		}
	})
}

func TestChecksumStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	css := &checksumStorage{}

	id, err := uuid.NewV4()
	require.NoError(t, err)

	t0 := timeutil.Unix(0, 0)

	// Expected usage.
	t.Run("basic", func(t *testing.T) {
		c := css.GetOrInit(id, t0.Add(time.Minute))
		var canceled bool
		c.SignalStart(func() {
			canceled = true
		})
		require.NoError(t, c.WaitStarted(ctx, time.Nanosecond))
		hash := &replicaHash{PersistedMS: enginepb.MVCCStats{AbortSpanBytes: 123}}
		raftData := &roachpb.RaftSnapshotData{}
		c.SignalResult(hash, raftData)
		ccr, err := c.WaitResult(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 123, ccr.Persisted.AbortSpanBytes)
		require.Equal(t, raftData, ccr.Snapshot)
		require.True(t, css.Delete(id))
		require.True(t, canceled)
	})

	// Context deadline respected in getChecksum.
	t.Run("deadline-getchecksum", func(t *testing.T) {
		c := css.GetOrInit(id, timeutil.Now().Add(24*time.Hour))
		require.NotNil(t, c)
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
		ccr, err := getChecksum(ctx, css, id)
		// We don't verify the error because it could either be Canceled
		// or DeadlineExceeded.
		t.Log(err)
		cancel()
		require.Error(t, err)
		require.Nil(t, ccr)
		require.False(t, css.Delete(id)) // getChecksum deleted it
	})

	// Deadline on the computation itself is observed by GC.
	t.Run("deadline-gc", func(t *testing.T) {
		tBegin := timeutil.Now()
		c := css.GetOrInit(id, tBegin.Add(time.Millisecond))
		require.NotNil(t, c)
		css.GC(tBegin.Add(2 * time.Millisecond))
		require.False(t, css.Delete(id)) // GC deleted it
	})

	// Ongoing computation is canceled on deletion.
	t.Run("delete-cancels", func(t *testing.T) {
		c := css.GetOrInit(id, timeutil.Now().Add(24*time.Hour))
		require.NotNil(t, c)
		var canceled bool
		c.SignalStart(func() {
			canceled = true
		})
		require.True(t, css.Delete(id))
		require.True(t, canceled)
	})
}
