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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
		rc, err := tc.repl.getChecksum(ctx, cc.ChecksumID)
		if !matchingVersion {
			if !testutils.IsError(err, "no checksum found") {
				t.Fatal(err)
			}
			require.Nil(t, rc.Checksum)
		} else {
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

	id := uuid.FastMakeV4()
	notify := make(chan struct{})
	close(notify)

	// Simple condition, the checksum is notified, but not computed.
	tc.repl.mu.Lock()
	tc.repl.mu.checksums[id] = replicaChecksum{notify: notify}
	tc.repl.mu.Unlock()
	rc, err := tc.repl.getChecksum(ctx, id)
	if !testutils.IsError(err, "no checksum found") {
		t.Fatal(err)
	}
	require.Nil(t, rc.Checksum)
	// Next condition, the initial wait expires and checksum is not started,
	// this will take 10ms.
	id = uuid.FastMakeV4()
	tc.repl.mu.Lock()
	tc.repl.mu.checksums[id] = replicaChecksum{notify: make(chan struct{})}
	tc.repl.mu.Unlock()
	rc, err = tc.repl.getChecksum(ctx, id)
	if !testutils.IsError(err, "checksum computation did not start") {
		t.Fatal(err)
	}
	require.Nil(t, rc.Checksum)
	// Next condition, initial wait expired and we found the started flag,
	// so next step is for context deadline.
	id = uuid.FastMakeV4()
	tc.repl.mu.Lock()
	tc.repl.mu.checksums[id] = replicaChecksum{notify: make(chan struct{}), started: true}
	tc.repl.mu.Unlock()
	rc, err = tc.repl.getChecksum(ctx, id)
	if !testutils.IsError(err, "context deadline exceeded") {
		t.Fatal(err)
	}
	require.Nil(t, rc.Checksum)

	// Need to reset the context, since we deadlined it above.
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// Next condition, node should quiesce.
	tc.repl.store.Stopper().Quiesce(ctx)
	rc, err = tc.repl.getChecksum(ctx, uuid.FastMakeV4())
	if !testutils.IsError(err, "store quiescing") {
		t.Fatal(err)
	}
	require.Nil(t, rc.Checksum)
}
