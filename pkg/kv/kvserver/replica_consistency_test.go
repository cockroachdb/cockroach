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
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
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
	tc.repl.mu.checksums[id] = ReplicaChecksum{notify: notify}
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
	tc.repl.mu.checksums[id] = ReplicaChecksum{notify: make(chan struct{})}
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
	tc.repl.mu.checksums[id] = ReplicaChecksum{notify: make(chan struct{}), started: true}
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

// TestReplicaChecksumSHA512 checks that a given dataset produces the expected
// checksum.
func TestReplicaChecksumSHA512(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We incrementally add writes, and check the checksums after each write to
	// make sure they differ such that each write affects the checksum.
	testcases := []struct {
		key    string
		endKey string
		ts     int32
		value  string
		expect string
	}{
		// Start with a noop write to hash the initial state.
		{"", "", 0, "", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"},
		{"a", "", 1, "a1", "8186c3fada2f5ef9d8445e3741368f966c4ae8e81bdd6165cda6e4adf008a513c53ec3a880cc55fddfc8f5ecef4459a6863469950d0525055f7228322610c450"},
		{"a", "", 2, "", "3ca5540015a61abd6ab7925a1793ac2a88d5bb6d348024d098ebd4092e6898d015c072d0f57ed23853f84bec5ab9e6b972adb0a1a4aa5fcdaf587f3c631a3a27"},
		{"b", "", 3, "b3", "e9cf677e6542c086ebe1e6be5e3851bf6e85078ddd600f4f5f776e5309074f5d035286a02751d3f503a5bcfac10a16c1ee776260e75982f7a6fc3f8c249e29d2"},
		{"x", "", 0, "x0", "c04816395be7029b6f73fa757901edf7ba92db0b4de50bb70c1e50a61f346b9dfdc83574ea1d894c0a34ed4c922c7f20db1dcf9b848b614375f45123053a9f11"},
		// Range keys can currently only be tombstones.
		{"p", "q", 1, "", "9df7d8ae3769ed34c591e14da8c6e4147d29bbbffedd2fa603f8b7ddeda2c268b8d95414bf7c4b7cc455e781a64e0a76a1210604299119f59c3eb2568721bfe5"},
		{"x", "z", 9, "", "6a8ecd693b31dd84ccf403c6b8cc0e8c0a4447ea95ef73704906bf4557ec8914f4716362eafa9cdd84ce6819622758b82633198f92855839a0dcb7946b41cb13"},
	}

	expectSnapshot := &roachpb.RaftSnapshotData{
		KV: []roachpb.RaftSnapshotData_KeyValue{
			{Key: []byte("a"), Timestamp: hlc.Timestamp{Logical: 2}, Value: []byte{}},
			{Key: []byte("a"), Timestamp: hlc.Timestamp{Logical: 1}, Value: []byte("a1")},
			{Key: []byte("b"), Timestamp: hlc.Timestamp{Logical: 3}, Value: []byte("b3")},
			{Key: []byte("x"), Timestamp: hlc.Timestamp{}, Value: []byte("x0")},
		},
		RangeKV: []roachpb.RaftSnapshotData_RangeKeyValue{
			{StartKey: []byte("p"), EndKey: []byte("q"), Timestamp: hlc.Timestamp{Logical: 1}, Value: []byte{}},
			{StartKey: []byte("x"), EndKey: []byte("z"), Timestamp: hlc.Timestamp{Logical: 9}, Value: []byte{}},
		},
	}

	expectStats := enginepb.MVCCStats{
		LiveCount:     2,
		LiveBytes:     20,
		KeyCount:      3,
		KeyBytes:      42,
		ValCount:      3,
		ValBytes:      6,
		RangeKeyCount: 2,
		RangeKeyBytes: 34,
	}

	// Set up an engine and some other infrastructure.
	ctx := context.Background()
	lim := quotapool.NewRateLimiter("rate", 1e9, 0)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	repl := &Replica{} // We don't actually need the replica at all, just the method.
	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("z"),
	}

	for i, tc := range testcases {
		t.Run(fmt.Sprintf("checksum%d", i), func(t *testing.T) {
			key := roachpb.Key(tc.key)
			endKey := roachpb.Key(tc.endKey)
			ts := hlc.Timestamp{Logical: tc.ts}
			value := []byte(tc.value)

			if tc.key != "" { // we allow noop writes
				if tc.endKey != "" {
					require.NoError(t, eng.ExperimentalPutMVCCRangeKey(storage.MVCCRangeKey{
						StartKey: key, EndKey: endKey, Timestamp: ts}, value))
				} else if tc.ts == 0 {
					require.NoError(t, eng.PutUnversioned(key, value))
				} else {
					require.NoError(t, eng.PutMVCC(storage.MVCCKey{Key: key, Timestamp: ts}, value))
				}
			}

			rh, err := repl.sha512(ctx, desc, eng, nil, roachpb.ChecksumMode_CHECK_FULL, lim)
			require.NoError(t, err)
			require.Equal(t, tc.expect, hex.EncodeToString(rh.SHA512[:]))
		})
	}

	t.Run("stats", func(t *testing.T) {
		kvSnapshot := &roachpb.RaftSnapshotData{}
		rh, err := repl.sha512(ctx, desc, eng, kvSnapshot, roachpb.ChecksumMode_CHECK_FULL, lim)
		require.NoError(t, err)
		require.Equal(t, testcases[len(testcases)-1].expect, hex.EncodeToString(rh.SHA512[:]))
		require.Equal(t, expectStats, rh.RecomputedMS)
		require.Equal(t, expectSnapshot, kvSnapshot)
	})
}
