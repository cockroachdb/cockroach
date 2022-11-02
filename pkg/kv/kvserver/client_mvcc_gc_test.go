// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestMVCCGCCorrectStats verifies that the mvcc gc queue corrects stats
// for a range that has bad ones that would unnecessarily trigger the mvcc
// gc queue.
func TestMVCCGCCorrectStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var args base.TestServerArgs
	args.Knobs.Store = &kvserver.StoreTestingKnobs{DisableCanAckBeforeApplication: true}
	serv, _, _ := serverutils.StartServer(t, args)
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	repl := store.LookupReplica(roachpb.RKey(key))
	for i := 0; i < 10; i++ {
		if err := store.DB().Put(ctx, key, "foo"); err != nil {
			t.Fatal(err)
		}
		key = key.Next()
	}

	// Put some garbage in the stats, so it triggers the mvcc gc queue.
	ms := repl.GetMVCCStats()
	oldKeyBytes := ms.KeyBytes
	oldValBytes := ms.ValBytes
	ms.KeyBytes = 16 * (1 << 20) // 16mb
	ms.ValBytes = 32 * (1 << 20) // 16mb
	ms.GCBytesAge = 48 * (1 << 20) * 100 * int64(time.Hour.Seconds())

	repl.SetMVCCStatsForTesting(&ms)
	require.NoError(t, store.ManualMVCCGC(repl))

	// Verify that the mvcc gc queue restored the stats.
	newStats := repl.GetMVCCStats()
	require.Equal(t, oldKeyBytes, newStats.KeyBytes)
	require.Equal(t, oldValBytes, newStats.ValBytes)
}
