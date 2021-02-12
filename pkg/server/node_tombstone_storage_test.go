// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestNodeTombstoneStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	eng1 := storage.NewDefaultInMemForTesting()
	defer eng1.Close()
	eng2 := storage.NewDefaultInMemForTesting()
	defer eng2.Close()

	engs := []storage.Engine{eng1, eng2}

	// The tombstone storage only writes to initialized engines.
	// We'll test uninited engines at the end of the test.
	id, err := uuid.NewV4()
	require.NoError(t, err)
	for i := range engs {
		require.NoError(t, kvserver.WriteClusterVersion(ctx, engs[i], clusterversion.TestingClusterVersion))
		require.NoError(t, kvserver.InitEngine(ctx, engs[i], roachpb.StoreIdent{
			ClusterID: id,
			NodeID:    1,
			StoreID:   roachpb.StoreID(1 + i),
		}))
	}

	mustTime := func(ts time.Time, err error) time.Time {
		t.Helper()
		require.NoError(t, err)
		return ts
	}

	s := &nodeTombstoneStorage{engs: []storage.Engine{eng1, eng2}}
	// Empty storage has nobody decommissioned.
	require.Equal(t, time.Time{}, mustTime(s.IsDecommissioned(ctx, 1)))

	// Decommission n2 at ts1.
	ts1 := timeutil.Unix(10, 0).UTC()
	require.NoError(t, s.SetDecommissioned(ctx, 2, ts1))
	// n1 is still active.
	require.Equal(t, time.Time{}, mustTime(s.IsDecommissioned(ctx, 1)))
	// n2 is decommissioned.
	require.Equal(t, ts1, mustTime(s.IsDecommissioned(ctx, 2)))
	// Decommission n2 again, at older timestamp.
	require.NoError(t, s.SetDecommissioned(ctx, 2, ts1.Add(-time.Second)))
	// n2 is still decommissioned at ts1.
	require.Equal(t, ts1, mustTime(s.IsDecommissioned(ctx, 2)))
	// Decommission n2 again, at newer timestamp.
	require.NoError(t, s.SetDecommissioned(ctx, 2, ts1.Add(time.Second)))
	// n2 is still decommissioned at ts1.
	require.Equal(t, ts1, mustTime(s.IsDecommissioned(ctx, 2)))

	// Also decommission n1.
	ts2 := timeutil.Unix(5, 0).UTC()
	require.NoError(t, s.SetDecommissioned(ctx, 1, ts2))
	// n1 is decommissioned at ts2.
	require.Equal(t, ts2, mustTime(s.IsDecommissioned(ctx, 1)))

	// n3 is not decommissioned.
	require.Equal(t, time.Time{}, mustTime(s.IsDecommissioned(ctx, 3)))

	// We're not hitting the disks any more; the decommissioned
	// status is cached. This includes both the decommissioned nodes
	// and n3, which is not decommissioned but was checked above.
	s.engs = nil
	require.Equal(t, ts1, mustTime(s.IsDecommissioned(ctx, 2)))
	require.Equal(t, ts2, mustTime(s.IsDecommissioned(ctx, 1)))
	require.Equal(t, time.Time{}, mustTime(s.IsDecommissioned(ctx, 3)))

	// If we recreate the cache, it rehydrates from disk.
	s = &nodeTombstoneStorage{engs: engs}
	require.Equal(t, ts1, mustTime(s.IsDecommissioned(ctx, 2)))
	require.Equal(t, ts2, mustTime(s.IsDecommissioned(ctx, 1)))
	require.Equal(t, time.Time{}, mustTime(s.IsDecommissioned(ctx, 3)))

	// Throw an uninitialized engine in the mix. It should be skipped over.
	eng3 := storage.NewDefaultInMemForTesting()
	defer eng3.Close()
	s = &nodeTombstoneStorage{engs: []storage.Engine{eng1, eng2, eng3}}
	// Decommission n100.
	ts3 := timeutil.Unix(15, 30).UTC()
	require.NoError(t, s.SetDecommissioned(ctx, 100, ts3))
	require.Equal(t, ts3, mustTime(s.IsDecommissioned(ctx, 100)))
	// Rehydrate.
	s = &nodeTombstoneStorage{engs: []storage.Engine{eng1, eng2, eng3}}
	require.Equal(t, ts3, mustTime(s.IsDecommissioned(ctx, 100)))
	// Rehydrate, but only from eng3. Now the entry is gone, meaning it
	// wasn't written to n3.
	s = &nodeTombstoneStorage{engs: []storage.Engine{eng3}}
	require.Equal(t, time.Time{}, mustTime(s.IsDecommissioned(ctx, 100)))
}
