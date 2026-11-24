// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCollectChecksStoreInfo(t *testing.T) {
	leaktest.AfterTest(t)
	ctx := context.Background()

	eng, err := storage.Open(ctx,
		storage.InMemory(),
		cluster.MakeClusterSettings(),
		storage.CacheSize(1<<20 /* 1 MiB */))
	require.NoError(t, err, "failed to create engine")
	engines := kvstorage.MakeEngines(eng)
	defer engines.Close()

	require.NoError(t, kvstorage.WriteClusterVersionToEngines(
		[]kvstorage.Engines{engines},
		clusterversion.ClusterVersion{Version: roachpb.Version{Major: 21, Minor: 2}}),
	)

	_, _, err = CollectStoresReplicaInfo(ctx, []kvstorage.Engines{engines})
	require.ErrorContains(t, err, "is too old for running version",
		"engine version check not triggered")
}
