// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/stretchr/testify/require"
)

func TestMemClustersRepo(t *testing.T) {
	ctx := context.Background()
	repo := NewClustersRepository()

	t.Run("Store and Get Clusters", func(t *testing.T) {
		clusters := cloud.Clusters{
			"cluster1": &cloud.Cluster{Name: "cluster1"},
			"cluster2": &cloud.Cluster{Name: "cluster2"},
		}
		err := repo.StoreClusters(ctx, clusters)
		require.NoError(t, err)

		storedClusters, err := repo.GetClusters(ctx)
		require.NoError(t, err)
		require.Equal(t, clusters, storedClusters)
	})

	t.Run("Store and Get Cluster", func(t *testing.T) {
		cluster := cloud.Cluster{Name: "cluster1"}
		err := repo.StoreCluster(ctx, cluster)
		require.NoError(t, err)

		storedCluster, err := repo.GetCluster(ctx, "cluster1")
		require.NoError(t, err)
		require.Equal(t, cluster, storedCluster)
	})

	t.Run("Get Non-Existent Cluster", func(t *testing.T) {
		_, err := repo.GetCluster(ctx, "nonexistent")
		require.Error(t, err)
		require.Equal(t, clusters.ErrClusterNotFound, err)
	})

	t.Run("Delete Cluster", func(t *testing.T) {
		cluster := cloud.Cluster{Name: "cluster1"}
		err := repo.StoreCluster(ctx, cluster)
		require.NoError(t, err)

		err = repo.DeleteCluster(ctx, cluster)
		require.NoError(t, err)

		_, err = repo.GetCluster(ctx, "cluster1")
		require.Error(t, err)
		require.Equal(t, clusters.ErrClusterNotFound, err)
	})
}
