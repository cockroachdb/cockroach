// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	filters "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/stretchr/testify/require"
)

func TestMemClustersRepo(t *testing.T) {
	ctx := context.Background()
	repo := NewClustersRepository()

	t.Run("Store and Get Clusters", func(t *testing.T) {
		clusters := cloudcluster.Clusters{
			"cluster1": &cloudcluster.Cluster{Name: "cluster1"},
			"cluster2": &cloudcluster.Cluster{Name: "cluster2"},
		}
		err := repo.StoreClusters(ctx, logger.DefaultLogger, clusters)
		require.NoError(t, err)

		storedClusters, err := repo.GetClusters(ctx, logger.DefaultLogger, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Equal(t, clusters, storedClusters)
	})

	t.Run("Store and Get Cluster", func(t *testing.T) {
		cluster := cloudcluster.Cluster{Name: "cluster1"}
		err := repo.StoreCluster(ctx, logger.DefaultLogger, cluster)
		require.NoError(t, err)

		storedCluster, err := repo.GetCluster(ctx, logger.DefaultLogger, "cluster1")
		require.NoError(t, err)
		require.Equal(t, cluster, storedCluster)
	})

	t.Run("Get Non-Existent Cluster", func(t *testing.T) {
		_, err := repo.GetCluster(ctx, logger.DefaultLogger, "nonexistent")
		require.Error(t, err)
		require.Equal(t, clusters.ErrClusterNotFound, err)
	})

	t.Run("Delete Cluster", func(t *testing.T) {
		cluster := cloudcluster.Cluster{Name: "cluster1"}
		err := repo.StoreCluster(ctx, logger.DefaultLogger, cluster)
		require.NoError(t, err)

		err = repo.DeleteCluster(ctx, logger.DefaultLogger, cluster)
		require.NoError(t, err)

		_, err = repo.GetCluster(ctx, logger.DefaultLogger, "cluster1")
		require.Error(t, err)
		require.Equal(t, clusters.ErrClusterNotFound, err)
	})
}
