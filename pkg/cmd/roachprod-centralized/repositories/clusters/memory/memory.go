// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// MemClustersRepo is a memory implementation of clusters repository.
type MemClustersRepo struct {
	clusters cloud.Clusters
	lock     syncutil.Mutex
}

// NewClustersRepository creates a new memory clusters repository.
func NewClustersRepository() *MemClustersRepo {
	return &MemClustersRepo{
		clusters: make(cloud.Clusters),
	}
}

// GetClusters returns all clusters.
func (s *MemClustersRepo) GetClusters(ctx context.Context) (cloud.Clusters, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.clusters, nil
}

// GetCluster returns a cluster by name.
func (s *MemClustersRepo) GetCluster(ctx context.Context, name string) (cloud.Cluster, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if c, ok := s.clusters[name]; !ok {
		return cloud.Cluster{}, clusters.ErrClusterNotFound
	} else {
		return *c, nil
	}
}

// StoreClusters stores all clusters.
func (s *MemClustersRepo) StoreClusters(ctx context.Context, clusters cloud.Clusters) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.clusters = clusters
	return nil
}

// StoreCluster stores a cluster.
func (s *MemClustersRepo) StoreCluster(ctx context.Context, cluster cloud.Cluster) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.clusters[cluster.Name] = &cluster
	return nil
}

// DeleteCluster deletes a cluster.
func (s *MemClustersRepo) DeleteCluster(ctx context.Context, cluster cloud.Cluster) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.clusters, cluster.Name)
	return nil
}
