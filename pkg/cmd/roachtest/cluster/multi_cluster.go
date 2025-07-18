// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

// multiClusterContextKey is the key used to store MultiCluster in context
const multiClusterContextKey contextKey = "multiCluster"

// MultiCluster provides an interface for managing multiple clusters in a single test.
type MultiCluster interface {
	// Cluster access
	GetCluster(name string) Cluster
	GetClusters() map[string]Cluster
	GetClusterNames() []string

	// Cluster management
	AddCluster(name string, cluster Cluster) error

	// Lifecycle management
	DestroyAll(ctx context.Context) error
}

// multiClusterImpl implements the MultiCluster interface.
type multiClusterImpl struct {
	clusters map[string]Cluster
	t        test.Test
	logger   *logger.Logger
	mu       sync.RWMutex
}

// NewMultiCluster creates a new MultiCluster instance.
func NewMultiCluster(t test.Test, logger *logger.Logger) MultiCluster {
	return &multiClusterImpl{
		clusters: make(map[string]Cluster),
		t:        t,
		logger:   logger,
	}
}

// GetCluster returns the cluster with the given name, or nil if not found.
func (mc *multiClusterImpl) GetCluster(name string) Cluster {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.clusters[name]
}

// GetClusters returns a copy of all clusters.
func (mc *multiClusterImpl) GetClusters() map[string]Cluster {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	result := make(map[string]Cluster)
	for k, v := range mc.clusters {
		result[k] = v
	}
	return result
}

// GetClusterNames returns a list of all cluster names.
func (mc *multiClusterImpl) GetClusterNames() []string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	names := make([]string, 0, len(mc.clusters))
	for name := range mc.clusters {
		names = append(names, name)
	}
	return names
}

// AddCluster adds a cluster with the given name.
func (mc *multiClusterImpl) AddCluster(name string, cluster Cluster) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if name == "" {
		return fmt.Errorf("cluster name cannot be empty")
	}

	if _, exists := mc.clusters[name]; exists {
		return fmt.Errorf("cluster with name %s already exists", name)
	}

	if cluster == nil {
		return fmt.Errorf("cluster cannot be nil")
	}

	mc.clusters[name] = cluster
	return nil
}

// DestroyAll destroys all clusters concurrently.
func (mc *multiClusterImpl) DestroyAll(ctx context.Context) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if len(mc.clusters) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errorsCh := make(chan error, len(mc.clusters))

	for name, cluster := range mc.clusters {
		wg.Add(1)
		mc.t.Go(func(ctx context.Context, l *logger.Logger) error {
			defer wg.Done()
			if err := cluster.WipeE(ctx, mc.logger); err != nil {
				errorsCh <- errors.Wrapf(err, "failed to wipe cluster %s", name)
			} else {
				mc.logger.Printf("Successfully wiped cluster %s", name)
			}
			return nil
		}, task.Name(fmt.Sprintf("wipe-cluster-%s", name)))
	}

	wg.Wait()
	close(errorsCh)

	var errors []error
	for err := range errorsCh {
		errors = append(errors, err)
	}

	// Clear the clusters map
	mc.clusters = make(map[string]Cluster)

	if len(errors) > 0 {
		return fmt.Errorf("multiple destroy errors: %v", errors)
	}
	return nil
}

// WithMultiCluster stores a MultiCluster instance in the context
func WithMultiCluster(ctx context.Context, mc MultiCluster) context.Context {
	return context.WithValue(ctx, multiClusterContextKey, mc)
}

// GetMultiClusterFromContext retrieves a MultiCluster instance from the context
// Returns nil if no MultiCluster is found in the context
func GetMultiClusterFromContext(ctx context.Context) MultiCluster {
	if mc, ok := ctx.Value(multiClusterContextKey).(MultiCluster); ok {
		return mc
	}
	return nil
}

