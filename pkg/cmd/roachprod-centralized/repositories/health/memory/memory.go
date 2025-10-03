// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	rhealth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MemHealthRepo is an in-memory implementation of the health repository.
type MemHealthRepo struct {
	mu        syncutil.Mutex
	instances map[string]*health.InstanceInfo // map[instanceID]*InstanceInfo
}

// NewHealthRepository creates a new in-memory health repository.
func NewHealthRepository() *MemHealthRepo {
	return &MemHealthRepo{
		instances: make(map[string]*health.InstanceInfo),
	}
}

// RegisterInstance registers or updates an instance.
func (r *MemHealthRepo) RegisterInstance(
	ctx context.Context, l *logger.Logger, instance health.InstanceInfo,
) error {
	// Create a copy to avoid mutation issues
	instanceCopy := health.InstanceInfo{
		InstanceID:    instance.InstanceID,
		Hostname:      instance.Hostname,
		Mode:          instance.Mode,
		StartedAt:     instance.StartedAt,
		LastHeartbeat: instance.LastHeartbeat,
		Metadata:      make(map[string]string),
	}

	// Deep copy metadata
	for k, v := range instance.Metadata {
		instanceCopy.Metadata[k] = v
	}

	r.mu.Lock()
	r.instances[instance.InstanceID] = &instanceCopy
	r.mu.Unlock()
	return nil
}

// UpdateHeartbeat updates the heartbeat timestamp for an instance.
func (r *MemHealthRepo) UpdateHeartbeat(
	ctx context.Context, l *logger.Logger, instanceID string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	instance, ok := r.instances[instanceID]
	if !ok {
		// Instance not found, ignore (could be first heartbeat before registration)
		return nil
	}

	// Create updated instance
	updated := *instance
	updated.LastHeartbeat = timeutil.Now()

	r.instances[instanceID] = &updated
	return nil
}

// GetInstance retrieves a specific instance by ID.
func (r *MemHealthRepo) GetInstance(
	ctx context.Context, l *logger.Logger, instanceID string,
) (*health.InstanceInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	instance, ok := r.instances[instanceID]
	if !ok {
		return nil, rhealth.ErrInstanceNotFound
	}

	// Return a copy to avoid mutation
	instanceCopy := health.InstanceInfo{
		InstanceID:    instance.InstanceID,
		Hostname:      instance.Hostname,
		Mode:          instance.Mode,
		StartedAt:     instance.StartedAt,
		LastHeartbeat: instance.LastHeartbeat,
		Metadata:      make(map[string]string),
	}

	for k, v := range instance.Metadata {
		instanceCopy.Metadata[k] = v
	}

	return &instanceCopy, nil
}

// IsInstanceHealthy checks if an instance is healthy within the given timeout.
func (r *MemHealthRepo) IsInstanceHealthy(
	ctx context.Context, l *logger.Logger, instanceID string, timeout time.Duration,
) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	instance, ok := r.instances[instanceID]
	if !ok {
		return false, nil
	}

	threshold := timeutil.Now().Add(-timeout)
	return instance.LastHeartbeat.After(threshold), nil
}

// GetHealthyInstances returns all healthy instances within the given timeout.
func (r *MemHealthRepo) GetHealthyInstances(
	ctx context.Context, l *logger.Logger, timeout time.Duration,
) ([]health.InstanceInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	threshold := timeutil.Now().Add(-timeout)
	var healthy []health.InstanceInfo

	for _, instance := range r.instances {
		if instance.LastHeartbeat.After(threshold) {
			// Create a copy
			instanceCopy := health.InstanceInfo{
				InstanceID:    instance.InstanceID,
				Hostname:      instance.Hostname,
				Mode:          instance.Mode,
				StartedAt:     instance.StartedAt,
				LastHeartbeat: instance.LastHeartbeat,
				Metadata:      make(map[string]string),
			}

			for k, v := range instance.Metadata {
				instanceCopy.Metadata[k] = v
			}

			healthy = append(healthy, instanceCopy)
		}
	}

	// Sort by last heartbeat descending
	slices.SortFunc(healthy, func(i, j health.InstanceInfo) int {
		return j.LastHeartbeat.Compare(i.LastHeartbeat)
	})

	return healthy, nil
}

// CleanupDeadInstances removes dead instances that are beyond the retention period.
func (r *MemHealthRepo) CleanupDeadInstances(
	ctx context.Context, l *logger.Logger, timeout time.Duration, retentionPeriod time.Duration,
) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	deadThreshold := timeutil.Now().Add(-timeout)
	retentionThreshold := timeutil.Now().Add(-retentionPeriod)

	var toDelete []string

	for instanceID, instance := range r.instances {
		isDead := instance.LastHeartbeat.Before(deadThreshold)
		beyondRetention := instance.LastHeartbeat.Before(retentionThreshold)

		if isDead && beyondRetention {
			toDelete = append(toDelete, instanceID)
		}
	}

	// Delete instances
	for _, instanceID := range toDelete {
		delete(r.instances, instanceID)
	}

	return len(toDelete), nil
}
