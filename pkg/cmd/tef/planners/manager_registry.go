// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package planners

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Global registry of managers for child task execution.
// This is automatically populated when managers are created via NewPlannerManager.
var (
	managersRegistry     map[string]PlannerManager
	managersRegistryLock syncutil.RWMutex
)

// RegisterManager adds a manager to the global registry for child task execution.
// This is called automatically by NewPlannerManager.
func RegisterManager(planName string, manager PlannerManager) {
	managersRegistryLock.Lock()
	defer managersRegistryLock.Unlock()

	if managersRegistry == nil {
		managersRegistry = make(map[string]PlannerManager)
	}

	managersRegistry[planName] = manager
}

// GetManagerForPlan retrieves a manager for the specified plan ID from the global registry.
// Returns an error if the registry hasn't been initialized or the plan ID is not found.
func GetManagerForPlan(planeName string) (PlannerManager, error) {
	managersRegistryLock.RLock()
	defer managersRegistryLock.RUnlock()

	if managersRegistry == nil {
		return nil, errors.Newf("managers registry not initialized")
	}

	manager, ok := managersRegistry[planeName]
	if !ok {
		return nil, errors.Newf("no manager found for plan: %s", planeName)
	}

	return manager, nil
}

// GetAllManagers returns a copy of the entire managers' registry.
// This is useful for components that need access to all managers.
func GetAllManagers() map[string]PlannerManager {
	managersRegistryLock.RLock()
	defer managersRegistryLock.RUnlock()

	// Return a copy to prevent external modification
	managers := make(map[string]PlannerManager, len(managersRegistry))
	for k, v := range managersRegistry {
		managers[k] = v
	}
	return managers
}
