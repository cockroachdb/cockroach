// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package planners

import (
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// resetRegistry clears the global registry for test isolation.
func resetRegistry() {
	managersRegistryLock.Lock()
	defer managersRegistryLock.Unlock()
	managersRegistry = nil
}

func TestRegisterManager(t *testing.T) {
	defer resetRegistry()

	t.Run("registers manager successfully", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		manager := NewMockPlannerManager(ctrl)
		RegisterManager("test-plan", manager)

		// Verify registration
		retrieved, err := GetManagerForPlan("test-plan")
		require.NoError(t, err)
		require.Equal(t, manager, retrieved)
	})

	t.Run("registers multiple managers", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		manager1 := NewMockPlannerManager(ctrl)
		manager2 := NewMockPlannerManager(ctrl)
		manager3 := NewMockPlannerManager(ctrl)

		RegisterManager("plan1", manager1)
		RegisterManager("plan2", manager2)
		RegisterManager("plan3", manager3)

		// Verify all are registered
		retrieved1, err := GetManagerForPlan("plan1")
		require.NoError(t, err)
		require.Equal(t, manager1, retrieved1)

		retrieved2, err := GetManagerForPlan("plan2")
		require.NoError(t, err)
		require.Equal(t, manager2, retrieved2)

		retrieved3, err := GetManagerForPlan("plan3")
		require.NoError(t, err)
		require.Equal(t, manager3, retrieved3)
	})

	t.Run("initializes registry on first registration", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Ensure registry is nil
		managersRegistryLock.RLock()
		require.Nil(t, managersRegistry)
		managersRegistryLock.RUnlock()

		// Register manager
		manager := NewMockPlannerManager(ctrl)
		RegisterManager("init-plan", manager)

		// Verify registry is now initialized
		managersRegistryLock.RLock()
		require.NotNil(t, managersRegistry)
		require.Len(t, managersRegistry, 1)
		managersRegistryLock.RUnlock()
	})

	t.Run("overwrites existing manager for same plan", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		manager1 := NewMockPlannerManager(ctrl)
		manager2 := NewMockPlannerManager(ctrl)

		RegisterManager("plan", manager1)
		RegisterManager("plan", manager2)

		// Should retrieve the second manager
		retrieved, err := GetManagerForPlan("plan")
		require.NoError(t, err)
		require.Equal(t, manager2, retrieved)
	})

	t.Run("is thread-safe with concurrent registrations", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		const numGoroutines = 50
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(idx int) {
				defer wg.Done()
				manager := NewMockPlannerManager(ctrl)
				RegisterManager("plan", manager)
			}(i)
		}

		wg.Wait()

		// Verify registry has one entry
		retrieved, err := GetManagerForPlan("plan")
		require.NoError(t, err)
		require.NotNil(t, retrieved)
	})
}

func TestGetManagerForPlan(t *testing.T) {
	defer resetRegistry()

	t.Run("retrieves registered manager", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		manager := NewMockPlannerManager(ctrl)
		RegisterManager("test-plan", manager)

		retrieved, err := GetManagerForPlan("test-plan")
		require.NoError(t, err)
		require.Equal(t, manager, retrieved)
	})

	t.Run("returns error for uninitialized registry", func(t *testing.T) {
		resetRegistry()

		// Don't register anything
		_, err := GetManagerForPlan("nonexistent")
		require.Error(t, err)
		require.Contains(t, err.Error(), "managers registry not initialized")
	})

	t.Run("returns error for non-existent plan", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Register one manager
		manager := NewMockPlannerManager(ctrl)
		RegisterManager("existing-plan", manager)

		// Try to get a different plan
		_, err := GetManagerForPlan("nonexistent-plan")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no manager found for plan: nonexistent-plan")
	})

	t.Run("is thread-safe with concurrent reads", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		manager := NewMockPlannerManager(ctrl)
		RegisterManager("plan", manager)

		const numGoroutines = 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				retrieved, err := GetManagerForPlan("plan")
				require.NoError(t, err)
				require.Equal(t, manager, retrieved)
			}()
		}

		wg.Wait()
	})

	t.Run("handles concurrent reads and writes", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		const numReaders = 50
		const numWriters = 10
		var wg sync.WaitGroup
		wg.Add(numReaders + numWriters)

		// Initial registration
		initialManager := NewMockPlannerManager(ctrl)
		RegisterManager("concurrent-plan", initialManager)

		// Concurrent readers
		for i := 0; i < numReaders; i++ {
			go func() {
				defer wg.Done()
				_, _ = GetManagerForPlan("concurrent-plan")
			}()
		}

		// Concurrent writers
		for i := 0; i < numWriters; i++ {
			go func(idx int) {
				defer wg.Done()
				manager := NewMockPlannerManager(ctrl)
				RegisterManager("concurrent-plan", manager)
			}(i)
		}

		wg.Wait()

		// Verify registry is still functional
		retrieved, err := GetManagerForPlan("concurrent-plan")
		require.NoError(t, err)
		require.NotNil(t, retrieved)
	})
}

func TestGetAllManagers(t *testing.T) {
	defer resetRegistry()

	t.Run("returns all registered managers", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		manager1 := NewMockPlannerManager(ctrl)
		manager2 := NewMockPlannerManager(ctrl)
		manager3 := NewMockPlannerManager(ctrl)

		RegisterManager("plan1", manager1)
		RegisterManager("plan2", manager2)
		RegisterManager("plan3", manager3)

		allManagers := GetAllManagers()
		require.Len(t, allManagers, 3)
		require.Equal(t, manager1, allManagers["plan1"])
		require.Equal(t, manager2, allManagers["plan2"])
		require.Equal(t, manager3, allManagers["plan3"])
	})

	t.Run("returns empty map for uninitialized registry", func(t *testing.T) {
		resetRegistry()

		allManagers := GetAllManagers()
		require.NotNil(t, allManagers)
		require.Empty(t, allManagers)
	})

	t.Run("returns copy that doesn't affect original registry", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		manager1 := NewMockPlannerManager(ctrl)
		manager2 := NewMockPlannerManager(ctrl)

		RegisterManager("plan1", manager1)
		RegisterManager("plan2", manager2)

		// Get copy
		copy := GetAllManagers()
		require.Len(t, copy, 2)

		// Modify the copy
		copy["plan3"] = NewMockPlannerManager(ctrl)
		delete(copy, "plan1")

		// Verify original registry unchanged
		allManagers := GetAllManagers()
		require.Len(t, allManagers, 2)
		require.Equal(t, manager1, allManagers["plan1"])
		require.Equal(t, manager2, allManagers["plan2"])
		require.NotContains(t, allManagers, "plan3")
	})

	t.Run("is thread-safe with concurrent access", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Register initial managers
		for i := 0; i < 10; i++ {
			manager := NewMockPlannerManager(ctrl)
			RegisterManager("plan", manager)
		}

		const numGoroutines = 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				allManagers := GetAllManagers()
				require.NotNil(t, allManagers)
			}()
		}

		wg.Wait()
	})

	t.Run("handles concurrent GetAllManagers and RegisterManager", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		const numGetters = 50
		const numRegisterers = 20
		var wg sync.WaitGroup
		wg.Add(numGetters + numRegisterers)

		// Initial registration
		initialManager := NewMockPlannerManager(ctrl)
		RegisterManager("initial-plan", initialManager)

		// Concurrent GetAllManagers calls
		for i := 0; i < numGetters; i++ {
			go func() {
				defer wg.Done()
				allManagers := GetAllManagers()
				require.NotNil(t, allManagers)
			}()
		}

		// Concurrent RegisterManager calls
		for i := 0; i < numRegisterers; i++ {
			go func(idx int) {
				defer wg.Done()
				manager := NewMockPlannerManager(ctrl)
				RegisterManager("plan", manager)
			}(i)
		}

		wg.Wait()

		// Verify registry is still functional
		allManagers := GetAllManagers()
		require.NotNil(t, allManagers)
		require.NotEmpty(t, allManagers)
	})
}

func TestManagerRegistry_Integration(t *testing.T) {
	defer resetRegistry()

	t.Run("complete workflow with multiple operations", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Register multiple managers
		var lastManager PlannerManager
		for i := 0; i < 5; i++ {
			lastManager = NewMockPlannerManager(ctrl)
			RegisterManager("plan", lastManager)
		}

		// Get individual manager
		retrieved, err := GetManagerForPlan("plan")
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		// Get all managers
		allManagers := GetAllManagers()
		require.Len(t, allManagers, 1)

		// Verify operations don't interfere with each other
		retrieved2, err := GetManagerForPlan("plan")
		require.NoError(t, err)
		require.Equal(t, retrieved, retrieved2)
	})

	t.Run("registry survives concurrent stress test", func(t *testing.T) {
		resetRegistry()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		const numOperations = 1000
		var wg sync.WaitGroup
		wg.Add(numOperations)

		for i := 0; i < numOperations; i++ {
			go func(idx int) {
				defer wg.Done()

				// Mix of operations
				switch idx % 3 {
				case 0:
					manager := NewMockPlannerManager(ctrl)
					RegisterManager("stress-plan", manager)
				case 1:
					_, _ = GetManagerForPlan("stress-plan")
				case 2:
					_ = GetAllManagers()
				}
			}(i)
		}

		wg.Wait()

		// Verify registry is still functional and consistent
		allManagers := GetAllManagers()
		require.NotNil(t, allManagers)
	})
}
