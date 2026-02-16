// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide tests for PlanRegistry functionality.
package planners

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestPlanRegistryGetRegistries(t *testing.T) {
	t.Run("returns empty slice for new registry", func(t *testing.T) {
		planRegistry := NewPlanRegistry()
		registries := planRegistry.GetRegistries()

		require.NotNil(t, registries)
		require.Len(t, registries, 0)
	})

	t.Run("returns all registered registries", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		planRegistry := NewPlanRegistry()
		mockReg1 := NewMockRegistry(ctrl)
		mockReg1.EXPECT().GetPlanName().Return("plan1").AnyTimes()
		mockReg1.EXPECT().GetPlanDescription().Return("desc1").AnyTimes()

		mockReg2 := NewMockRegistry(ctrl)
		mockReg2.EXPECT().GetPlanName().Return("plan2").AnyTimes()
		mockReg2.EXPECT().GetPlanDescription().Return("desc2").AnyTimes()

		planRegistry.Register(mockReg1, mockReg2)
		registries := planRegistry.GetRegistries()

		require.Len(t, registries, 2)
		require.Equal(t, "plan1", registries[0].GetPlanName())
		require.Equal(t, "desc1", registries[0].GetPlanDescription())
		require.Equal(t, "plan2", registries[1].GetPlanName())
		require.Equal(t, "desc2", registries[1].GetPlanDescription())
	})

	t.Run("returns registries in registration order", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		planRegistry := NewPlanRegistry()
		regs := make([]Registry, 5)
		for i := 0; i < 5; i++ {
			mockReg := NewMockRegistry(ctrl)
			mockReg.EXPECT().GetPlanName().Return(string(rune('a' + i))).AnyTimes()
			regs[i] = mockReg
			planRegistry.Register(mockReg)
		}

		retrieved := planRegistry.GetRegistries()
		require.Len(t, retrieved, 5)
		for i, reg := range retrieved {
			require.Equal(t, regs[i], reg)
		}
	})
}
