// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func createSomePlan(rangeID int) loqrecoverypb.ReplicaUpdatePlan {
	planID := uuid.FastMakeV4()
	return loqrecoverypb.ReplicaUpdatePlan{
		PlanID: &planID,
		Updates: []loqrecoverypb.ReplicaUpdate{
			{
				RangeID: roachpb.RangeID(rangeID),
			},
		},
	}
}

func TestWritePlan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runDiskAndInMem(t, func(t *testing.T, base string, fs vfs.FS) {
		ps := NewPlanStore(base, fs)
		plan := createSomePlan(7)
		require.NoError(t, ps.SavePlan(plan), "failed creating plan in empty store")

		storePlan, err := ps.HasPlan()
		require.NoError(t, err, "plan check failed")
		require.False(t, storePlan.Empty(), "plan must not be empty")

		readPlan, err := ps.LoadPlan(storePlan)
		require.NoError(t, err, "load plan failed")

		require.EqualValues(t, plan, readPlan, "saved are loaded plans")
	})
}

func TestNoPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runDiskAndInMem(t, func(t *testing.T, base string, fs vfs.FS) {
		ps := NewPlanStore(base, fs)
		storePlan, err := ps.HasPlan()
		require.NoError(t, err, "plan check failed")
		require.True(t, storePlan.Empty(), "plan must be empty")
	})
}

func TestOverwritePlan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runDiskAndInMem(t, func(t *testing.T, base string, fs vfs.FS) {
		ps := NewPlanStore(base, fs)
		plan := createSomePlan(7)
		require.NoError(t, ps.SavePlan(plan), "failed creating plan in empty store")

		plan2 := createSomePlan(13)
		require.ErrorContains(t, ps.SavePlan(plan2), plan.PlanID.String(),
			"must fail overwriting plan with old plan id in message")

		// Ensure that stored file was not corrupt.
		storePlan, err := ps.HasPlan()
		require.NoError(t, err, "plan check failed")
		require.False(t, storePlan.Empty(), "plan must not be empty")

		readPlan, err := ps.LoadPlan(storePlan)
		require.NoError(t, err, "load plan failed")

		require.EqualValues(t, plan, readPlan, "saved are loaded plans")
	})
}

func TestFailWhenMultiplePlans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runDiskAndInMem(t, func(t *testing.T, base string, fs vfs.FS) {
		ps := NewPlanStore(base, fs)
		plan := createSomePlan(13)
		require.NoError(t, ps.SavePlan(plan), "failed creating plan in empty store")

		fakeID := uuid.FastMakeV4()
		f, err := fs.Create(filepath.Join(ps.path, fakeID.String()+".json"))
		require.NoError(t, err, "failed to create fake plan file")
		_ = f.Close()

		_, err = ps.HasPlan()
		require.Error(t, err, "must fail when more than one matching file")
	})
}

func TestRemovePlan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runDiskAndInMem(t, func(t *testing.T, base string, fs vfs.FS) {
		ps := NewPlanStore(base, fs)
		plan := createSomePlan(7)
		require.NoError(t, ps.SavePlan(plan), "failed creating plan in empty store")

		require.NoError(t, ps.RemovePlans(), "failed removing plans")

		storePlan, err := ps.HasPlan()
		require.NoError(t, err, "plan check failed")
		require.True(t, storePlan.Empty(), "plan must be empty after clear")
	})
}

func TestRemoveEmptyPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runDiskAndInMem(t, func(t *testing.T, base string, fs vfs.FS) {
		ps := NewPlanStore(base, fs)
		plan, err := ps.HasPlan()
		require.NoError(t, err, "check plan existence failed")
		require.True(t, plan.Empty(), "plan must be empty")

		require.NoError(t, ps.RemovePlans(), "failed removing plans")
	})
}

func TestRemoveMultiple(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runDiskAndInMem(t, func(t *testing.T, base string, fs vfs.FS) {
		ps := NewPlanStore(base, fs)
		plan := createSomePlan(13)
		require.NoError(t, ps.SavePlan(plan), "failed creating plan in empty store")

		fakeID := uuid.FastMakeV4()
		f, err := fs.Create(filepath.Join(ps.path, fakeID.String()+".json"))
		require.NoError(t, err, "failed to create fake plan file")
		_ = f.Close()

		require.NoError(t, ps.RemovePlans(), "failed removing plans")
		planHandle, err := ps.HasPlan()
		require.NoError(t, err, "plan check failed with valid plan")
		require.True(t, planHandle.Empty(), "all files must be cleaned by RemovePlans")
	})
}

func runDiskAndInMem(t *testing.T, test func(*testing.T, string, vfs.FS)) {
	t.Helper()
	t.Run("disk", func(t *testing.T) {
		storeDir, err := os.MkdirTemp(".", "*")
		require.NoError(t, err, "failed to create temp dir for test")
		defer func() { _ = os.RemoveAll(storeDir) }()

		test(t, storeDir, vfs.Default)
	})
	t.Run("mem", func(t *testing.T) {
		test(t, "", vfs.NewMem())
	})
}
