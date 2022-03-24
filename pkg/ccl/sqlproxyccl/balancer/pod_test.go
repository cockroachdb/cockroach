// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSelectTenantPods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("no pods", func(t *testing.T) {
		require.Nil(t, selectTenantPod(0, nil))
	})

	t.Run("one pod", func(t *testing.T) {
		pod := selectTenantPod(0, []*tenant.Pod{{Addr: "1"}})
		require.Equal(t, "1", pod.Addr)
	})

	t.Run("many pods", func(t *testing.T) {
		pods := []*tenant.Pod{
			{Addr: "1", Load: 0.0},
			{Addr: "2", Load: 0.5},
			{Addr: "3", Load: 0.9},
		}

		distribution := map[string]int{}
		rng := rand.New(rand.NewSource(0))

		for i := 0; i < 10000; i++ {
			pod := selectTenantPod(rng.Float32(), pods)
			distribution[pod.Addr]++
		}

		// Assert that the distribution is a roughly function of 1 - Load.
		require.Equal(t, map[string]int{
			"1": 6121,
			"2": 3214,
			"3": 665,
		}, distribution)
	})
}
