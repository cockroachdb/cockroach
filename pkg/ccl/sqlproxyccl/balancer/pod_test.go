// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package balancer

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSelectTenantPods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	t.Run("no pods", func(t *testing.T) {
		require.Nil(t, selectTenantPod(nil, newTenantEntry()))
	})

	t.Run("no entry", func(t *testing.T) {
		require.Nil(t, selectTenantPod([]*tenant.Pod{{TenantID: 10, Addr: "1"}}, nil))
	})

	t.Run("one pod", func(t *testing.T) {
		pod := selectTenantPod([]*tenant.Pod{{Addr: "1"}}, newTenantEntry())
		require.Equal(t, "1", pod.Addr)
	})

	t.Run("many pods", func(t *testing.T) {
		for _, update := range []bool{true, false} {
			t.Run(fmt.Sprintf("update=%v", update), func(t *testing.T) {
				// Counts: 0, 5, 9 for pods 1, 2, 3 respectively.
				entry := newTenantEntry()
				for i := 0; i < 5; i++ {
					entry.addAssignment(&ServerAssignment{addr: "2"})
				}
				for i := 0; i < 9; i++ {
					entry.addAssignment(&ServerAssignment{addr: "3"})
				}

				pods := []*tenant.Pod{
					{TenantID: 10, Addr: "1"},
					{TenantID: 10, Addr: "2"},
					{TenantID: 10, Addr: "3"},
				}

				distribution := map[string]int{}
				for i := 0; i < 10000; i++ {
					pod := selectTenantPod(pods, entry)
					if update {
						// Simulate the case where the entry gets updated after.
						entry.addAssignment(&ServerAssignment{addr: pod.Addr})
					}
					distribution[pod.Addr]++
				}

				if update {
					// Distribution should eventually converge to ~3333 each.
					require.Equal(t, map[string]int{
						"1": 3338,
						"2": 3333,
						"3": 3329,
					}, distribution)
				} else {
					// All requests get routed to the pod with the lowest
					// number of assignments, which is pod 1.
					require.Equal(t, map[string]int{
						"1": 10000,
					}, distribution)
				}
			})
		}
	})
}
