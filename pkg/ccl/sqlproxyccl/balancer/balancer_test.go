// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBalancer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := balancer.NewBalancer()
	b.TestingKnobs.SelectTenantPod = func(rand float32, pods []*tenant.Pod) *tenant.Pod {
		if len(pods) == 0 {
			return nil
		}
		// Always return the first one for deterministic test results.
		return pods[0]
	}

	t.Run("no pods", func(t *testing.T) {
		pod, err := b.SelectTenantPod([]*tenant.Pod{})
		require.EqualError(t, err, balancer.ErrNoAvailablePods.Error())
		require.Nil(t, pod)
	})

	t.Run("few pods", func(t *testing.T) {
		pod, err := b.SelectTenantPod([]*tenant.Pod{{Addr: "1"}, {Addr: "2"}})
		require.NoError(t, err)
		require.Equal(t, "1", pod.Addr)
	})
}
