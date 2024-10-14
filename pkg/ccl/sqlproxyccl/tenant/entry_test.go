// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenant

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestHasRunningPod(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	for _, tc := range []struct {
		name     string
		pods     []*Pod
		expected bool
	}{
		{
			name:     "no pods",
			pods:     nil,
			expected: false,
		},
		{
			name:     "single running pod",
			pods:     []*Pod{{State: RUNNING}},
			expected: true,
		},
		{
			name:     "single draining pod",
			pods:     []*Pod{{State: DRAINING}},
			expected: false,
		},
		{
			name: "multiple pods",
			pods: []*Pod{
				{State: DRAINING},
				{State: DRAINING},
				{State: RUNNING},
				{State: RUNNING},
			},
			expected: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, hasRunningPod(tc.pods))
		})
	}
}

func TestTenantMetadataUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	e := &tenantEntry{}
	require.False(t, e.IsValid())

	e.UpdateTenant(&Tenant{Version: "001"})
	require.True(t, e.IsValid())
	require.Equal(t, "001", e.mu.tenant.Version)

	// Send a new version.
	e.UpdateTenant(&Tenant{Version: "003"})
	require.True(t, e.IsValid())
	require.Equal(t, "003", e.mu.tenant.Version)

	// Use an old version.
	e.UpdateTenant(&Tenant{Version: "002"})
	require.True(t, e.IsValid())
	require.Equal(t, "003", e.mu.tenant.Version)

	// Invalidate that entry.
	e.MarkInvalid()
	require.False(t, e.IsValid())

	// Use an old version.
	ten := &Tenant{
		Version:                 "002",
		AllowedCIDRRanges:       []string{"0.0.0.0/0"},
		AllowedPrivateEndpoints: []string{"a", "b"},
	}
	e.UpdateTenant(ten)
	require.True(t, e.IsValid())
	require.Equal(t, "002", e.mu.tenant.Version)
}
