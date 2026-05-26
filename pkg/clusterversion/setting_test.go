// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMakeMetricsAndRegisterOnVersionChangeCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s1 := cluster.MakeTestingClusterSettingsWithVersions(clusterversion.Latest.Version(), clusterversion.MinSupported.Version(), true)
	s2 := cluster.MakeTestingClusterSettingsWithVersions(clusterversion.Latest.Version(), clusterversion.MinSupported.Version(), true)

	m1 := clusterversion.MakeMetricsAndRegisterOnVersionChangeCallback(&s1.SV)
	m2 := clusterversion.MakeMetricsAndRegisterOnVersionChangeCallback(&s2.SV)

	require.Equal(t, int64(0), m1.PreserveDowngradeLastUpdated.Value())
	require.Equal(t, int64(0), m2.PreserveDowngradeLastUpdated.Value())

	// Test that the metrics returned are independent.
	clusterversion.PreserveDowngradeVersion.Override(ctx, &s1.SV, clusterversion.Latest.String())
	testutils.SucceedsSoon(t, func() error {
		if int64(0) >= m1.PreserveDowngradeLastUpdated.Value() {
			return errors.New("metric is zero. expected positive number.")
		}
		return nil
	})
	require.Equal(t, int64(0), m2.PreserveDowngradeLastUpdated.Value())

	clusterversion.PreserveDowngradeVersion.Override(ctx, &s2.SV, clusterversion.Latest.String())
	testutils.SucceedsSoon(t, func() error {
		if int64(0) >= m2.PreserveDowngradeLastUpdated.Value() {
			return errors.New("metric is zero. expected positive number.")
		}
		return nil
	})

	clusterversion.PreserveDowngradeVersion.Override(ctx, &s1.SV, "")
	testutils.SucceedsSoon(t, func() error {
		if int64(0) != m1.PreserveDowngradeLastUpdated.Value() {
			return errors.New("metric is non-zero. expected zero.")
		}
		return nil
	})
}

func BenchmarkClusterVersionSettingIsActive(b *testing.B) {
	s := cluster.MakeTestingClusterSettings()
	ctx := context.Background()
	active := true
	for i := 0; i < b.N; i++ {
		active = s.Version.IsActive(ctx, clusterversion.Latest) && active
	}
	require.True(b, active)
}
