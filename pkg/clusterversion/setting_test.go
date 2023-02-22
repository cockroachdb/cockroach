// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	s1 := cluster.MakeTestingClusterSettingsWithVersions(clusterversion.TestingBinaryVersion, clusterversion.TestingBinaryMinSupportedVersion, true)
	s2 := cluster.MakeTestingClusterSettingsWithVersions(clusterversion.TestingBinaryVersion, clusterversion.TestingBinaryMinSupportedVersion, true)

	m1 := clusterversion.MakeMetricsAndRegisterOnVersionChangeCallback(&s1.SV)
	m2 := clusterversion.MakeMetricsAndRegisterOnVersionChangeCallback(&s2.SV)

	require.Equal(t, int64(0), m1.PreserveDowngradeLastUpdated.Value())
	require.Equal(t, int64(0), m2.PreserveDowngradeLastUpdated.Value())

	// Test that the metrics returned are independent.
	clusterversion.PreserveDowngradeVersion.Override(ctx, &s1.SV, clusterversion.TestingBinaryVersion.String())
	testutils.SucceedsSoon(t, func() error {
		if int64(0) >= m1.PreserveDowngradeLastUpdated.Value() {
			return errors.New("metric is zero. expected positive number.")
		}
		return nil
	})
	require.Equal(t, int64(0), m2.PreserveDowngradeLastUpdated.Value())

	clusterversion.PreserveDowngradeVersion.Override(ctx, &s2.SV, clusterversion.TestingBinaryVersion.String())
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
