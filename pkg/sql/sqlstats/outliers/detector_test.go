// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package outliers

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/require"
)

func TestLatencyThresholdDetector(t *testing.T) {
	t.Run("enabled false by default", func(t *testing.T) {
		detector := latencyThresholdDetector{st: cluster.MakeTestingClusterSettings()}
		require.False(t, detector.enabled())
	})

	t.Run("enabled true with nonzero threshold", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.True(t, detector.enabled())
	})

	t.Run("isOutlier false when disabled", func(t *testing.T) {
		detector := latencyThresholdDetector{st: cluster.MakeTestingClusterSettings()}
		require.False(t, detector.isOutlier(&Outlier_Statement{LatencyInSeconds: 1}))
	})

	t.Run("isOutlier false when fast enough", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.False(t, detector.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.5}))
	})

	t.Run("isOutlier true beyond threshold", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.True(t, detector.isOutlier(&Outlier_Statement{LatencyInSeconds: 1}))
	})
}
