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

func TestAnyDetector(t *testing.T) {
	t.Run("enabled is false without any detectors", func(t *testing.T) {
		detector := &anyDetector{}
		require.False(t, detector.enabled())
	})

	t.Run("enabled is false with all disabled detectors", func(t *testing.T) {
		detector := &anyDetector{[]detector{&fakeDetector{}, &fakeDetector{}}}
		require.False(t, detector.enabled())
	})

	t.Run("enabled is true with at least one enabled detector", func(t *testing.T) {
		detector := &anyDetector{[]detector{&fakeDetector{stubEnabled: true}, &fakeDetector{}}}
		require.True(t, detector.enabled())
	})

	t.Run("isOutlier is false without any detectors", func(t *testing.T) {
		detector := &anyDetector{}
		require.False(t, detector.isOutlier(&Outlier_Statement{}))
	})

	t.Run("isOutlier is false without any concerned detectors", func(t *testing.T) {
		detector := &anyDetector{[]detector{&fakeDetector{}, &fakeDetector{}}}
		require.False(t, detector.isOutlier(&Outlier_Statement{}))
	})

	t.Run("isOutlier is true with at least one concerned detector", func(t *testing.T) {
		detector := &anyDetector{[]detector{&fakeDetector{stubIsOutlier: true}, &fakeDetector{}}}
		require.True(t, detector.isOutlier(&Outlier_Statement{}))
	})

	t.Run("isOutlier consults all detectors without short-circuiting", func(t *testing.T) {
		// Detector implementations may wish to observe all statements, to
		// build up their baseline sense of what "usual" is. To short-circuit
		// would deny them that chance.
		d1 := &fakeDetector{stubIsOutlier: true}
		d2 := &fakeDetector{stubIsOutlier: true}
		detector := &anyDetector{[]detector{d1, d2}}
		detector.isOutlier(&Outlier_Statement{})
		require.True(t, d1.isOutlierCalled, "the first detector should be consulted")
		require.True(t, d2.isOutlierCalled, "the second detector should be consulted")
	})
}

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

type fakeDetector struct {
	stubEnabled     bool
	stubIsOutlier   bool
	isOutlierCalled bool
}

func (f fakeDetector) enabled() bool {
	return f.stubEnabled
}

func (f *fakeDetector) isOutlier(_ *Outlier_Statement) bool {
	f.isOutlierCalled = true
	return f.stubIsOutlier
}

var _ detector = &fakeDetector{}
