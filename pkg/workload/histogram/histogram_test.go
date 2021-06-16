// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package histogram

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	const (
		good = "foo_bar_baz"
		bad  = "yes/we/do*not*@crash"
	)
	r := NewRegistry(10*time.Second, "lorkwoad")
	h := r.GetHandle()

	h.Get(good).Record(time.Second)

	h.Get(bad).Record(time.Minute)
	h.Get(bad).Record(time.Second)

	hGood := h.mu.hists[good]
	hBad := h.mu.hists[bad]

	require.NotNil(t, hGood)
	require.NotNil(t, hBad)

	require.Equal(t, good, hGood.name)
	require.Contains(t, hGood.prometheusHistogram.Desc().String(), good)

	require.Equal(t, bad, hBad.name)
	require.Contains(t, hBad.prometheusHistogram.Desc().String(), "yes_we_do_not__crash")

	// Check that the "bad" metric registered both observations. We could do the same for hGood
	// but there's little reason for that to behave differently.
	ch := make(chan prometheus.Metric, 1)
	hBad.prometheusHistogram.Collect(ch)
	var m dto.Metric
	require.NoError(t, (<-ch).Write(&m))
	require.EqualValues(t, 2, m.GetHistogram().GetSampleCount())
}
