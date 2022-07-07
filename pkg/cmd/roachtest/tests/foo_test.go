// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestFoo(t *testing.T) {
	client, err := promapi.NewClient(promapi.Config{
		Address: "http://tobias-overload-new-0004.roachprod.crdb.io:9090",
	})
	now := timeutil.Now()
	require.NoError(t, err)
	c := promv1.NewAPI(client)
	// NB: this is, at each datapoint, the larger of the two values.
	const q = `
storage_l0_sublevels{store="3"}/20 > storage_l0_num_files{store="3"}/1000 or storage_l0_num_files{store="3"}/1000
`
	v, warns, err := c.QueryRange(context.Background(), q, promv1.Range{
		Start: now.Add(-10 * time.Minute),
		End:   now,
		Step:  time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, warns, 0)

	m := v.(model.Matrix)

	var n int
	for _, ss := range m {
		for _, v := range ss.Values {
			n++
			// We're pausing once we hit .8, but there's some delay so we may overshoot a bit.
			// In practice, we're hoping to stay away from 1.0 most of the time and definitely
			// we shouldn't be hitting 2.0 (unless pausing is broken, in which case we're
			// sure to hit it).
			if v.Value > 2.0 {
				t.Errorf("%s at %s: overload score %.2f", ss.Metric, v.Timestamp, v.Value)
			}
		}
	}

	require.NotZero(t, n)

	if t.Failed() {
		t.Log(m)
	}
}
