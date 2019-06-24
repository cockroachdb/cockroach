// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package status

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/kr/pretty"
)

func TestHealthCheckMetricsMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	m := metricsMap{}

	tracked := map[string]threshold{
		"gauge0":   gaugeZero,
		"gauge1":   gaugeZero,
		"gauge2":   {gauge: true, min: 100},
		"counter0": counterZero,
		"counter1": counterZero,
		"counter2": {min: 100},
	}

	check := func(act, exp metricsMap) {
		t.Helper()
		if diff := pretty.Diff(act, exp); len(diff) != 0 {
			t.Fatalf("diff(act,exp) = %s\n\nact=%+v\nexp=%+v", strings.Join(diff, "\n"), act, exp)
		}
	}

	// A gauge and a counter show up.
	check(m.update(tracked, metricsMap{
		0: {
			"gauge0":   1,
			"counter0": 12,
		},
		1: {
			"gauge0":   10,
			"counter2": 0,
		},
	}), metricsMap{
		0: {"gauge0": 1},
		1: {"gauge0": 10},
	})

	check(m, metricsMap{
		0: {
			"counter0": 12,
		},
		1: {
			"counter2": 0,
		},
	})

	// A counter increments for the second time, and another one shows up for the
	// first time. The thresholded counter moves, but stays below threshold.
	check(m.update(tracked, metricsMap{
		0: {
			"counter0": 14,
			"counter1": 5,
		},
		1: {
			"counter2": 100, // barely misses threshold
		},
	}), metricsMap{
		0: {"counter0": 2},
	})

	check(m, metricsMap{
		0: {
			"counter0": 14,
			"counter1": 5,
		},
		1: {
			"counter2": 100,
		},
	})

	// A gauge shows up for the second time. A counter we've seen before increments,
	// but on a different store (so it's really a first time still). The thresholded
	// counter jumps by the threshold value (note that counter thresholds aren't
	// really that useful, except as a very poor man's rate limiter).
	check(m.update(tracked, metricsMap{
		1: {
			"gauge0":   12,
			"counter1": 9,
			"counter2": 201,
		},
	}), metricsMap{
		1: {
			"gauge0":   12,
			"counter2": 101,
		},
	})

	check(m, metricsMap{
		0: {
			"counter0": 14,
			"counter1": 5,
		},
		1: {
			"counter1": 9,
			"counter2": 201,
		},
	})

	// Both metrics we've seen before change. One increments (expected) and one
	// decrements (we never do that in practice).
	check(m.update(tracked, metricsMap{
		0: {
			"counter0": 3,
			"counter1": 10,
		},
		1: {
			"counter1": 4,
		},
	}), metricsMap{
		0: {
			"counter1": 5,
		},
	})

	finalMap := metricsMap{
		0: {
			"counter0": 3,
			"counter1": 10,
		},
		1: {
			"counter1": 4,
			"counter2": 201,
		},
	}
	check(m, finalMap)

	// Nothing changes, except something we don't track.
	check(m.update(tracked, metricsMap{1: {"banana": 100}}), metricsMap{})
	check(m.update(tracked, metricsMap{1: {"banana": 300}}), metricsMap{})
	check(m, finalMap)
}
