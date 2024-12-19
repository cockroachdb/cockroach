// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/datadriven"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

// TestPacer runs the pacer with simulated insert and delete operations and
// fixups. It prints ASCII plots of key metrics over the course of the run in
// order to evaluate its effectiveness.
func TestPacer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, "testdata/pacer", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "plot":
				return plotPacerDecisions(t, d)
			}

			t.Fatalf("unknown cmd: %s", d.Cmd)
			return ""
		})
	})
}

func plotPacerDecisions(t *testing.T, d *datadriven.TestData) string {
	const seed = 42

	var err error
	seconds := 10
	height := 10
	width := 90
	initialOpsPerSec := 500
	queuedFixups := 0
	opsPerFixup := 50
	fixupsPerSec := 10
	showActualOpsPerSec := false
	showQueueSize := false
	showQueueSizeRate := false
	showDelayMillis := false
	noise := 0.0

	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "seconds":
			require.Len(t, arg.Vals, 1)
			seconds, err = strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)

		case "height":
			require.Len(t, arg.Vals, 1)
			height, err = strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)

		case "width":
			require.Len(t, arg.Vals, 1)
			width, err = strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)

		case "initial-ops-per-sec":
			require.Len(t, arg.Vals, 1)
			initialOpsPerSec, err = strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)

		case "initial-fixups":
			require.Len(t, arg.Vals, 1)
			queuedFixups, err = strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)

		case "ops-per-fixup":
			require.Len(t, arg.Vals, 1)
			opsPerFixup, err = strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)

		case "fixups-per-sec":
			require.Len(t, arg.Vals, 1)
			fixupsPerSec, err = strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)

		case "show-actual-ops-per-sec":
			require.Len(t, arg.Vals, 0)
			showActualOpsPerSec = true

		case "show-queue-size":
			require.Len(t, arg.Vals, 0)
			showQueueSize = true

		case "show-queue-size-rate":
			require.Len(t, arg.Vals, 0)
			showQueueSizeRate = true

		case "show-delay-millis":
			require.Len(t, arg.Vals, 0)
			showDelayMillis = true

		case "noise":
			require.Len(t, arg.Vals, 1)
			noise, err = strconv.ParseFloat(arg.Vals[0], 64)
			require.NoError(t, err)
		}
	}

	var now crtime.Mono
	var p pacer
	p.Init(initialOpsPerSec, queuedFixups, func() crtime.Mono { return now })

	var allowedOpsPerSecPoints, actualOpsPerSecPoints []float64
	var queueSizePoints, delayPoints, queueSizeRatePoints []float64
	var opCountPoints []int
	var nextOp, nextFixup, nextPoint crtime.Mono
	var totalOpCount int
	var delay time.Duration

	rng := rand.New(rand.NewSource(seed))

	// Simulate run with deterministic clock.
	end := crtime.Mono(time.Duration(seconds) * time.Second)
	for now < end {
		noiseFactor := rng.Float64()*2*noise - noise

		if now == nextOp {
			totalOpCount++
			delay = p.OnInsertOrDelete(queuedFixups)
			// Keep ops/sec <= 1000 (not counting noise).
			interval := max(delay, time.Millisecond)
			interval = time.Duration(float64(interval) * (1 + noiseFactor))
			nextOp = now + crtime.Mono(interval)

			if totalOpCount%opsPerFixup == 0 {
				queuedFixups++
				p.OnFixup(queuedFixups)
			}
		}

		if now == nextFixup {
			if queuedFixups > 0 {
				queuedFixups--
				p.OnFixup(queuedFixups)
			}

			fixupsInterval := crtime.Mono(time.Second / time.Duration(fixupsPerSec))
			fixupsInterval = max(crtime.Mono(float64(fixupsInterval)*(1+noiseFactor)), 1)
			nextFixup = now + fixupsInterval
		}

		if now == nextPoint {
			// Compute running average of last second of actual ops/sec.
			opCountPoints = append(opCountPoints, totalOpCount)
			startOffset := 0
			endOffset := len(opCountPoints) - 1
			if len(opCountPoints) > 1000 {
				startOffset = endOffset - 1000
			}

			actualOpsPerSec := float64(opCountPoints[endOffset] - opCountPoints[startOffset])
			actualOpsPerSecPoints = append(actualOpsPerSecPoints, actualOpsPerSec)

			delayPoints = append(delayPoints, delay.Seconds()*1000)
			allowedOpsPerSecPoints = append(allowedOpsPerSecPoints, p.allowedOpsPerSec)
			queueSizePoints = append(queueSizePoints, float64(queuedFixups))
			queueSizeRatePoints = append(queueSizeRatePoints, p.queueSizeRate)

			nextPoint += crtime.Mono(time.Millisecond)
		}

		now = min(nextOp, nextFixup, nextPoint)
	}

	finalOffset := len(allowedOpsPerSecPoints) - 1
	if showQueueSize {
		queueSizePoints = normalizeFloats(queueSizePoints)
		caption := fmt.Sprintf(" Fixup queue size = %0.2f fixups (avg), %v fixups (final)\n",
			computePointAverage(queueSizePoints), queueSizePoints[finalOffset])
		return caption + asciigraph.Plot(queueSizePoints,
			asciigraph.Width(width),
			asciigraph.Height(height))
	}

	if showQueueSizeRate {
		queueSizeRatePoints = normalizeFloats(queueSizeRatePoints)
		caption := fmt.Sprintf(" Fixup queue size rate = %0.2f fixups/sec (avg)\n",
			computePointAverage(queueSizeRatePoints))
		return caption + asciigraph.Plot(queueSizeRatePoints,
			asciigraph.Width(width),
			asciigraph.Height(height))
	}

	if showDelayMillis {
		delayPoints = normalizeFloats(delayPoints)
		caption := fmt.Sprintf(" Delay (ms) = %0.2f ms (avg), %0.2f ms (final)\n",
			computePointAverage(delayPoints), delayPoints[finalOffset])
		return caption + asciigraph.Plot(delayPoints,
			asciigraph.Width(width),
			asciigraph.Height(height))
	}

	if showActualOpsPerSec {
		actualOpsPerSecPoints = normalizeFloats(actualOpsPerSecPoints)
		caption := fmt.Sprintf(" Actual ops per second = %0.2f ops/sec (avg), %0.2f ops/sec (final)\n",
			computePointAverage(actualOpsPerSecPoints), actualOpsPerSecPoints[finalOffset])
		return caption + asciigraph.Plot(actualOpsPerSecPoints,
			asciigraph.Width(width),
			asciigraph.Height(height))
	}

	allowedOpsPerSecPoints = normalizeFloats(allowedOpsPerSecPoints)
	caption := fmt.Sprintf(" Allowed ops per second = %0.2f ops/sec (avg), %0.2f ops/sec (final)\n",
		computePointAverage(allowedOpsPerSecPoints), allowedOpsPerSecPoints[finalOffset])
	return caption + asciigraph.Plot(allowedOpsPerSecPoints,
		asciigraph.Width(width),
		asciigraph.Height(height))
}

func computePointAverage(points []float64) float64 {
	sum := 0.0
	for _, p := range points {
		sum += p
	}
	return sum / float64(len(points))
}

// normalizeFloats maps any negative zero values to positive zero. This can
// differ in x86 vs. ARM and cause test failures.
func normalizeFloats(values []float64) []float64 {
	for i := range values {
		if values[i] == 0 {
			values[i] = 0
		}
	}
	return values
}

// TestMultipleArrival tests multiple operations arriving while still waiting
// out the delay for previous operations.
func TestMultipleArrival(t *testing.T) {
	var p pacer
	var now crtime.Mono
	p.Init(500, 0, func() crtime.Mono { return now })

	// Initial delay is 2 milliseconds.
	delay := p.OnInsertOrDelete(5)
	require.Equal(t, 2*time.Millisecond, delay)

	// After 1 millisecond has elapsed, another op arrives; it should be delayed
	// by 3 milliseconds.
	now += crtime.Mono(time.Millisecond)
	delay = p.OnInsertOrDelete(5)
	require.Equal(t, 3*time.Millisecond, delay)

	// Before any further waiting, yet another op arrives.
	delay = p.OnInsertOrDelete(5)
	require.Equal(t, 5*time.Millisecond, delay)
}

func TestCancelOp(t *testing.T) {
	var p pacer
	var now crtime.Mono
	p.Init(500, 0, func() crtime.Mono { return now })
	delay := p.OnInsertOrDelete(5)
	require.Equal(t, 2*time.Millisecond, delay)
	delay = p.OnInsertOrDelete(5)
	require.Equal(t, 4*time.Millisecond, delay)

	// "Cancel" one of the operations and expect the next operation to get the
	// same delay as the second operation rather than 6 ms.
	p.OnInsertOrDeleteCanceled()
	delay = p.OnInsertOrDelete(5)
	require.Equal(t, 4*time.Millisecond, delay)
}

// TestCalculateQueueSizeRate tests the queue size rate EMA. The "exact" field
// shows that the EMA is a good approximation of the true value.
func TestCalculateQueueSizeRate(t *testing.T) {
	testCases := []struct {
		desc         string
		elapsed      time.Duration
		queuedFixups int
		expected     float64
		exact        float64
	}{
		{
			desc:         "EMA is exact when elapsed time is = 1 second",
			elapsed:      time.Second,
			queuedFixups: 5,
			expected:     5,
			exact:        5,
		},
		{
			desc:         "EMA is exact when elapsed time is > 1 second",
			elapsed:      2 * time.Second,
			queuedFixups: 8,
			expected:     1.5,
			exact:        1.5,
		},
		{
			desc:         "rate increases over a smaller time period",
			elapsed:      time.Second / 2,
			queuedFixups: 9,
			expected:     1.75,
			exact:        1,
		},
		{
			desc:         "rate continues to increase",
			elapsed:      time.Second / 2,
			queuedFixups: 10,
			expected:     1.88,
			exact:        2,
		},
		{
			desc:         "rate begins to decrease over a smaller time period",
			elapsed:      time.Second / 4,
			queuedFixups: 9,
			expected:     0.41,
			exact:        0,
		},
		{
			desc:         "rate continues to decrease",
			elapsed:      time.Second / 4,
			queuedFixups: 6,
			expected:     -2.7,
			exact:        -3,
		},
	}

	var p pacer
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actual := p.calculateQueueSizeRate(tc.elapsed, tc.queuedFixups)
			require.Equal(t, tc.expected, scalar.Round(actual, 2))
		})
	}
}
