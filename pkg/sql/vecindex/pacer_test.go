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
	queriesPerFixup := 50
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
			queriesPerFixup, err = strconv.Atoi(arg.Vals[0])
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
	p.Init(initialOpsPerSec, queriesPerFixup, func() crtime.Mono { return now })

	var allowedOpsPerSecPoints, actualOpsPerSecPoints []float64
	var queueSizePoints, delayPoints, queueSizeRatePoints []float64
	var queryCountPoints []int
	var nextQuery, nextFixup crtime.Mono
	var totalQueryCount int
	var delay time.Duration

	rng := rand.New(rand.NewSource(seed))

	// Simulate run with 1 millisecond "ticks".
	end := crtime.Mono(time.Duration(seconds) * time.Second)
	for now = 0; now < end; now += crtime.Mono(time.Millisecond) {
		noiseFactor := rng.Float64()*2*noise - noise

		if now >= nextFixup {
			if queuedFixups > 0 {
				queuedFixups--
			}

			fixupsInterval := crtime.Mono(time.Second / time.Duration(fixupsPerSec))
			fixupsInterval = crtime.Mono(float64(fixupsInterval) * (1 + noiseFactor))
			nextFixup += 1 + fixupsInterval
		}

		if now >= nextQuery {
			totalQueryCount++
			delay = p.OnInsertOrDelete(queuedFixups)
			delay = time.Duration(float64(delay) * (1 + noiseFactor))
			nextQuery += crtime.Mono(delay)

			if totalQueryCount%queriesPerFixup == 0 {
				queuedFixups++
				p.OnFixup()
			}
		}

		// Compute running average of last second of actual ops/sec.
		queryCountPoints = append(queryCountPoints, totalQueryCount)
		startOffset := 0
		endOffset := len(queryCountPoints) - 1
		if len(queryCountPoints) > 1000 {
			startOffset = endOffset - 1000
		}
		actualOpsPerSec := float64(queryCountPoints[endOffset] - queryCountPoints[startOffset])
		actualOpsPerSecPoints = append(actualOpsPerSecPoints, actualOpsPerSec)

		delayPoints = append(delayPoints, float64(delay.Milliseconds()))
		allowedOpsPerSecPoints = append(allowedOpsPerSecPoints, p.mu.allowedOpsPerSec)
		queueSizePoints = append(queueSizePoints, float64(queuedFixups))
		queueSizeRatePoints = append(queueSizeRatePoints, p.mu.queueSizeRate)
	}

	finalOffset := len(allowedOpsPerSecPoints) - 1
	if showQueueSize {
		caption := fmt.Sprintf(
			" Fixup queue size = %0.2f fixups (avg)\n", computePointAverage(queueSizePoints))
		return caption + asciigraph.Plot(queueSizePoints,
			asciigraph.Width(width),
			asciigraph.Height(height))
	}

	if showQueueSizeRate {
		caption := fmt.Sprintf(
			" Fixup queue size rate = %0.2f fixups/sec (avg)\n", computePointAverage(queueSizeRatePoints))
		return caption + asciigraph.Plot(queueSizeRatePoints,
			asciigraph.Width(width),
			asciigraph.Height(height))
	}

	if showDelayMillis {
		caption := fmt.Sprintf(" Delay (ms) = %v ms (avg)\n", computePointAverage(delayPoints))
		return caption + asciigraph.Plot(delayPoints,
			asciigraph.Width(width),
			asciigraph.Height(height))
	}

	if showActualOpsPerSec {
		caption := fmt.Sprintf(" Actual ops per second = %0.2f ops/sec (avg), %0.2f ops/sec (final)\n",
			computePointAverage(actualOpsPerSecPoints), actualOpsPerSecPoints[finalOffset])
		return caption + asciigraph.Plot(actualOpsPerSecPoints,
			asciigraph.Width(width),
			asciigraph.Height(height))
	}

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
