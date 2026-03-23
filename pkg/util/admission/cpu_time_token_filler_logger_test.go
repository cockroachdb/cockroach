// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
)

func TestCPUTimeTokenFitLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var fl fitLogger
	var buf strings.Builder

	const cpuCapacity = 16.0

	// Helper that calls accumulate and appends any emitted log line.
	// The two knobs are multiplier and intCPU; everything else derives:
	//   tokensUsed = intCPU / multiplier
	//   isLowCPU   = intCPU <= 2s
	acc := func(multiplier float64, intCPU time.Duration) {
		tokensUsed := int64(float64(intCPU) / multiplier)
		isLowCPU := intCPU <= 2*time.Second
		if msg, shouldLog := fl.accumulate(
			multiplier, isLowCPU,
			intCPU, tokensUsed,
			time.Second, cpuCapacity,
		); shouldLog {
			buf.WriteString(string(redact.Sprint(msg)))
			buf.WriteByte('\n')
		}
	}

	// Scenario 1: 10 steady-state intervals, multiplier stable at 1.0.
	for i := 0; i < 10; i++ {
		acc(
			1.0,           // multiplier
			8*time.Second, // intCPU
		)
	}

	// Scenario 2: multiplier ramps from 1.1 to 2.0 over 10 intervals,
	// exercising the [lo,hi] range tracking.
	for i := 0; i < 10; i++ {
		acc(
			1.0+float64(i+1)*0.1, // multiplier
			8*time.Second,        // intCPU
		)
	}

	// Scenario 3: isLowCPU flips true on interval 3, triggering early emit.
	acc(
		2.0,           // multiplier
		8*time.Second, // intCPU
	)
	acc(
		2.0,           // multiplier
		8*time.Second, // intCPU
	)
	acc(
		2.0,           // multiplier
		1*time.Second, // intCPU
	)

	// Scenario 4: sustained low CPU for a full window.
	for i := 0; i < 10; i++ {
		acc(
			2.0,           // multiplier
			1*time.Second, // intCPU
		)
	}

	// Scenario 5: isLowCPU flips back to high, triggering early emit.
	acc(
		2.0,           // multiplier
		8*time.Second, // intCPU
	)

	echotest.Require(t, buf.String(), datapathutils.TestDataPath(t, "cpu_time_token_fit_logger"))
}
