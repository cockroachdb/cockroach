// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type traceHelper struct {
	enabled  bool
	testName string
	plotDir  string
	sample   int

	traceDirCreated  map[int64]struct{} // by storeID
	atDurationFormat string
}

func (tr traceHelper) OnRecording(
	t *testing.T, storeID int64, atDuration time.Duration, rec tracingpb.Recording,
) {
	if !tr.enabled || len(rec[0].Logs) == 0 {
		return
	}

	traceDir := filepath.Join(tr.plotDir, "traces", fmt.Sprintf("s%d", storeID))
	if _, ok := tr.traceDirCreated[storeID]; !ok {
		tr.traceDirCreated[storeID] = struct{}{}
		require.NoError(t, os.MkdirAll(traceDir, 0755))
	}
	re := regexp.MustCompile(`[^a-zA-Z0-9.]+`)

	var sampleS string
	if tr.sample > 0 {
		sampleS = strconv.Itoa(tr.sample)
	}
	outName := fmt.Sprintf("%s%s_%s_%s_s%d", tr.testName, sampleS, fmt.Sprintf(tr.atDurationFormat, atDuration.Seconds()),
		re.ReplaceAllString(rec[0].Operation, "_"), storeID)
	assert.NoError(t, os.WriteFile(
		filepath.Join(traceDir, outName),
		[]byte(rec.String()), 0644))
}

func makeTraceHelper(
	enabled bool, plotDir string, testName string, sample int, duration time.Duration,
) traceHelper {
	secondsSinceBeginningWidth := len(fmt.Sprintf("%d", int64(duration.Seconds()+1)))
	return traceHelper{
		enabled:         enabled,
		testName:        testName,
		plotDir:         plotDir,
		sample:          sample,
		traceDirCreated: map[int64]struct{}{},
		// Print seconds with two decimal places. The decimal point and two decimal
		// places need three more characters.
		atDurationFormat: fmt.Sprintf("%%0%d.2fs", secondsSinceBeginningWidth+3),
	}
}
