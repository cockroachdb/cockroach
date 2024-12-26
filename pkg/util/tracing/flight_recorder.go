// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/trace"
)

const timeFormat = "2006-01-02T15_04_05.000"
const min_record_interval = time.Minute

type FlightRecorder struct {
	fr        *trace.FlightRecorder
	outputDir string
	// lastSnapshot prevents the flight recorder from being called too frequently.
	lastSnapshot time.Time
}

// NewFlightRecorder creates and starts a trace.FlightRecorder
func NewFlightRecorder(ctx context.Context, outputDir string) (*FlightRecorder, error) {
	if outputDir == "" {
		return nil, errors.Newf("output directory not set: %s", outputDir)
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, err
	}

	fr := FlightRecorder{
		fr:        trace.NewFlightRecorder(),
		outputDir: outputDir,
	}

	// TODO: Only start if the cluster setting is enabled.
	if err := fr.fr.Start(); err != nil {
		return nil, err
	}

	return &fr, nil
}

func (fr *FlightRecorder) MaybeSnapshot() bool {
	now := timeutil.Now()
	// Don't take the snapshot if there was already one in the past interval.
	if now.Sub(fr.lastSnapshot) < min_record_interval {
		return false
	}
	fr.lastSnapshot = now

	// Attempt to grab the snapshot.
	var b bytes.Buffer
	if _, err := fr.fr.WriteTo(&b); err != nil {
		return false
	}
	filename := fmt.Sprintf(
		"%s/trace.%s",
		fr.outputDir,
		now.Format(timeFormat),
	)
	// Write it to a file.
	if err := os.WriteFile(filename, b.Bytes(), 0o755); err != nil {
		return false
	}
	return true
}

func (fr *FlightRecorder) Close() {
	// We don't care about any errors from stopping the flight recorder as we
	// are likely shutting down.
	_ = fr.fr.Stop()
}
