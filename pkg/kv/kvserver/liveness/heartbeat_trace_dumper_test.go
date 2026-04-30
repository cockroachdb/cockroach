// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package liveness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

func TestHeartbeatTraceDumper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()
	dumper := newHeartbeatTraceDumper(dir, st)

	// Create a trace recording to dump.
	tr := tracing.NewTracer()
	ctx, sp := tracing.EnsureChildSpan(
		context.Background(), tr, "test heartbeat",
		tracing.WithRecording(tracingpb.RecordingVerbose),
	)
	sp.Record("test event")
	rec := sp.GetRecording(tracingpb.RecordingVerbose)
	sp.Finish()

	// Dump the trace with a simulated slow duration.
	dumper.dump(ctx, 2*time.Second, rec)

	// Verify a file was created.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, strings.HasPrefix(entries[0].Name(), heartbeatTraceDumpPrefix))
	require.True(t, strings.HasSuffix(entries[0].Name(), heartbeatTraceDumpSuffix))
	require.Contains(t, entries[0].Name(), "2s")

	// Verify the file contains the trace recording.
	content, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)
	require.Contains(t, string(content), "test event")
}

func TestHeartbeatTraceDumperGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	// Set a very small size limit to force GC.
	SlowHeartbeatTraceDumpSizeLimit.Override(context.Background(), &st.SV, 1)

	dumper := newHeartbeatTraceDumper(dir, st)

	tr := tracing.NewTracer()

	// Create and dump two traces.
	for i := 0; i < 2; i++ {
		ctx, sp := tracing.EnsureChildSpan(
			context.Background(), tr, "test heartbeat",
			tracing.WithRecording(tracingpb.RecordingVerbose),
		)
		sp.Record("test event")
		rec := sp.GetRecording(tracingpb.RecordingVerbose)
		sp.Finish()
		dumper.dump(ctx, time.Duration(i+1)*time.Second, rec)
		// Sleep briefly so file names are distinct (they include timestamps).
		time.Sleep(10 * time.Millisecond)
	}

	// With a 1-byte size limit, only the latest (preserved) file should remain.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "expected GC to remove old dumps; found: %v", entries)
}
