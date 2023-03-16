// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package profiler_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/backgroundprofiler/profiler"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/gogo/protobuf/types"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
)

func TestBackgroundProfiler(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	dir, _ := testutils.TempDir(t)
	//defer cleanup()

	bp := profiler.NewBackgroundProfiler(ctx, st, stopper, 1, dir)
	wg := ctxgroup.WithContext(ctx)
	wg.GoCtx(func(ctx context.Context) error {
		if err := bp.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return nil
	})
	defer func() {
		if err := wg.Wait(); err != nil {
			t.Fatal(err)
		}
	}()

	tr := tracing.NewTracerWithOpt(context.Background(),
		tracing.WithTracingMode(tracing.TracingModeActiveSpansRegistry))
	tr.SetBackgroundProfiler(bp)

	_, sp := tr.StartSpanCtx(ctx, "root",
		tracing.WithRecording(tracingpb.RecordingStructured), tracing.WithBackgroundProfiling())

	done := make(chan int)
	wg2 := ctxgroup.WithContext(ctx)
	for i := 0; i < runtime.NumCPU(); i++ {
		i := i
		wg2.GoCtx(func(ctx context.Context) error {
			// Start a new "root" span for each goroutine that is part of the same
			// trace as sp.
			operationName := fmt.Sprintf("op%d", i)
			innerSp := tr.StartSpan(operationName, tracing.WithRemoteParentFromSpanMeta(sp.Meta()))
			defer innerSp.Finish()
			for {
				select {
				case <-done:
					return nil
				default:
					log.Infof(ctx, "spinning in %d", i)
				}
			}
		})
	}

	<-time.After(time.Second * 10)
	close(done)
	require.NoError(t, wg2.Wait())
	traceID := sp.TraceID()
	rec := sp.FinishAndGetConfiguredRecording()

	// At this point we expect the background profiler to have written the
	// profiles (CPU and execution traces) to dir.
	dirEntry, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, dirEntry, 2)

	expectedProfileEvent := profiler.Profile{
		NodeID: 1,
	}
	for _, entry := range dirEntry {
		// For CPU profiles we can verify that only samples labelled with our
		// traceID are outputted in the profile.
		profilePath := filepath.Join(dir, entry.Name())
		if strings.Contains(entry.Name(), profiler.CPUProfilerFileNamePrefix) {
			expectedProfileEvent.CPUProfile = profilePath
			f, err := os.Open(profilePath)
			require.NoError(t, err)
			prof, err := profile.Parse(f)
			require.NoError(t, err)
			ignoreTagFilter, err := profiler.CompileTagFilter(fmt.Sprintf("%d", traceID))
			require.NoError(t, err)
			_, ignoreMatch := prof.FilterSamplesByTag(nil, ignoreTagFilter)
			require.True(t, ignoreMatch)
			require.Empty(t, prof.Sample)
		} else {
			expectedProfileEvent.RuntimeTrace = profilePath
		}
	}

	require.Len(t, rec[0].StructuredRecords, 1)
	var p profiler.Profile
	require.NoError(t, types.UnmarshalAny(rec[0].StructuredRecords[0].Payload, &p))
	require.Equal(t, expectedProfileEvent, p)
	log.Infof(ctx, "dir is %s", dir)
}
