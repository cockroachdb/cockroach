// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exectrace

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/exp/trace"
	"golang.org/x/sys/unix"
)

var FR *trace.FlightRecorder
var internalCh chan struct{}

func init() {
	FR = trace.NewFlightRecorder()
	internalCh = make(chan struct{}, 1)
}

var FlightRecorderEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"observability.flightrecorder.enabled",
	"TODO",
	true,
	settings.WithPublic,
	settings.WithReportable(true),
)

// TakeFlightRecorderSnapshot signals to the internal flight recorder
// to write a snapshot of the current buffer to the target directory.
// This call may wait if another snapshot request is in progress, this
// function will return if the context is cancelled.
func TakeFlightRecorderSnapshot(ctx context.Context) {
	select {
	case internalCh <- struct{}{}:
	case <-ctx.Done():
	}
}

func StartFlightRecorder(
	ctx context.Context, stopper *stop.Stopper, sv *settings.Values, destDir string,
) {
	stopper.RunAsyncTask(ctx, "go-flight-recorder", func(ctx context.Context) {
		err := FR.Start()
		if err != nil {
			log.Errorf(ctx, "unable to start flight recorder: %v", err)
			return
		}

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, unix.SIGINT, unix.SIGTERM, unix.SIGQUIT, unix.SIGINFO)

		for {
			select {
			case <-signalCh:
				if !FlightRecorderEnabled.Get(sv) {
					continue
				}
				filename := fmt.Sprintf("flight-recorder-%d.trace", timeutil.Now().UnixMilli())
				destFile, err := os.OpenFile(filepath.Join(destDir, filename), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
				if err != nil {
					log.Errorf(ctx, "unable to open file %s to dump flight record: %v", destDir, err)
					continue
				}
				_, err = FR.WriteTo(destFile)
				if err != nil {
					log.Errorf(ctx, "error while writing flight record to %s: %v", destDir, err)
				}
			case <-ctx.Done():
			case <-stopper.ShouldQuiesce():
				err := FR.Stop()
				if err != nil {
					log.Errorf(ctx, "unable to stop flight recorder: %v", err)
				}
				return
			}
		}
	})
}
