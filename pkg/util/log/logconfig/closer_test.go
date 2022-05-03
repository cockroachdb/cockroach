// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logconfig

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSignalAndWaitForShutdown(t *testing.T) {
	t.Run("no NPE error if shutdown function not explicitly set", func(t *testing.T) {
		shutdown := NewLoggingCloser()
		assert.NotPanics(t, shutdown.SignalAndWaitForShutdown)
	})

	t.Run("returns after all registered sinks exit", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fakeSinkRoutine := func(ctx context.Context, ls *LoggingCloser) {
			ls.RegisterBufferSink()
			defer ls.BufferSinkDone()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
		}

		logShutdown := NewLoggingCloser()
		logShutdown.SetShutdownFn(func() {
			cancel()
		})

		for i := 0; i < 3; i++ {
			go fakeSinkRoutine(ctx, logShutdown)
		}

		doneChan := make(chan struct{})
		go func() {
			logShutdown.SignalAndWaitForShutdown()
			doneChan <- struct{}{}
		}()

		timeout := time.After(10 * time.Second)
		for {
			select {
			case <-doneChan:
				return
			case <-timeout:
				t.Error("Close hanging")
				return
			}
		}
	})
}
