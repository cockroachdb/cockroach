// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/stretchr/testify/require"
)

func TestIterHTTPSinks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	oneMicro := 1 * time.Microsecond
	oneByte := logconfig.ByteSize(1)
	oneThousandBytes := logconfig.ByteSize(1000)
	bufferedAddr := "buffered.sink.com"
	unbufferedAddr := "unbuffered.sink.com"

	// Set up a log config containing both buffered and unbuffered HTTP sinks.
	cfg := logconfig.DefaultConfig()
	cfg.Sinks.HTTPServers = map[string]*logconfig.HTTPSinkConfig{
		"unbuffered": {
			Channels: logconfig.SelectChannels(channel.OPS),
			HTTPDefaults: logconfig.HTTPDefaults{
				Address: &unbufferedAddr,
			},
		},
		"buffered": {
			Channels: logconfig.SelectChannels(channel.OPS),
			HTTPDefaults: logconfig.HTTPDefaults{
				Address: &bufferedAddr,
				CommonSinkConfig: logconfig.CommonSinkConfig{
					Buffering: logconfig.CommonBufferSinkConfigWrapper{
						CommonBufferSinkConfig: logconfig.CommonBufferSinkConfig{
							MaxStaleness:     &oneMicro,
							MaxBufferSize:    &oneThousandBytes,
							FlushTriggerSize: &oneByte,
						},
					},
				},
			},
		},
	}
	cfg.Sinks.FluentServers = map[string]*logconfig.FluentSinkConfig{
		"excluded": {
			Channels: logconfig.SelectChannels(channel.OPS),
			Address:  "some-addr.com",
		},
	}
	require.NoError(t, cfg.Validate(&sc.logDir))

	// Apply the configuration
	TestingResetActive()
	cleanup, err := ApplyConfig(cfg, FileSinkMetrics{})
	require.NoError(t, err)
	defer cleanup()

	// Verify both sinks are selected when iterating. Also, make sure that
	// we don't have any extra calls for non-http sinks (e.g. the fluent
	// server we added earlier).
	callCount := 0
	expCallCount := 2
	calls := map[string]bool{
		bufferedAddr:   false,
		unbufferedAddr: false,
	}
	fn := func(hs *httpSink) error {
		callCount++
		if called, ok := calls[*hs.config.Address]; ok {
			if called {
				t.Errorf("httpSink %q unexpectedly called twice", *hs.config.Address)
			}
			calls[*hs.config.Address] = true
		}
		return nil
	}
	require.NoError(t, logging.allSinkInfos.iterHTTPSinks(fn))
	for k, v := range calls {
		require.Truef(t, v, "httpSink %q was never called during iteration", k)
	}
	require.Equal(t, expCallCount, callCount)
}
