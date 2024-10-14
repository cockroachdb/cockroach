// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/stretchr/testify/require"
)

var (
	oneMicro         = 1 * time.Microsecond
	oneByte          = logconfig.ByteSize(1)
	oneThousandBytes = logconfig.ByteSize(1000)
	notBuffered      = false
	bufferedAddr     = "buffered.sink.com"
)

func TestIterFileSinks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	// Set up a log config containing a file sink.
	cfg := logconfig.DefaultConfig()

	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"unbuffered": {
			Channels:     logconfig.SelectChannels(channel.OPS),
			FileDefaults: logconfig.FileDefaults{},
		},
		"buffered": {
			Channels: logconfig.SelectChannels(channel.DEV),
			FileDefaults: logconfig.FileDefaults{
				BufferedWrites: &notBuffered,
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

	// add an HTTP sink to make sure the iteration doesn't pick up all the
	// buffered sinks. It should only pick up the file sinks.
	cfg.Sinks.HTTPServers = map[string]*logconfig.HTTPSinkConfig{
		"buffered-http": {
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

	require.NoError(t, cfg.Validate(&sc.logDir))

	// Apply the configuration
	TestingResetActive()
	cleanup, err := ApplyConfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
	require.NoError(t, err)
	defer cleanup()

	callMap := map[string]bool{
		"logtest-stderr":     false,
		"logtest-unbuffered": false,
		"logtest-buffered":   false,
	}

	fn := func(fs *fileSink) error {
		require.NotEqual(
			t, fs.nameGenerator.fileNamePrefix,
			"logtest-buffered-http", "unexpected fileSink %q", fs.nameGenerator.fileNamePrefix,
		)
		require.False(
			t, callMap[fs.nameGenerator.fileNamePrefix], "fileSink %q was called twice", fs.nameGenerator.fileNamePrefix,
		)

		callMap[fs.nameGenerator.fileNamePrefix] = true
		return nil
	}
	require.NoError(t, logging.allSinkInfos.iterFileSinks(fn))

	for k, v := range callMap {
		require.Truef(t, v, "fileSink %q was never called during iteration", k)
	}
}

func TestIterHTTPSinks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

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
	cleanup, err := ApplyConfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
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
