// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/stretchr/testify/require"
)

// TestDefaultTestLogConfig checks the logging configuration used in tests.
func TestDefaultTestLogConfig(t *testing.T) {
	t.Run("no-dir", func(t *testing.T) {
		// No directory.
		testConfig := getTestConfig(nil, true /* mostly inline */)

		// Capturing fd2 writes is disabled.
		require.False(t, testConfig.CaptureFd2.Enable)
		// Test configs without a directory send all channels at severity INFO to stderr.
		for _, c := range logconfig.AllChannels() {
			require.True(t, testConfig.Sinks.Stderr.Channels.AllChannels.HasChannel(c))
			require.Equal(t, severity.INFO, testConfig.Sinks.Stderr.Channels.ChannelFilters[c])
		}
		// Output to files disabled.
		require.Equal(t, 0, len(testConfig.Sinks.FileGroups))

		// Redaction markers disabled on stderr.
		require.False(t, *testConfig.Sinks.Stderr.Redactable)
	})
	t.Run("with-dir", func(t *testing.T) {
		for _, mostlyInline := range []bool{false, true} {
			t.Run(fmt.Sprintf("mostly-inline=%v", mostlyInline), func(t *testing.T) {
				fakeDir := "/foo"
				testConfig := getTestConfig(&fakeDir, mostlyInline)

				// Capturing fd2 writes is disabled in any case, because we want
				// panic in tests to show up on the test's stderr.
				require.False(t, testConfig.CaptureFd2.Enable)

				if mostlyInline {
					// Test configs that prefer stderr send all channels to
					// stderr at severity INFO, except for HEALTH, STORAGE, and
					// KV_DISTRIBUTION.
					for _, c := range logconfig.AllChannels() {
						require.True(t, testConfig.Sinks.Stderr.Channels.AllChannels.HasChannel(c))
						if c == channel.HEALTH || c == channel.STORAGE || c == channel.KV_DISTRIBUTION {
							require.Equal(t, severity.WARNING, testConfig.Sinks.Stderr.Channels.ChannelFilters[c])
						} else {
							require.Equal(t, severity.INFO, testConfig.Sinks.Stderr.Channels.ChannelFilters[c])
						}
					}
				} else {
					// Test configs that prefer files send all channels at
					// severity FATAL to stderr.
					for _, c := range logconfig.AllChannels() {
						require.True(t, testConfig.Sinks.Stderr.Channels.AllChannels.HasChannel(c))
						require.Equal(t, severity.FATAL, testConfig.Sinks.Stderr.Channels.ChannelFilters[c])
					}
				}
				// Redaction markers disabled on stderr.
				require.False(t, *testConfig.Sinks.Stderr.Redactable)

				// Output to files enabled, with just 1 file sink for all channels at severity INFO.
				require.Equal(t, 4, len(testConfig.Sinks.FileGroups))
				fc1, ok := testConfig.Sinks.FileGroups["health"]
				require.True(t, ok)
				require.Equal(t, 1, len(fc1.Channels.ChannelFilters))
				require.Equal(t, severity.INFO, fc1.Channels.ChannelFilters[channel.HEALTH])
				require.Equal(t, fakeDir, *fc1.Dir)
				fc2, ok := testConfig.Sinks.FileGroups["storage"]
				require.True(t, ok)
				require.Equal(t, 1, len(fc2.Channels.ChannelFilters))
				require.Equal(t, severity.INFO, fc2.Channels.ChannelFilters[channel.STORAGE])
				require.Equal(t, fakeDir, *fc2.Dir)
				fc3, ok := testConfig.Sinks.FileGroups["kv-distribution"]
				require.True(t, ok)
				require.Equal(t, 1, len(fc3.Channels.ChannelFilters))
				require.Equal(t, severity.INFO, fc3.Channels.ChannelFilters[channel.KV_DISTRIBUTION])
				require.Equal(t, fakeDir, *fc3.Dir)
				def, ok := testConfig.Sinks.FileGroups["default"]
				require.True(t, ok)
				require.Equal(t, fakeDir, *def.Dir)
				for _, c := range logconfig.AllChannels() {
					require.True(t, def.Channels.AllChannels.HasChannel(c), "channel %v", c)
					if c == channel.HEALTH || c == channel.STORAGE || c == channel.KV_DISTRIBUTION {
						// These two have been selected at severity WARNING.
						require.Equal(t, severity.WARNING, def.Channels.ChannelFilters[c], "channel %v", c)
					} else {
						require.Equal(t, severity.INFO, def.Channels.ChannelFilters[c], "channel %v", c)
					}
				}
				require.True(t, *def.Redactable)
			})
		}
	})
}

// TestDefaultTestLogConfig checks that the logging configuration used
// in tests can be overidden using logging.testLogConfig.
func TestOverrideTestLogConfig(t *testing.T) {
	const fakeConf = `
file-defaults:
   redactable: false
sinks:
  file-groups:
     storage-errors:
        channels: STORAGE
        filter: ERROR
     force-file:
        dir: /force-dir
        channels: HEALTH
  stderr:
      channels: STORAGE
      filter: WARNING
`
	t.Run("no-dir", func(t *testing.T) {
		defer func(prev string) { logging.testLogConfig = prev }(logging.testLogConfig)
		logging.testLogConfig = fakeConf
		testConfig := getTestConfig(nil, true /* mostly inline */)

		// Capturing fd2 writes is disabled by default.
		// This is different from the default config for servers, where
		// fd2 capture is enabled by default.
		require.False(t, testConfig.CaptureFd2.Enable)

		// Check the forced stderr config.
		require.Equal(t, 1, len(testConfig.Sinks.Stderr.Channels.ChannelFilters))
		require.Equal(t, severity.WARNING, testConfig.Sinks.Stderr.Channels.ChannelFilters[channel.STORAGE])

		// Since the file default directory is undefined in this case,
		// only the force-file group remains. The other one was elided
		// because it misses a directory.
		require.Equal(t, 1, len(testConfig.Sinks.FileGroups))
		fc, ok := testConfig.Sinks.FileGroups["force-file"]
		require.True(t, ok)
		require.Equal(t, "/force-dir", *fc.Dir)
		require.Equal(t, 1, len(fc.Channels.ChannelFilters))
		require.Equal(t, severity.INFO, fc.Channels.ChannelFilters[channel.HEALTH])
		require.False(t, *fc.Redactable) // check inheritance
	})

	t.Run("with-dir", func(t *testing.T) {
		defer func(prev string) { logging.testLogConfig = prev }(logging.testLogConfig)
		logging.testLogConfig = fakeConf
		fakeDir := "/foo"
		testConfig := getTestConfig(&fakeDir, false /* mostly inline */)

		// Capturing fd2 writes is disabled by default.
		// This is different from the default config for servers, where
		// fd2 capture is enabled by default.
		require.False(t, testConfig.CaptureFd2.Enable)

		// Check the forced stderr config.
		require.Equal(t, 1, len(testConfig.Sinks.Stderr.Channels.ChannelFilters))
		require.Equal(t, severity.WARNING, testConfig.Sinks.Stderr.Channels.ChannelFilters[channel.STORAGE])

		// We have a file directory so both sinks are active.
		// We also get the implicit default target for the DEV channel, since
		// our config did not define it.
		require.Equal(t, 3, len(testConfig.Sinks.FileGroups))
		fc1, ok := testConfig.Sinks.FileGroups["force-file"]
		require.True(t, ok)
		require.Equal(t, 1, len(fc1.Channels.ChannelFilters))
		require.Equal(t, severity.INFO, fc1.Channels.ChannelFilters[channel.HEALTH])
		require.False(t, *fc1.Redactable) // check inheritance
		require.Equal(t, "/force-dir", *fc1.Dir)
		fc2, ok := testConfig.Sinks.FileGroups["storage-errors"]
		require.True(t, ok)
		require.Equal(t, 1, len(fc2.Channels.ChannelFilters))
		require.Equal(t, severity.ERROR, fc2.Channels.ChannelFilters[channel.STORAGE])
		require.False(t, *fc1.Redactable) // check inheritance
		require.Equal(t, "/foo", *fc2.Dir)
		def, ok := testConfig.Sinks.FileGroups["default"]
		require.True(t, ok)
		require.Equal(t, fakeDir, *def.Dir)
		for _, c := range logconfig.AllChannels() {
			if c == channel.HEALTH || c == channel.STORAGE {
				// These two have been selected away by the two other sinks already.
				require.False(t, def.Channels.AllChannels.HasChannel(c))
			} else {
				require.True(t, def.Channels.AllChannels.HasChannel(c))
				require.Equal(t, severity.INFO, def.Channels.ChannelFilters[c])
			}
		}
		require.False(t, *def.Redactable)
	})
}
