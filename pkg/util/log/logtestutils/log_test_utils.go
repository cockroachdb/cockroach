// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logtestutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
)

// InstallTelemetryLogFileSink installs a file sink for telemetry logging tests.
func InstallTelemetryLogFileSink(sc *log.TestLogScope, t *testing.T) func() {
	// Enable logging channels.
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	// Make a sink for just the session log.
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"telemetry": {
			Channels: logconfig.SelectChannels(channel.TELEMETRY),
		}}
	dir := sc.GetDirectory()
	if err := cfg.Validate(&dir); err != nil {
		t.Fatal(err)
	}
	cleanup, err := log.ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return cleanup
}
