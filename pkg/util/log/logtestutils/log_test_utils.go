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
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// InstallLogFileSink installs a file sink for telemetry logging tests.
func InstallLogFileSink(sc *log.TestLogScope, t *testing.T, channel logpb.Channel) func() {
	// Enable logging channels.
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	// Make a sink for just the session log.
	cfg.Sinks.FileGroups = make(map[string]*logconfig.FileSinkConfig)
	fileSinkConfig := logconfig.FileSinkConfig{Channels: logconfig.SelectChannels(channel)}
	switch channel {
	case logpb.Channel_TELEMETRY:
		cfg.Sinks.FileGroups["telemetry"] = &fileSinkConfig
	case logpb.Channel_SENSITIVE_ACCESS:
		cfg.Sinks.FileGroups["sql-audit"] = &fileSinkConfig
	default:
		panic("unrecognized logging channel")
	}
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
