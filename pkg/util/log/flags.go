// Copyright 2015 The Cockroach Authors.
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
	"flag"

	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
)

func init() {
	logflags.InitFlags(
		&mainLog.noStderrRedirect,
		&mainLog.logDir, &showLogs, &noColor,
		&logging.vmoduleConfig.mu.vmodule,
		&LogFileMaxSize, &LogFilesCombinedMaxSize,
	)
	// We define these flags here because they have the type Severity
	// which we can't pass to logflags without creating an import cycle.
	flag.Var(&logging.stderrThreshold,
		logflags.LogToStderrName, "logs at or above this threshold go to stderr")
	flag.Var(&mainLog.fileThreshold,
		logflags.LogFileVerbosityThresholdName, "minimum verbosity of messages written to the log file")
}
