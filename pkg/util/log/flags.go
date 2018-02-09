// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package log

import (
	"flag"

	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
)

func init() {
	logflags.InitFlags(
		&logging.noStderrRedirect,
		&logging.logDir, &showLogs, &noColor, &logging.verbosity,
		&logging.vmodule, &logging.traceLocation,
		&LogFileMaxSize, &LogFilesCombinedMaxSize,
	)
	// We define these flags here because they have the type Severity
	// which we can't pass to logflags without creating an import cycle.
	flag.Var(&logging.stderrThreshold,
		logflags.LogToStderrName, "logs at or above this threshold go to stderr")
	flag.Var(&logging.fileThreshold,
		logflags.LogFileVerbosityThresholdName, "minimum verbosity of messages written to the log file")
}
