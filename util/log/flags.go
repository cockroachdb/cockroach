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
//
// Author: Tobias Schottdorf

package log

import (
	"flag"

	"github.com/cockroachdb/cockroach/util/log/logflags"
)

func init() {
	logflags.InitFlags(&logging.mu, &logging.toStderr,
		newStringValue(&logDir, &logDirSet), &logging.nocolor, &logging.verbosity,
		&logging.vmodule, &logging.traceLocation)
	// We define this flag here because stderrThreshold has the type Severity
	// which we can't pass to logflags without creating an import cycle.
	flag.Var(&logging.stderrThreshold,
		logflags.AlsoLogToStderrName, "logs at or above this threshold go to stderr")
}
