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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf

package log

import "flag"

// init adds the logging flags on the supplied FlagSet.
func init() {
	flag.BoolVar(&logging.toStderr, "logtostderr", true, "log to standard error instead of files")
	flag.BoolVar(&logging.alsoToStderr, "alsologtostderr", false, "log to standard error as well as files")
	flag.BoolVar(&logging.json, "logjson", false, "log in JSON format")
	flag.StringVar(&logging.color, "color", "auto", "colorize standard error output according to severity")
	flag.Var(&logging.verbosity, "verbosity", "log level for V logs")
	// TODO(tschottdorf): decide if we need this.
	// pf.Var(&logging.stderrThreshold, "log-threshold", "logs at or above this threshold go to stderr")
	flag.Var(&logging.vmodule, "vmodule", "comma-separated list of file=N settings for file-filtered logging")
	flag.Var(&logging.traceLocation, "log-backtrace-at", "when logging hits line file:N, emit a stack trace")
	logDir = flag.String("log-dir", "", "if non-empty, write log files in this directory") // in clog_file.go
}
