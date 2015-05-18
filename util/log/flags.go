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

import (
	"os"
	"strings"

	"github.com/spf13/pflag"
)

// InitFlags adds the logging flags on the supplied FlagSet.
func InitFlags(pf *pflag.FlagSet) {
	pf.BoolVar(&logging.toStderr, "logtostderr", true, "log to standard error instead of files")
	pf.BoolVar(&logging.alsoToStderr, "alsologtostderr", false, "log to standard error as well as files")
	pf.Var(&pflagValue{&logging.verbosity}, "verbosity", "log level for V logs")
	// TODO(tschottdorf): decide if we need this; it specifies the log messages
	// which should go to stderr, which is only relevant if neither log-stderr
	// and log-tee are set.
	// pf.Var(&pflagValue{&logging.stderrThreshold}, "log-threshold", "logs at or above this threshold go to stderr")
	pf.Var(&pflagValue{&logging.vmodule}, "vmodule", "comma-separated list of file=N settings for file-filtered logging")
	pf.Var(&pflagValue{&logging.traceLocation}, "log-backtrace-at", "when logging hits line file:N, emit a stack trace")
	logDir = pf.String("log-dir", "", "if non-empty, write log files in this directory") // in clog.go

	pf.BoolVar(&humanLogging, "log-human", false, "log human-friendly output to stderr (slower)") // in log.go
}

// trimTestArgs iterates through os.Args[1:] and looks for the first flag that
// does not match the pattern `\-test\..*`. It returns the subslice of those
// values and (a copy of) the remaining values. Both returned slices contain
// os.Args[0].
func trimTestArgs() ([]string, []string) {
	cutoff := 1
	args := append([]string(nil), os.Args[0])
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-test.") {
			cutoff++
			continue
		}
		args = append(args, arg)
	}
	return os.Args[:cutoff], args
}

// SetupTestingFlags sets up the logging flags, feeds those flags from
// os.Args into it which don't belong to go tooling (`\-test\..*`), and
// truncates os.Args accordingly.
func SetupTestingFlags() {
	InitFlags(pflag.CommandLine)
	var ourArgs []string
	os.Args, ourArgs = trimTestArgs()
	if err := pflag.CommandLine.Parse(ourArgs); err != nil {
		panic(err.Error())
	}
}
