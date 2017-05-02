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

package logflags

import (
	"flag"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

type atomicBool struct {
	sync.Locker
	b *bool
}

func (ab *atomicBool) IsBoolFlag() bool {
	return true
}

func (ab *atomicBool) String() string {
	if ab.Locker == nil {
		return strconv.FormatBool(false)
	}
	ab.Lock()
	defer ab.Unlock()
	return strconv.FormatBool(*ab.b)
}

func (ab *atomicBool) Set(s string) error {
	ab.Lock()
	defer ab.Unlock()
	b, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	*ab.b = b
	return nil
}

func (ab *atomicBool) Type() string {
	return "bool"
}

var _ flag.Value = &atomicBool{}

// LogToStderrName and others are flag names.
const (
	LogToStderrName               = "logtostderr"
	NoColorName                   = "no-color"
	VerbosityName                 = "verbosity"
	VModuleName                   = "vmodule"
	LogBacktraceAtName            = "log-backtrace-at"
	LogDirName                    = "log-dir"
	NoRedirectStderrName          = "no-redirect-stderr"
	ShowLogsName                  = "show-logs"
	LogFileMaxSizeName            = "log-file-max-size"
	LogFilesCombinedMaxSizeName   = "log-dir-max-size"
	LogFileVerbosityThresholdName = "log-file-verbosity"
)

// InitFlags creates logging flags which update the given variables. The passed mutex is
// locked while the boolean variables are accessed during flag updates.
func InitFlags(
	noRedirectStderr *bool,
	logDir flag.Value,
	showLogs *bool,
	nocolor *bool,
	verbosity, vmodule, traceLocation flag.Value,
	logFileMaxSize, logFilesCombinedMaxSize *int64,
) {
	flag.BoolVar(nocolor, NoColorName, *nocolor, "disable standard error log colorization")
	flag.BoolVar(noRedirectStderr, NoRedirectStderrName, *noRedirectStderr, "disable redirect of stderr to the log file")
	flag.Var(verbosity, VerbosityName, "log level for V logs")
	flag.Var(vmodule, VModuleName, "comma-separated list of pattern=N settings for file-filtered logging")
	flag.Var(traceLocation, LogBacktraceAtName, "when logging hits line file:N, emit a stack trace")
	flag.Var(logDir, LogDirName, "if non-empty, write log files in this directory")
	flag.BoolVar(showLogs, ShowLogsName, *showLogs, "print logs instead of saving them in files")
	flag.Var(humanizeutil.NewBytesValue(logFileMaxSize), LogFileMaxSizeName, "maximum size of each log file")
	flag.Var(humanizeutil.NewBytesValue(logFilesCombinedMaxSize), LogFilesCombinedMaxSizeName, "maximum combined size of all log files")
}
