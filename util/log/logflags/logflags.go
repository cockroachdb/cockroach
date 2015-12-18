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
)

type atomicBool struct {
	sync.Locker
	b *bool
}

func (ab *atomicBool) IsBoolFlag() bool {
	return true
}

func (ab *atomicBool) String() string {
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

func (ab *atomicBool) Get() string {
	return ab.String()
}

var _ flag.Value = &atomicBool{}

// InitFlags creates logging flags which update the given variables. The passed mutex is
// locked while the boolean variables are accessed during flag updates.
func InitFlags(mu sync.Locker, toStderr *bool, alsoToStderr *bool, logDir, color *string,
	verbosity, vmodule, traceLocation flag.Value) {
	*toStderr = true // wonky way of specifying a default
	flag.Var(&atomicBool{Locker: mu, b: toStderr}, "logtostderr", "log to standard error instead of files")
	flag.Var(&atomicBool{Locker: mu, b: alsoToStderr}, "alsologtostderr", "log to standard error as well as files")
	flag.StringVar(color, "color", "auto", "colorize standard error output according to severity")
	flag.Var(verbosity, "verbosity", "log level for V logs")
	// TODO(tschottdorf): decide if we need this.
	// pf.Var(&logging.stderrThreshold, "log-threshold", "logs at or above this threshold go to stderr")
	flag.Var(vmodule, "vmodule", "comma-separated list of file=N settings for file-filtered logging")
	flag.Var(traceLocation, "log-backtrace-at", "when logging hits line file:N, emit a stack trace")
	flag.StringVar(logDir, "log-dir", "", "if non-empty, write log files in this directory") // in util/log/file.go

}
