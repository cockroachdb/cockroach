// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// IsBoolFlag is recognized by pflags.
func (ab *atomicBool) IsBoolFlag() bool {
	return true
}

var _ = (*atomicBool).IsBoolFlag

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
	RedactableLogsName            = "redactable-logs"
	VModuleName                   = "vmodule"
	LogDirName                    = "log-dir"
	ShowLogsName                  = "show-logs"
	LogFileMaxSizeName            = "log-file-max-size"
	LogFilesCombinedMaxSizeName   = "log-group-max-size"
	LogFileVerbosityThresholdName = "log-file-verbosity"

	DeprecatedLogFilesCombinedMaxSizeName = "log-dir-max-size"
)

// InitFlags creates logging flags which update the given variables. The passed mutex is
// locked while the boolean variables are accessed during flag updates.
func InitFlags(
	logDir flag.Value,
	showLogs *bool,
	nocolor *bool,
	redactableLogs *bool,
	vmodule flag.Value,
	logFileMaxSize, logFilesCombinedMaxSize *int64,
) {
	flag.BoolVar(nocolor, NoColorName, *nocolor, "disable standard error log colorization")
	flag.BoolVar(redactableLogs, RedactableLogsName, *redactableLogs, "make log outputs redactable for confidentiality")
	flag.Var(vmodule, VModuleName, "comma-separated list of pattern=N settings for file-filtered logging (significantly hurts performance)")
	flag.Var(logDir, LogDirName, "if non-empty, write log files in this directory")
	flag.BoolVar(showLogs, ShowLogsName, *showLogs, "print logs instead of saving them in files")
	flag.Var(humanizeutil.NewBytesValue(logFileMaxSize), LogFileMaxSizeName, "maximum size of each log file")
	flag.Var(humanizeutil.NewBytesValue(logFilesCombinedMaxSize), LogFilesCombinedMaxSizeName, "maximum combined size of all log files in a logging group")
	// The deprecated  name for this last flag.
	flag.Var(humanizeutil.NewBytesValue(logFilesCombinedMaxSize), "log-dir-max-size", "maximum combined size of all log files in a logging group (deprecated)")
}
