// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logflags

import "flag"

// LogToStderrName and others are flag names.
const (
	VModuleName       = "vmodule"
	ShowLogsName      = "show-logs"
	TestLogConfigName = "test-log-config"
)

// InitFlags creates logging flags which update the given variables. The passed mutex is
// locked while the boolean variables are accessed during flag updates.
func InitFlags(showLogs *bool, testLogConfig *string, vmodule flag.Value) {
	flag.Var(vmodule, VModuleName, "comma-separated list of pattern=N settings for file-filtered logging (significantly hurts performance)")
	flag.StringVar(testLogConfig, TestLogConfigName, "", "YAML log configuration for tests")
	flag.BoolVar(showLogs, ShowLogsName, *showLogs, "print logs instead of saving them in files")
}
