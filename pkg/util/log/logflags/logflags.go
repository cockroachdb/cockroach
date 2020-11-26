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

import "flag"

// LogToStderrName and others are flag names.
const (
	VModuleName  = "vmodule"
	ShowLogsName = "show-logs"
)

// InitFlags creates logging flags which update the given variables. The passed mutex is
// locked while the boolean variables are accessed during flag updates.
func InitFlags(showLogs *bool, vmodule flag.Value) {
	flag.Var(vmodule, VModuleName, "comma-separated list of pattern=N settings for file-filtered logging (significantly hurts performance)")
	flag.BoolVar(showLogs, ShowLogsName, *showLogs, "print logs instead of saving them in files")
}
