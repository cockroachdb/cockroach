// Copyright 2019 The Cockroach Authors.
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
	"bytes"
	"fmt"
	stdLog "log"
	"strconv"
)

// NewStdLogger creates a *stdLog.Logger that forwards messages to the
// CockroachDB logs with the specified severity.
//
// The prefix appears at the beginning of each generated log line.
func NewStdLogger(severity Severity, prefix string) *stdLog.Logger {
	return stdLog.New(logBridge(severity), prefix, stdLog.Lshortfile)
}

// logBridge provides the Write method that enables copyStandardLogTo to connect
// Go's standard logs to the logs provided by this package.
type logBridge Severity

// copyStandardLogTo arranges for messages written to the Go "log"
// package's default logs to also appear in the CockroachDB logs with
// the specified severity.  Subsequent changes to the standard log's
// default output location or format may break this behavior.
//
// Valid names are "INFO", "WARNING", "ERROR", and "FATAL".  If the name is not
// recognized, copyStandardLogTo panics.
func copyStandardLogTo(severityName string) {
	sev, ok := SeverityByName(severityName)
	if !ok {
		panic(fmt.Sprintf("copyStandardLogTo(%q): unrecognized Severity name", severityName))
	}
	// Set a log format that captures the user's file and line:
	//   d.go:23: message
	stdLog.SetFlags(stdLog.Lshortfile)
	stdLog.SetOutput(logBridge(sev))
}

func init() {
	copyStandardLogTo("INFO")
}

// Write parses the standard logging line and passes its components to the
// logger for Severity(lb).
func (lb logBridge) Write(b []byte) (n int, err error) {
	var (
		file = "???"
		line = 1
		text string
	)
	// Split "d.go:23: message" into "d.go", "23", and "message".
	if parts := bytes.SplitN(b, []byte{':'}, 3); len(parts) != 3 || len(parts[0]) < 1 || len(parts[2]) < 1 {
		text = fmt.Sprintf("bad log format: %s", b)
	} else {
		file = string(parts[0])
		text = string(parts[2][1 : len(parts[2])-1]) // skip leading space and trailing newline
		line, err = strconv.Atoi(string(parts[1]))
		if err != nil {
			text = fmt.Sprintf("bad line number: %s", b)
			line = 1
		}
	}
	mainLog.outputLogEntry(Severity(lb), file, line, text)
	return len(b), nil
}
