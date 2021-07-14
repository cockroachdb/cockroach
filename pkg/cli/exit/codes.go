// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exit

const (
	//
	// Codes that are common to all command times (server + client) follow.
	//

	// Success represents a normal process termination.
	Success Code = 0

	// UnspecifiedError indicates the process has terminated with an
	// error condition. The specific cause of the error can be found in
	// the logging output.
	UnspecifiedError = 1

	// UnspecifiedGoPanic indicates the process has terminated due to
	// an uncaught Go panic or some other error in the Go runtime.
	//
	// The reporting of this exit code likely indicates a programming
	// error inside CockroachDB.
	//
	// Conversely, this should not be used when implementing features.
	UnspecifiedGoPanic = 2

	// Interrupted indicates the server process was interrupted with
	// Ctrl+C / SIGINT.
	Interrupted = 3

	// CommandLineFlagError indicates there was an error in the
	// command-line parameters.
	CommandLineFlagError = 4

	// LoggingStderrUnavailable indicates that an error occurred
	// during a logging operation to the process' stderr stream.
	LoggingStderrUnavailable = 5

	// LoggingFileUnavailable indicates that an error occurred
	// during a logging operation to a file.
	LoggingFileUnavailable = 6

	// FatalError indicates that a logical error in the server caused
	// an emergency shutdown.
	FatalError = 7

	// TimeoutAfterFatalError indicates that an emergency shutdown
	// due to a fatal error did not occur properly due to some blockage
	// in the logging system.
	TimeoutAfterFatalError = 8

	// LoggingNetCollectorUnavailable indicates that an error occurred
	// during a logging operation to a network collector.
	LoggingNetCollectorUnavailable = 9

	//
	// Codes that are specific to client commands follow. It's possible
	// for codes to be reused across separate client or server commands.
	// Command-specific exit codes should be allocated down from 125.
	//

	// 'doctor' exit codes.

	// DoctorValidationFailed indicates that the 'doctor' command has detected
	// an inconsistency in the SQL metaschema.
	DoctorValidationFailed = 125
)
