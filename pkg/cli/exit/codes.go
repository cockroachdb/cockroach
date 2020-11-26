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

// Codes that are common to all command times (server + client) follow.

// Success (0) represents a normal process termination.
func Success() Code { return Code{0} }

// UnspecifiedError (1) indicates the process has terminated with an
// error condition. The specific cause of the error can be found in
// the logging output.
func UnspecifiedError() Code { return Code{1} }

// UnspecifiedGoPanic (2) indicates the process has terminated due to
// an uncaught Go panic or some other error in the Go runtime.
//
// The reporting of this exit code likely indicates a programming
// error inside CockroachDB.
//
// Conversely, this should not be used when implementing features.
func UnspecifiedGoPanic() Code { return Code{2} }

// Interrupted (3) indicates the server process was interrupted with
// Ctrl+C / SIGINT.
func Interrupted() Code { return Code{3} }

// CommandLineFlagError (4) indicates there was an error in the
// command-line parameters.
func CommandLineFlagError() Code { return Code{4} }

// LoggingStderrUnavailable (5) indicates that an error occurred
// during a logging operation to the process' stderr stream.
func LoggingStderrUnavailable() Code { return Code{5} }

// LoggingFileUnavailable (6) indicates that an error occurred
// during a logging operation to a file.
func LoggingFileUnavailable() Code { return Code{6} }

// FatalError (7) indicates that a logical error in the server caused
// an emergency shutdown.
func FatalError() Code { return Code{7} }

// TimeoutAfterFatalError (8) indicates that an emergency shutdown
// due to a fatal error did not occur properly due to some blockage
// in the logging system.
func TimeoutAfterFatalError() Code { return Code{8} }

// LoggingNetCollectorUnavailable (9) indicates that an error occurred
// during a logging operation to a network collector.
func LoggingNetCollectorUnavailable() Code { return Code{9} }

// Codes that are specific to client commands follow. It's possible
// for codes to be reused across separate client or server commands.
// Command-specific exit codes should be allocated down from 125.

// 'doctor' exit codes.

// DoctorValidationFailed indicates that the 'doctor' command has detected
// an inconsistency in the SQL metaschema.
func DoctorValidationFailed() Code { return Code{125} }
