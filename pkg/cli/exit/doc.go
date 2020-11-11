// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package exit encapsulates calls to os.Exit to control the
// production of process exit status codes.
//
// Its goal is to ensure that all possible exit codes produced
// by the 'cockroach' process upon termination are documented.
// It achieves this by providing a type exit.Code and requiring that
// all possible values come from constructors in the package (see
// codes.go). A linter ensures that no direct call to os.Exit() can be
// present elsewhere.
//
// Note that due to the limited range of unix exit codes, it is not
// possible to map all possible error situations inside a CockroachDB
// server to a unique exit code.
// This is why the main mechanism to explain the cause of a process
// termination must remain the logging subsystem.
//
// The introduction of discrete exit codes here is thus meant to
// merely complement logging, in those cases where logging is unable
// to detail the reason why the process is terminating; for example:
//
// - before logging is initialized (e.g. during command-line parsing)
// - when a logging operation fails.
//
// For client commands, the situation is different: there are much
// fewer different exit situations, so we could envision discrete
// error codes for them. Additionally, different client commands
// can reuse the same numeric codes for different error situations,
// when they do not overlap.
//
// This package accommodates this as follows:
//
// - exit codes common to all commands should be allocated
//   incrementally starting from the last defined common error
//   in codes.go.
//
// - exit codes specific to one command should be allocated downwards
//   starting from 125.
//
package exit
