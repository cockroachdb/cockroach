// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package errors

import (
	"fmt"
	"os/exec"

	crdberrors "github.com/cockroachdb/errors"
)

// Error: Errors implementing this interface are used by the wrap() function in
// the main roachprod package to output correctly classified log messages and exit codes.
type Error interface {
	Error() string

	// The exit code for the error when exiting roachprod.
	ExitCode() int

	// Get the wrapped error.
	Unwrap() error
}

// CmdError: Wrap errors that result from a non-cockroach command run against
// the cluster.
//
// For errors coming from a cockroach command, use CockroachError.
type CmdError struct {
	Err error
}

func (e CmdError) Error() string {
	return fmt.Sprintf("COMMAND_PROBLEM: %s", e.Err.Error())
}

func (e CmdError) ExitCode() int {
	return 2
}

func (e CmdError) Unwrap() error {
	return e.Err
}

// CockroachError: Wrap errors that result from a cockroach command run against the cluster.
//
// For non-cockroach commands, use CmdError.
type CockroachError struct {
	Err error
}

func (e CockroachError) Error() string {
	return fmt.Sprintf("DEAD_ROACH_PROBLEM: %s", e.Err.Error())
}

func (e CockroachError) ExitCode() int {
	return 3
}

func (e CockroachError) Unwrap() error {
	return e.Err
}

// SSHError: Wrap ssh-specific errors from connections to remote hosts.
type SSHError struct {
	Err error
}

func (e SSHError) Error() string {
	return fmt.Sprintf("SSH_PROBLEM: %s", e.Err.Error())
}

func (e SSHError) ExitCode() int {
	return 5
}

func (e SSHError) Unwrap() error {
	return e.Err
}

// UnclassifiedError: Wrap roachprod and unclassified errors.
type UnclassifiedError struct {
	Err error
}

func (e UnclassifiedError) Error() string {
	return fmt.Sprintf("UNCLASSIFIED_PROBLEM: %s", e.Err.Error())
}

func (e UnclassifiedError) ExitCode() int {
	return 1
}

func (e UnclassifiedError) Unwrap() error {
	return e.Err
}

// Classify an error received while executing a non-cockroach command remotely
// over an ssh connection to the right Error type.
func ClassifyCmdError(err error) Error {
	if err == nil {
		return nil
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		if exitErr.ExitCode() == 255 {
			return SSHError{err}
		} else {
			return CmdError{err}
		}
	}

	return UnclassifiedError{err}
}

// Classify an error received while executing a cockroach command remotely
// over an ssh connection to the right Error type.
func ClassifyCockroachError(err error) Error {
	if err == nil {
		return nil
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		if exitErr.ExitCode() == 255 {
			return SSHError{err}
		} else {
			return CockroachError{err}
		}
	}

	return UnclassifiedError{err}
}

// Extract the Error from err's error tree or (nil, false) if none exists.
func AsError(err error) (Error, bool) {
	if rpErr, ok := crdberrors.If(err, func(err error) (interface{}, bool) {
		if rpErr, ok := err.(Error); ok {
			return rpErr, true
		}
		return nil, false
	}); ok {
		return rpErr.(Error), true
	}
	return nil, false
}
