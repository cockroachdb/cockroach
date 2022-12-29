// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/errors"
)

// Error is an interface for error types used by the main.wrap() function
// to output correctly classified log messages and exit codes.
type Error interface {
	error

	// The exit code for the error when exiting roachprod.
	ExitCode() int
}

// Exit codes for the errors
const (
	cmdExitCode          = 20
	sshExitCode          = 10
	unclassifiedExitCode = 1
)

// ErrSSH255 is a reference error used to mark an SSH error with an exit
// code of 255. This could be indicative of an SSH flake.
var ErrSSH255 = errors.New("SSH error occurred with exit code 255")

// Cmd wraps errors that result from a command run against the cluster.
type Cmd struct {
	Err error
}

func (e Cmd) Error() string {
	return fmt.Sprintf("COMMAND_PROBLEM: %s", e.Err.Error())
}

// ExitCode gives the process exit code to return for non-cockroach command
// errors.
func (e Cmd) ExitCode() int {
	return cmdExitCode
}

// Format passes formatting responsibilities to cockroachdb/errors
func (e Cmd) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// Unwrap the wrapped the non-cockroach command error.
func (e Cmd) Unwrap() error {
	return e.Err
}

// SSH wraps ssh-specific errors from connections to remote hosts.
type SSH struct {
	Err error
}

func (e SSH) Error() string {
	return fmt.Sprintf("SSH_PROBLEM: %s", e.Err.Error())
}

// ExitCode gives the process exit code to return for SSH errors.
func (e SSH) ExitCode() int {
	return sshExitCode
}

// Format passes formatting responsibilities to cockroachdb/errors
func (e SSH) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// Unwrap the wrapped SSH error.
func (e SSH) Unwrap() error {
	return e.Err
}

// Unclassified wraps roachprod and unclassified errors.
type Unclassified struct {
	Err error
}

func (e Unclassified) Error() string {
	return fmt.Sprintf("UNCLASSIFIED_PROBLEM: %s", e.Err.Error())
}

// ExitCode gives the process exit code to return for unclassified errors.
func (e Unclassified) ExitCode() int {
	return unclassifiedExitCode
}

// Format passes formatting responsibilities to cockroachdb/errors
func (e Unclassified) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// Unwrap the wrapped unclassified error.
func (e Unclassified) Unwrap() error {
	return e.Err
}

// ClassifyCmdError classifies an error received while executing a
// non-cockroach command remotely over an ssh connection to the right Error
// type.
func ClassifyCmdError(err error) Error {
	if err == nil {
		return nil
	}

	if exitCode, ok := GetExitCode(err); ok {
		if exitCode == 255 {
			return SSH{errors.Mark(err, ErrSSH255)}
		}
		return Cmd{err}
	}

	return Unclassified{err}
}

// GetExitCode returns an exit code, true if the error is an instance
// of an ExitError, or -1, false otherwise
func GetExitCode(err error) (int, bool) {
	if exitErr, ok := asExitError(err); ok {
		return exitErr.ExitCode(), true
	}

	return -1, false
}

// Extract the ExitError from err's error tree or (nil, false) if none exists.
func asExitError(err error) (*exec.ExitError, bool) {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr, true
	}
	return nil, false
}

// AsError extracts the Error from err's error tree or (nil, false) if none exists.
func AsError(err error) (Error, bool) {
	var e Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}
