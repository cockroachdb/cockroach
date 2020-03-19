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
	error

	// The exit code for the error when exiting roachprod.
	ExitCode() int
}

// Cmd: Wrap errors that result from a non-cockroach command run against
// the cluster.
//
// For errors coming from a cockroach command, use Cockroach.
type Cmd struct {
	Err error
}

func (e Cmd) Error() string {
	return fmt.Sprintf("COMMAND_PROBLEM: %s", e.Err.Error())
}

func (e Cmd) ExitCode() int {
	return 20
}

func (e Cmd) Unwrap() error {
	return e.Err
}

// Cockroach: Wrap errors that result from a cockroach command run against the cluster.
//
// For non-cockroach commands, use Cmd.
type Cockroach struct {
	Err error
}

func (e Cockroach) Error() string {
	return fmt.Sprintf("DEAD_ROACH_PROBLEM: %s", e.Err.Error())
}

func (e Cockroach) ExitCode() int {
	return 30
}

func (e Cockroach) Unwrap() error {
	return e.Err
}

// SSH: Wrap ssh-specific errors from connections to remote hosts.
type SSH struct {
	Err error
}

func (e SSH) Error() string {
	return fmt.Sprintf("SSH_PROBLEM: %s", e.Err.Error())
}

func (e SSH) ExitCode() int {
	return 10
}

func (e SSH) Unwrap() error {
	return e.Err
}

// Unclassified: Wrap roachprod and unclassified errors.
type Unclassified struct {
	Err error
}

func (e Unclassified) Error() string {
	return fmt.Sprintf("UNCLASSIFIED_PROBLEM: %s", e.Err.Error())
}

func (e Unclassified) ExitCode() int {
	return 1
}

func (e Unclassified) Unwrap() error {
	return e.Err
}

// Classify an error received while executing a non-cockroach command remotely
// over an ssh connection to the right Error type.
func ClassifyCmdError(err error) Error {
	if err == nil {
		return nil
	}

	if exitErr, ok := asExitError(err); ok {
		if exitErr.ExitCode() == 255 {
			return SSH{err}
		} else {
			return Cmd{err}
		}
	}

	return Unclassified{err}
}

// Classify an error received while executing a cockroach command remotely
// over an ssh connection to the right Error type.
func ClassifyCockroachError(err error) Error {
	if err == nil {
		return nil
	}

	if exitErr, ok := asExitError(err); ok {
		if exitErr.ExitCode() == 255 {
			return SSH{err}
		} else {
			return Cockroach{err}
		}
	}

	return Unclassified{err}
}

// Extract the an ExitError from err's error tree or (nil, false) if none exists.
func asExitError(err error) (*exec.ExitError, bool) {
	if exitErr, ok := crdberrors.If(err, func(err error) (interface{}, bool) {
		if err, ok := err.(*exec.ExitError); ok {
			return err, true
		}
		return nil, false
	}); ok {
		return exitErr.(*exec.ExitError), true
	}
	return nil, false
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

// Returns an error from the list in this priority order:
//
// - the Error with the highest exit code
// - one of the `error`s
// - nil
func SelectPriorityError(errors []error) error {
	var result Error
	for _, err := range errors {
		if err == nil {
			continue
		}

		rpErr, _ := AsError(err)
		if result == nil {
			result = rpErr
			continue
		}

		if rpErr.ExitCode() > result.ExitCode() {
			result = rpErr
		}
	}

	if result != nil {
		return result
	}

	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

