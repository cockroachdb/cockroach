// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
)

// capture executes the command specified by args and returns its stdout. If
// the process exits with a failing exit code, capture instead returns an error
// which includes the process's stderr.
func capture(args ...string) (string, error) {
	var cmd *exec.Cmd
	if len(args) == 0 {
		panic("capture called with no arguments")
	} else if len(args) == 1 {
		cmd = exec.Command(args[0])
	} else {
		cmd = exec.Command(args[0], args[1:]...)
	}
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			err = fmt.Errorf("%w: %s", err, exitErr.Stderr)
		}
		return "", err
	}
	return string(bytes.TrimSpace(out)), err
}

// spawn executes the command specified by args. The subprocess inherits the
// current processes's stdin, stdout, and stderr streams. If the process exits
// with a failing exit code, run returns a generic "process exited with
// status..." error, as the process has likely written an error message to
// stderr.
func spawn(args ...string) error {
	var cmd *exec.Cmd
	if len(args) == 0 {
		panic("spawn called with no arguments")
	} else if len(args) == 1 {
		cmd = exec.Command(args[0])
	} else {
		cmd = exec.Command(args[0], args[1:]...)
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
