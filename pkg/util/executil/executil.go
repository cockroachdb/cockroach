// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package executil

import (
	"bytes"
	"os"
	"os/exec"

	"github.com/pkg/errors"
)

// Capture executes the command specified by args and returns its stdout. If
// the process exits with a failing exit code, Capture instead returns an error
// which includes the process's stderr.
func Capture(args ...string) (string, error) {
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
		if exitErr, ok := err.(*exec.ExitError); ok {
			err = errors.Errorf("%s: %s", err, exitErr.Stderr)
		}
		return "", err
	}
	return string(bytes.TrimSpace(out)), err
}

// Run executes the command specified by args. The subprocess inherits the
// current processes's stdin, stdout, and stderr streams. If the process exits
// with a failing exit code, Run returns a generic "process exited with
// status..." error, as the process has likely written an error message to
// stderr.
func Run(args ...string) error {
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
