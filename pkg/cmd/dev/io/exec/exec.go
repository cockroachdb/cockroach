// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/recording"
)

// Exec is a convenience wrapper around the stdlib os/exec package. It lets us
// mock all instances where we shell out for tests.
type Exec struct {
	dir            string
	logger         *log.Logger
	stdout, stderr io.Writer
	*recording.Recording
}

// New returns a new Exec with the given options.
func New(opts ...Option) *Exec {
	e := &Exec{}

	// Apply the default options.
	defaults := []func(executor *Exec){
		WithLogger(log.New(os.Stdout, "executing: ", 0)),
		WithStdOutErr(os.Stdout, os.Stderr),
	}
	for _, opt := range defaults {
		opt(e)
	}

	// Apply the user-provided options, overriding the defaults as necessary.
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Option is a start-up option that can be specified when constructing a new
// Exec handle.
type Option func(e *Exec)

// WithLogger configures Exec to use the provided logger.
func WithLogger(logger *log.Logger) func(e *Exec) {
	return func(e *Exec) {
		e.logger = logger
	}
}

// WithStdOutErr configures Exec to use the provided sinks for std{out,err}.
func WithStdOutErr(stdout, stderr io.Writer) func(e *Exec) {
	return func(e *Exec) {
		e.stdout = stdout
		e.stderr = stderr
	}
}

// WithRecording configures Exec to use the provided recording.
func WithRecording(r *recording.Recording) func(e *Exec) {
	return func(e *Exec) {
		e.Recording = r
	}
}

// CommandContextSilent is like CommandContext, but does not take over
// stdout/stderr. It's to be used for "internal" operations.
func (e *Exec) CommandContextSilent(
	ctx context.Context, name string, args ...string,
) ([]byte, error) {
	return e.commandContextImpl(ctx, nil, true, name, args...)
}

// CommandContextWithInput is like CommandContext, but stdin is piped from an
// in-memory string.
func (e *Exec) CommandContextWithInput(
	ctx context.Context, stdin, name string, args ...string,
) ([]byte, error) {
	r := strings.NewReader(stdin)
	return e.commandContextImpl(ctx, r, false, name, args...)
}

// CommandContextInheritingStdStreams is like CommandContext, but stdin,
// stdout, and stderr are passed directly to the terminal.
func (e *Exec) CommandContextInheritingStdStreams(
	ctx context.Context, name string, args ...string,
) error {
	var command string
	if len(args) > 0 {
		command = fmt.Sprintf("%s %s", name, strings.Join(args, " "))
	} else {
		command = name
	}
	e.logger.Print(command)

	if e.Recording == nil {
		// Do the real thing.
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = e.stdout
		cmd.Stderr = e.stderr
		cmd.Dir = e.dir

		if err := cmd.Start(); err != nil {
			return err
		}
		if err := cmd.Wait(); err != nil {
			return err
		}
		return nil
	}

	_, err := e.replay(command)
	return err
}

// LookPath wraps around exec.LookPath, which searches for an executable named
// file in the directories named by the PATH environment variable.
func (e *Exec) LookPath(path string) (string, error) {
	command := fmt.Sprintf("which %s", path)
	e.logger.Print(command)

	if e.Recording == nil {
		// Do the real thing.
		var err error
		fullPath, err := exec.LookPath(path)
		if err != nil {
			return "", err
		}
		return fullPath, nil
	}

	ret, err := e.replay(command)
	return ret, err
}

func (e *Exec) commandContextImpl(
	ctx context.Context, stdin io.Reader, silent bool, name string, args ...string,
) ([]byte, error) {
	var command string
	if len(args) > 0 {
		command = fmt.Sprintf("%s %s", name, strings.Join(args, " "))
	} else {
		command = name
	}
	e.logger.Print(command)

	var buffer bytes.Buffer
	if e.Recording == nil {
		// Do the real thing.
		cmd := exec.CommandContext(ctx, name, args...)
		if silent {
			cmd.Stdout = &buffer
			cmd.Stderr = ioutil.Discard
		} else {
			cmd.Stdout = io.MultiWriter(e.stdout, &buffer)
			cmd.Stderr = e.stderr
		}
		if stdin != nil {
			cmd.Stdin = stdin
		}
		cmd.Dir = e.dir

		if err := cmd.Start(); err != nil {
			return nil, err
		}
		if err := cmd.Wait(); err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	}

	output, err := e.replay(command)
	if err != nil {
		return nil, err
	}

	return []byte(output), nil
}

// replay replays the specified command, erroring out if it's mismatched with
// what the recording plays back next. It returns the recorded output.
func (e *Exec) replay(command string) (output string, err error) {
	found, err := e.Recording.Next(func(op recording.Operation) error {
		if op.Command != command {
			return fmt.Errorf("expected %q, got %q", op.Command, command)
		}
		output = op.Output
		return nil
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("recording for %q not found", command)
	}
	return output, nil
}
